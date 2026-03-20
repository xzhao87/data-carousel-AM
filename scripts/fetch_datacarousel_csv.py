#!/usr/bin/env python3
"""
Fetch Data Carousel staging records from CERN OpenSearch and save to CSV.

Key features:
- Uses config.ini for OpenSearch connection
- Supports full eshost URL (https://...:port/prefix)
- Filters:
    method = stage_request
    ddm_rule_id exists
    to_pin != true   (handled in Python)
- Outputs selected columns in strict order
- Adds parsed dataset fields via DatasetParsing module

Usage examples:

1) Default:
    python scripts/fetch_datacarousel_csv.py

2) Custom time:
    python scripts/fetch_datacarousel_csv.py \
        --start 2025-09-01T00:00:00Z \
        --end now

3) Custom index:
    python scripts/fetch_datacarousel_csv.py \
        --index "atlas_datacarousel-2026.03"
"""

from __future__ import annotations

import argparse
import configparser
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import pandas as pd
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_gssapi import HTTPSPNEGOAuth, OPTIONAL

# Allow importing DatasetParsing from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from DatasetParsing.hep_dataset_parser import HEPDatasetParser  # noqa


class DataCarouselLogs:

    FRONT_COLUMNS = [
        "source_tape",
        "source_rse",
        "destination_rse",
        "timeEvent",
        "dataset",
        "total_files",
        "dataset_size",
        "task_id",
        "task_type",
        "task_group",
        "task_user",
        "ddm_rule_id",
        "dataset_format",
        "dataset_format_short",
        "to_pin",
    ]

    def __init__(self):
        self.config = self._read_config()
        self._parse_eshost()

    def _read_config(self):
        config_path = REPO_ROOT / "config.ini"

        config = configparser.ConfigParser()
        config.read(config_path)

        return config["es_connection"]

    def _parse_eshost(self):
        """
        Parse full eshost URL like:
            https://os-atlas.cern.ch:443/os
        """
        url = self.config["eshost"]
        parsed = urlparse(url)

        self.host = parsed.hostname
        self.port = parsed.port or 443
        self.url_prefix = parsed.path.strip("/")

        self.ca_cert = self.config["certpath"]

    def connect_es(self):
        return OpenSearch(
            hosts=[{
                "host": self.host,
                "port": self.port,
                "url_prefix": self.url_prefix
            }],
            use_ssl=True,
            verify_certs=True,
            ca_certs=self.ca_cert,
            connection_class=RequestsHttpConnection,
            http_auth=HTTPSPNEGOAuth(mutual_authentication=OPTIONAL),
        )

    @staticmethod
    def flatten(d, parent_key="", sep="_"):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(DataCarouselLogs.flatten(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def build_query(start, end):
        return {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"method.keyword": "stage_request"}},
                        {"exists": {"field": "ddm_rule_id"}},
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start,
                                    "lte": end
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [{"@timestamp": "asc"}]
        }

    @staticmethod
    def scroll(client, index, query):
        res = client.search(index=index, body=query, scroll="2m", size=5000)
        sid = res["_scroll_id"]
        hits = res["hits"]["hits"]

        while hits:
            for h in hits:
                yield h
            res = client.scroll(scroll_id=sid, scroll="2m")
            sid = res["_scroll_id"]
            hits = res["hits"]["hits"]

    @staticmethod
    def normalize_to_pin(val):
        if val is None:
            return 0
        return 1 if str(val).lower() == "true" else 0

    def fetch(self, start, end, index):

        client = self.connect_es()
        query = self.build_query(start, end)

        records = []

        for hit in self.scroll(client, index, query):
            src = hit["_source"]

            # Python-level filtering for to_pin
            if self.normalize_to_pin(src.get("to_pin")) == 1:
                continue

            flat = self.flatten(src)
            records.append(flat)

        df = pd.DataFrame(records)

        if df.empty:
            return df

        # Normalize to_pin
        df["to_pin"] = df["to_pin"].apply(self.normalize_to_pin)

        # Convert numeric fields
        for col in ["total_files", "dataset_size", "task_id"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Parse dataset
        parser = HEPDatasetParser()
        original_cols = set(df.columns)
        df = parser.parse_dataset_column(df, column_name="dataset")
        parser_cols = [c for c in df.columns if c not in original_cols]

        # Ensure required columns exist
        for c in self.FRONT_COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA

        # Drop request_id explicitly
        if "request_id" in df.columns:
            df = df.drop(columns=["request_id"])

        # Final column order
        front = [c for c in self.FRONT_COLUMNS if c in df.columns]
        rest = [c for c in df.columns if c not in front and c not in parser_cols]

        df = df[front + parser_cols + rest]

        return df

    def save_csv(self, start, end, index, output):

        df = self.fetch(start, end, index)

        Path(output).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output, index=False)

        print(f"Saved {len(df)} rows → {output}")


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--start", default="2025-09-01T00:00:00Z")
    parser.add_argument("--end", default="now")
    parser.add_argument("--index", default="atlas_datacarousel-*")
    parser.add_argument("--output", default=str(REPO_ROOT / "data/prodsyslogs.csv"))

    args = parser.parse_args()

    fetcher = DataCarouselLogs()

    fetcher.save_csv(
        start=args.start,
        end=args.end,
        index=args.index,
        output=args.output
    )


if __name__ == "__main__":
    main()
