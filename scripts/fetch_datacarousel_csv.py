#!/usr/bin/env python3
"""
fetch_datacarousel_csv.py

Purpose
-------
Fetch Data Carousel staging records from CERN OpenSearch and write them to a CSV
file, reproducing the OpenSearch-only part of the original data-carousel repo,
but adapted to the newer atlas_datacarousel-* document format.

This script:
  1. Reads OpenSearch connection settings from config.ini
  2. Connects to OpenSearch using Kerberos / SPNEGO auth
  3. Queries atlas_datacarousel-* for records matching:
       - method = stage_request
       - ddm_rule_id exists
       - @timestamp in the requested time range
  4. Filters out records where to_pin is true/True/TRUE
       - this filtering is done in Python, not in the OpenSearch query,
         because we do not assume a to_pin.keyword mapping exists
  5. Flattens nested JSON documents into a tabular structure
  6. Removes Filebeat / Logstash / transport-related columns not useful for
     Data Carousel analysis
  7. Normalizes selected fields
  8. Parses the dataset column using the repo's local DatasetParsing module
  9. Renames selected parser-derived columns for preferred output naming
 10. Writes the result to a CSV file with columns in a controlled order

What this script does NOT do
----------------------------
It does NOT access the DEFT Oracle DB and does NOT enrich the CSV with DEFT data.

Why request_id is ignored
-------------------------
The newer OpenSearch documents include request_id, but for this workflow that is
an internal PanDA DC DB counter and not the real physics task identifier. So
this script intentionally ignores request_id.

Prerequisites
-------------
1. You have cloned the data-carousel repo locally
2. This script is placed under:
      scripts/fetch_datacarousel_csv.py
3. The repo contains the local module:
      DatasetParsing/hep_dataset_parser.py
4. You have a config.ini at the repo top directory with:

      [es_connection]
      eshost = https://os-atlas.cern.ch:443/os
      certpath = /full/path/to/CA-bundle.pem

5. You have a valid Kerberos ticket before running:
      kinit
      klist

6. You installed the minimal required Python packages:
      pip install -r requirements.txt

Typical usage
-------------
Run with defaults:
    python scripts/fetch_datacarousel_csv.py

This will:
  - query atlas_datacarousel-*
  - collect records from 2025-09-01T00:00:00Z to now
  - write data/prodsyslogs.csv

Custom time range:
    python scripts/fetch_datacarousel_csv.py \
        --start 2025-09-01T00:00:00Z \
        --end 2026-03-20T23:59:59Z

Custom output file:
    python scripts/fetch_datacarousel_csv.py \
        --output data/my_datacarousel.csv

Custom index pattern:
    python scripts/fetch_datacarousel_csv.py \
        --index "atlas_datacarousel-2026.01,atlas_datacarousel-2026.02,atlas_datacarousel-2026.03"

CLI arguments
-------------
--start   Start timestamp, inclusive, in ISO format. Default:
          2025-09-01T00:00:00Z

--end     End timestamp, inclusive, in ISO format or "now". Default:
          now

--index   OpenSearch index pattern. Default:
          atlas_datacarousel-*

--output  Output CSV path. Default:
          <repo>/data/prodsyslogs.csv

--help    Show usage/help text (provided automatically by argparse)

Output CSV columns
------------------
The CSV begins with these columns in this exact order if present:

  a) source_tape
  b) source_rse
  c) destination_rse
  d) timeEvent
  e) dataset
  f) total_files
  g) dataset_size
  h) task_id
  i) task_type
  j) task_group
  k) task_user
  l) ddm_rule_id
  m) scope
  n) dataset_format
  o) dataset_format_short
  p) to_pin
  q) runNumber/datasetID
  r) physicsShort/StreamName
  s) production_step
  t) dataset_origin
  u) dataset_category
  v) versionTag
  w) amiTag
  x) tid
  y) full_tid
  z) ... any remaining fields

Notes
-----
- The columns year, energy, b_unit, data_format, dataset_format_parser, and
  dataset_format_short_parser are removed from the final CSV output.
- parser field dataset_id is renamed to runNumber/datasetID
- parser field stream_physics is renamed to physicsShort/StreamName
- scope is placed between ddm_rule_id and dataset_format in the final CSV
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

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from DatasetParsing.hep_dataset_parser import HEPDatasetParser  # noqa: E402


class DataCarouselLogs:
    """
    Encapsulates the OpenSearch-only workflow:
      - read config
      - connect to OpenSearch
      - query records
      - flatten JSON
      - drop transport/log shipping fields
      - normalize fields
      - parse dataset names
      - rename selected parser fields
      - save to CSV
    """

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
        "scope",
        "dataset_format",
        "dataset_format_short",
        "to_pin",
        "runNumber/datasetID",
        "physicsShort/StreamName",
        "production_step",
        "dataset_origin",
        "dataset_category",
        "versionTag",
        "amiTag",
        "tid",
        "full_tid",
    ]

    DROP_COLUMNS = [
        "message",
        "event_original",
        "input_type",
        "logLevel",
        "@version",
        "agent_id",
        "agent_version",
        "agent_ephemeral_id",
        "agent_name",
        "agent_type",
        "tags",
        "@timestamp",
        "fields_type",
        "log_file_path",
        "log_offset",
        "ecs_version",
        "host_name",
        "logName",
        "year",
        "energy",
        "b_unit",
        "data_format",
        "dataset_format_parser",
        "dataset_format_short_parser",
    ]

    RENAME_COLUMNS = {
        "dataset_id": "runNumber/datasetID",
        "stream_physics": "physicsShort/StreamName",
    }

    def __init__(self, config_path: Path | None = None) -> None:
        if config_path is None:
            config_path = REPO_ROOT / "config.ini"

        self.config_path = Path(config_path)
        self.config = self._read_config()
        self._parse_eshost()

    def _read_config(self) -> configparser.ConfigParser:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        config = configparser.ConfigParser()
        config.read(self.config_path)

        if "es_connection" not in config:
            raise KeyError(f"Missing [es_connection] section in {self.config_path}")

        for key in ("eshost", "certpath"):
            if key not in config["es_connection"]:
                raise KeyError(
                    f"Missing '{key}' in [es_connection] section of {self.config_path}"
                )

        return config

    def _parse_eshost(self) -> None:
        es_url = self.config["es_connection"]["eshost"]
        parsed = urlparse(es_url)

        if not parsed.scheme or not parsed.hostname:
            raise ValueError(
                f"Invalid eshost URL in config.ini: {es_url}\n"
                f"Expected something like: https://os-atlas.cern.ch:443/os"
            )

        self.host = parsed.hostname
        self.port = parsed.port or 443
        self.url_prefix = parsed.path.strip("/")
        self.ca_cert = self.config["es_connection"]["certpath"]

    def connect_es(self) -> OpenSearch:
        return OpenSearch(
            hosts=[
                {
                    "host": self.host,
                    "port": self.port,
                    "url_prefix": self.url_prefix,
                }
            ],
            use_ssl=True,
            verify_certs=True,
            ca_certs=self.ca_cert,
            connection_class=RequestsHttpConnection,
            http_auth=HTTPSPNEGOAuth(mutual_authentication=OPTIONAL),
            timeout=120,
            max_retries=3,
            retry_on_timeout=True,
        )

    @staticmethod
    def flatten(
        nested: Dict[str, Any],
        parent_key: str = "",
        sep: str = "_",
    ) -> Dict[str, Any]:
        items: List[tuple[str, Any]] = []

        for key, value in nested.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(DataCarouselLogs.flatten(value, new_key, sep=sep).items())
            else:
                items.append((new_key, value))

        return dict(items)

    @staticmethod
    def build_query(start: str, end: str) -> Dict[str, Any]:
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
                                    "lte": end,
                                }
                            }
                        },
                    ]
                }
            },
            "sort": [{"@timestamp": "asc"}],
        }

    @staticmethod
    def scroll_search(
        client: OpenSearch,
        index: str,
        query: Dict[str, Any],
        batch_size: int = 5000,
        scroll_keepalive: str = "2m",
    ) -> Iterable[Dict[str, Any]]:
        response = client.search(
            index=index,
            body=query,
            scroll=scroll_keepalive,
            size=batch_size,
        )
        scroll_id = response.get("_scroll_id")
        hits = response["hits"]["hits"]

        try:
            while hits:
                for hit in hits:
                    yield hit
                response = client.scroll(scroll_id=scroll_id, scroll=scroll_keepalive)
                scroll_id = response.get("_scroll_id")
                hits = response["hits"]["hits"]
        finally:
            if scroll_id:
                try:
                    client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

    @staticmethod
    def normalize_to_pin(value: Any) -> int:
        if isinstance(value, bool):
            return 1 if value else 0
        if value is None:
            return 0
        return 1 if str(value).strip().lower() == "true" else 0

    @staticmethod
    def ensure_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA
        return df

    def fetch(
        self,
        start: str = "2025-09-01T00:00:00Z",
        end: str = "now",
        index: str = "atlas_datacarousel-*",
    ) -> pd.DataFrame:
        client = self.connect_es()
        query = self.build_query(start, end)

        records: List[Dict[str, Any]] = []

        for hit in self.scroll_search(client, index=index, query=query):
            source = hit.get("_source", {})

            if self.normalize_to_pin(source.get("to_pin")) == 1:
                continue

            flat = self.flatten(source)
            records.append(flat)

        if not records:
            return pd.DataFrame(columns=self.FRONT_COLUMNS)

        df = pd.DataFrame(records)

        if "to_pin" in df.columns:
            df["to_pin"] = df["to_pin"].apply(self.normalize_to_pin)

        for col in ("total_files", "dataset_size", "task_id"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        parser_added_columns: List[str] = []
        if "dataset" in df.columns:
            parser = HEPDatasetParser()
            original_columns = list(df.columns)
            df = parser.parse_dataset_column(df, column_name="dataset")
            parser_added_columns = [c for c in df.columns if c not in original_columns]

        if "request_id" in df.columns:
            df = df.drop(columns=["request_id"])

        rename_map = {k: v for k, v in self.RENAME_COLUMNS.items() if k in df.columns}
        if rename_map:
            df = df.rename(columns=rename_map)

        parser_added_columns = [self.RENAME_COLUMNS.get(c, c) for c in parser_added_columns]

        df = df.drop(columns=[c for c in self.DROP_COLUMNS if c in df.columns], errors="ignore")

        # Keep first occurrence only if duplicate names somehow appear.
        df = df.loc[:, ~df.columns.duplicated()]

        # Create missing final-output columns only at the very end.
        df = self.ensure_columns(df, self.FRONT_COLUMNS)

        front = [c for c in self.FRONT_COLUMNS if c in df.columns]
        parser_cols = [c for c in parser_added_columns if c in df.columns and c not in front]
        rest = [c for c in df.columns if c not in front and c not in parser_cols]

        df = df[front + parser_cols + rest]

        return df

    def save_csv(
        self,
        output_csv: Path | str = REPO_ROOT / "data" / "prodsyslogs.csv",
        start: str = "2025-09-01T00:00:00Z",
        end: str = "now",
        index: str = "atlas_datacarousel-*",
    ) -> None:
        output_csv = Path(output_csv)
        output_csv.parent.mkdir(parents=True, exist_ok=True)

        df = self.fetch(start=start, end=end, index=index)
        df.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"Wrote {len(df)} rows to {output_csv}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Data Carousel stage_request records from OpenSearch and write them to CSV. "
            "Reads eshost and certpath from config.ini [es_connection]."
        )
    )

    parser.add_argument(
        "--start",
        default="2025-09-01T00:00:00Z",
        help="Start timestamp, inclusive, in ISO format.",
    )

    parser.add_argument(
        "--end",
        default="now",
        help="End timestamp, inclusive, in ISO format or 'now'.",
    )

    parser.add_argument(
        "--index",
        default="atlas_datacarousel-*",
        help="OpenSearch index pattern.",
    )

    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "data" / "prodsyslogs.csv"),
        help="Output CSV path.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    fetcher = DataCarouselLogs()
    fetcher.save_csv(
        output_csv=args.output,
        start=args.start,
        end=args.end,
        index=args.index,
    )

