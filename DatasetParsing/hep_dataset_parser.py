#!/usr/bin/env python3
"""
hep_dataset_parser.py

Purpose
-------
Parse ATLAS / HEP dataset names into structured columns that are useful for
analysis.

This version:
  1. Avoids duplicate column names like repeated "scope"
  2. Adds versionTag
  3. Adds amiTag
  4. Adds tid
  5. Adds full_tid

Example
-------
For a dataset such as:

  mc16_13TeV:mc16_13TeV.364703.Pythia8EvtGen_A14NNPDF23LO_jetjet_JZ3WithSW.merge.EVNT.e7142_e5984_tid23108064_00

this parser extracts:
  - versionTag = e7142_e5984_tid23108064_00
  - amiTag     = e7142_e5984
  - tid        = 23108064
  - full_tid   = tid23108064_00
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

import pandas as pd


class HEPDatasetParser:
    """
    Parser for HEP / ATLAS dataset names.
    """

    def __init__(self) -> None:
        self.scope_pattern = re.compile(
            r"^(?P<dataset_origin>mc|data|valid)"
            r"(?P<year>\d{2})?"
            r"(?:_(?P<energy>[0-9p]+)(?P<b_unit>TeV|GeV))?"
        )

    def parse_scope(self, scope: str) -> Optional[Dict[str, Any]]:
        parsed = {
            "scope": scope,
            "dataset_origin": None,
            "year": None,
            "energy": None,
            "b_unit": None,
            "dataset_category": None,
        }

        match = self.scope_pattern.match(scope)
        if not match:
            return None

        parsed.update(match.groupdict())

        if parsed["energy"]:
            parsed["energy"] = parsed["energy"].replace("p", ".")

        if "hi" in scope:
            parsed["dataset_category"] = "heavy_ion"
        elif "cos" in scope:
            parsed["dataset_category"] = "cosmic"
        elif "pPb" in scope or "hip" in scope:
            parsed["dataset_category"] = "proton_lead"
        else:
            parsed["dataset_category"] = "standard"

        if parsed["dataset_origin"] == "valid":
            parsed["year"] = None

        return parsed

    @staticmethod
    def make_dataset_format_short(dataset_format: Optional[str]) -> Optional[str]:
        if not dataset_format:
            return None

        if dataset_format.startswith("DAOD_"):
            return "DAOD"
        if dataset_format.startswith("NTUP_"):
            return "NTUP"
        return dataset_format

    @staticmethod
    def split_version_tag(version_tag: Optional[str]) -> Dict[str, Optional[str]]:
        result = {
            "amiTag": None,
            "tid": None,
            "full_tid": None,
        }

        if not version_tag:
            return result

        tid_match = re.search(r"(tid\d+_\d+)$", version_tag)
        if tid_match:
            result["full_tid"] = tid_match.group(1)

        num_match = re.search(r"_tid(\d+)(?:_\d+)?$", version_tag)
        if num_match:
            result["tid"] = num_match.group(1)

        result["amiTag"] = re.sub(r"_tid\d+(?:_\d+)?$", "", version_tag)

        return result

    def parse_full_dataset_name(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        parts = dataset_name.split(":")
        scope = parts[0] if len(parts) == 2 else dataset_name.split(".")[0]

        scope_info = self.parse_scope(scope)
        if scope_info is None:
            return None

        name_part = parts[1] if len(parts) == 2 else dataset_name
        tokens = name_part.split(".")

        if len(tokens) < 5:
            return None

        dataset_id = tokens[1]
        stream_or_physics = tokens[2]
        production_step = tokens[3]
        dataset_format = tokens[4]
        version_tag = tokens[5] if len(tokens) > 5 else None

        split_tag = self.split_version_tag(version_tag)

        parsed = {
            "scope": scope,
            "dataset_origin": scope_info["dataset_origin"],
            "year": scope_info["year"],
            "energy": scope_info["energy"],
            "b_unit": scope_info["b_unit"],
            "dataset_category": scope_info["dataset_category"],
            "dataset_id": dataset_id,
            "stream_physics": stream_or_physics,
            "production_step": production_step,
            "data_format": dataset_format,
            "dataset_format_parser": dataset_format,
            "dataset_format_short_parser": self.make_dataset_format_short(dataset_format),
            "versionTag": version_tag,
            "amiTag": split_tag["amiTag"],
            "tid": split_tag["tid"],
            "full_tid": split_tag["full_tid"],
        }

        return parsed

    def parse_dataset_column(self, df: pd.DataFrame, column_name: str = "dataset") -> pd.DataFrame:
        parsed_df = df[column_name].apply(self.parse_full_dataset_name).apply(pd.Series)

        if parsed_df.empty:
            return df

        duplicate_cols = [c for c in parsed_df.columns if c in df.columns]
        parsed_df = parsed_df.drop(columns=duplicate_cols, errors="ignore")

        return pd.concat([df, parsed_df], axis=1)

