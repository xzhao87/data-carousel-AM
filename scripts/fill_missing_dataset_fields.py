#!/usr/bin/env python3
"""
fill_missing_dataset_fields.py

Fix ONLY these columns if missing:
  - scope
  - dataset_format
  - dataset_format_short

Rules:
  - Do NOT modify any other columns
  - Do NOT convert "None" to empty
  - Only fill values when missing (empty or NaN)

Usage:
  python tools/fill_missing_dataset_fields.py \
      --input data/prodsyslogs.csv \
      --output data/fixed.csv

  python tools/fill_missing_dataset_fields.py \
      --input data/prodsyslogs.csv \
      --inplace
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


def is_missing(value) -> bool:
    """
    Treat as missing only if:
      - NaN
      - empty string
    BUT NOT the string "None"
    """
    if pd.isna(value):
        return True
    if isinstance(value, str) and value == "":
        return True
    return False


def make_dataset_format_short(fmt: Optional[str]) -> Optional[str]:
    if not fmt:
        return None
    if fmt.startswith("DAOD_"):
        return "DAOD"
    if fmt.startswith("NTUP_"):
        return "NTUP"
    return fmt


def parse_dataset(dataset: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract:
      scope
      dataset_format
      dataset_format_short
    """
    if not isinstance(dataset, str) or dataset.strip() == "":
        return None, None, None

    dataset = dataset.strip()

    if ":" in dataset:
        scope, name_part = dataset.split(":", 1)
    else:
        scope = dataset.split(".")[0]
        name_part = dataset

    tokens = name_part.split(".")
    if len(tokens) < 5:
        return scope, None, None

    fmt = tokens[4]
    fmt_short = make_dataset_format_short(fmt)

    return scope, fmt, fmt_short


def fill(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    rows_updated = 0

    for idx in df.index:
        dataset = df.at[idx, "dataset"] if "dataset" in df.columns else None

        if not isinstance(dataset, str):
            continue

        scope_missing = is_missing(df.at[idx, "scope"]) if "scope" in df.columns else False
        fmt_missing = is_missing(df.at[idx, "dataset_format"]) if "dataset_format" in df.columns else False
        fmt_short_missing = is_missing(df.at[idx, "dataset_format_short"]) if "dataset_format_short" in df.columns else False

        if not (scope_missing or fmt_missing or fmt_short_missing):
            continue

        parsed_scope, parsed_fmt, parsed_fmt_short = parse_dataset(dataset)

        updated = False

        if scope_missing and parsed_scope is not None:
            df.at[idx, "scope"] = parsed_scope
            updated = True

        if fmt_missing and parsed_fmt is not None:
            df.at[idx, "dataset_format"] = parsed_fmt
            updated = True

        if fmt_short_missing and parsed_fmt_short is not None:
            df.at[idx, "dataset_format_short"] = parsed_fmt_short
            updated = True

        if updated:
            rows_updated += 1

    return df, rows_updated


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fix missing scope/dataset_format/dataset_format_short without touching other columns."
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--output")
    parser.add_argument("--inplace", action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()

    input_path = Path(args.input)

    if not input_path.exists():
        raise FileNotFoundError(input_path)

    if not args.inplace and not args.output:
        raise ValueError("Provide --output or use --inplace")

    # IMPORTANT: preserve "None" exactly
    df = pd.read_csv(
        input_path,
        keep_default_na=False,   # <- critical
        na_values=[],            # <- critical
        dtype=str                # <- avoid type coercion
    )

    df, updated = fill(df)

    print(f"Rows updated: {updated}")

    output_path = input_path if args.inplace else Path(args.output)

    df.to_csv(output_path, index=False)

    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()

