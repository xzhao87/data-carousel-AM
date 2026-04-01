#!/usr/bin/env python3
"""
split-csv.py

Splits a recall history CSV into data-format / origin subsets and then further
splits each subset by task_type (production vs analysis).

Usage
-----
  python split-csv.py -f data-carousel-AM/data/prodsyslogs.csv

Output files (written into the same directory as the input file)
----------------------------------------------------------------
  new-RAW.csv          RAW  + data origin
  new-data-AOD.csv     AOD  + data origin
  new-mc-AOD.csv       AOD  + mc origin
  new-mc-HITS.csv      HITS + mc origin
  others.csv           all remaining rows

For each base file, two task-type splits are also produced:
  <base>-production.csv   task_type == production
  <base>-analysis.csv     task_type == analysis

Total: 5 base files + 10 task-type split files = 15 new CSV files.

Column-name compatibility
-------------------------
Supports both the old CSV format and the new CSV format (prodsyslogs.csv).
Old name           New name
-----------        ----------------------
dataType           dataset_format_short
datamc             dataset_origin
"""

import argparse
import os
import sys

import pandas as pd


# ---------------------------------------------------------------------------
# Column-name aliases: (old format name) ↔ (new format name)
# The script detects which format is in use and normalises internally.
# ---------------------------------------------------------------------------
COL_ALIASES = {
    "timeEvent":            "asctime",
    "source_tape":          "resource",
    "total_files":          "files",
    "dataset_origin":       "datamc",
    "dataset_format_short": "dataType",
    "production_step":      "productionStep",
    "runNumber/datasetID":  "runNumber",
    "physicsShort/StreamName": "StreamName",
}


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename new-format columns to the canonical old-format names where needed."""
    return df.rename(columns={k: v for k, v in COL_ALIASES.items() if k in df.columns})


def write_subset(df: pd.DataFrame, path: str, label: str) -> None:
    df.to_csv(path, index=False)
    print(f"  {len(df):>7,} rows  →  {path}  ({label})")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Split a recall history CSV by data format/origin and task type."
    )
    parser.add_argument(
        "-f", "--file", required=True,
        help="Input CSV file (e.g. data-carousel-AM/data/prodsyslogs.csv)"
    )
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"ERROR: file not found: {args.file}", file=sys.stderr)
        return 2

    print(f"Reading {args.file} …")
    df = pd.read_csv(args.file)
    df = normalise_columns(df)

    # Validate required columns
    for col in ("dataType", "datamc", "task_type"):
        if col not in df.columns:
            print(f"ERROR: required column '{col}' not found in {args.file}", file=sys.stderr)
            return 2

    out_dir = os.path.dirname(os.path.abspath(args.file))

    # ------------------------------------------------------------------
    # Primary filters (applied on canonical column names after normalise)
    # ------------------------------------------------------------------
    fmt    = df["dataType"].str.upper()
    origin = df["datamc"].str.lower()

    masks = {
        "new-RAW":      (fmt == "RAW")  & (origin == "data"),
        "new-data-AOD": (fmt == "AOD")  & (origin == "data"),
        "new-mc-AOD":   (fmt == "AOD")  & (origin == "mc"),
        "new-mc-HITS":  (fmt == "HITS") & (origin == "mc"),
    }

    selected = masks["new-RAW"] | masks["new-data-AOD"] | masks["new-mc-AOD"] | masks["new-mc-HITS"]
    masks["others"] = ~selected

    # ------------------------------------------------------------------
    # Write base files + production/analysis splits
    # ------------------------------------------------------------------
    print(f"\nOutput directory: {out_dir}\n")

    for base_name, mask in masks.items():
        subset = df[mask].copy()
        base_path = os.path.join(out_dir, f"{base_name}.csv")
        write_subset(subset, base_path, "all")

        task_col = subset["task_type"].str.lower()
        for task_type in ("production", "analysis"):
            split = subset[task_col == task_type]
            split_path = os.path.join(out_dir, f"{base_name}-{task_type}.csv")
            write_subset(split, split_path, task_type)

        print()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
