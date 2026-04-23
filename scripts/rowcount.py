#!/usr/bin/env python3
"""Show row counts per day and per step for a dataset within a date range.

Usage:
    python scripts/rowcount.py DATASET --from 2026-04-10 --to 2026-04-23
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

STEPS = [
    "00_raw",
    "10_parsed",
    "20_clean",
    "25_resampled",
    "30_transform",
    "35_normalized",
    "40_aggregated",
]

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def count_rows(path: Path) -> int:
    if path.suffix == ".parquet":
        return pq.read_metadata(path).num_rows
    if path.suffix == ".csv":
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    return 0


def collect(dataset_path: Path, step: str, date_from: str, date_to: str) -> dict[str, int]:
    step_path = dataset_path / step
    if not step_path.exists():
        return {}

    rows_by_day: dict[str, int] = defaultdict(int)
    for f in step_path.rglob("*"):
        if f.suffix not in (".parquet", ".csv"):
            continue
        m = DATE_RE.search(f.name)
        if not m:
            continue
        day = m.group(1)
        if day < date_from or day > date_to:
            continue
        rows_by_day[day] += count_rows(f)

    return dict(rows_by_day)


def main():
    parser = argparse.ArgumentParser(description="Row counts per day and step")
    parser.add_argument("dataset", help="Dataset name (e.g. Expo_pre_GRACE_2)")
    parser.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--steps", nargs="+", default=None, help="Steps to show (default: all)")
    args = parser.parse_args()

    root = Path(__file__).parent.parent / "datasets" / args.dataset
    if not root.exists():
        print(f"Dataset not found: {root}", file=sys.stderr)
        sys.exit(1)

    steps = args.steps or STEPS
    steps = [s for s in steps if (root / s).exists()]

    # Collect all data
    data: dict[str, dict[str, int]] = {}  # step -> day -> rows
    all_days: set[str] = set()
    for step in steps:
        data[step] = collect(root, step, args.date_from, args.date_to)
        all_days |= data[step].keys()

    if not all_days:
        print("No data found for this date range.")
        sys.exit(0)

    days = sorted(all_days)

    # Column widths
    step_labels = [s.split("_", 1)[-1] if "_" in s else s for s in steps]  # strip leading digits
    col_w = max(10, *(len(s) for s in step_labels)) + 2

    # Header
    header = f"{'Day':<12}" + "".join(f"{s:>{col_w}}" for s in step_labels)
    print(header)
    print("-" * len(header))

    for day in days:
        row = f"{day:<12}"
        for step in steps:
            n = data[step].get(day)
            row += f"{n if n is not None else '-':>{col_w}}"
        print(row)


if __name__ == "__main__":
    main()
