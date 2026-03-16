from collections import defaultdict
from pathlib import Path

import pandas as pd

from pyperun.core.filename import list_parquet_files, parse_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    aggregation = params["aggregation"]
    domain = params["domain"]
    date_from = params.get("from")
    date_to = params.get("to")
    columns_map = params.get("columns", {})  # {src_col: dest_col} or empty = keep all

    # Find matching parquet files (right domain + aggregation), grouped by device
    parquet_files = list_parquet_files(in_path)
    by_device = defaultdict(list)
    experience = None

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        if parts.domain != domain:
            continue
        if parts.aggregation != aggregation:
            continue
        if date_from and parts.day < date_from:
            continue
        if date_to and parts.day > date_to:
            continue
        by_device[parts.device_id].append(pf)
        if experience is None:
            experience = parts.experience

    if not by_device:
        raise FileNotFoundError(
            f"No parquet files found matching domain={domain}, aggregation={aggregation}"
        )

    total_files = sum(len(v) for v in by_device.values())
    print(f"  [exportparquet] Found {total_files} files, {len(by_device)} devices (domain={domain}, agg={aggregation})")

    for device_id, files in sorted(by_device.items()):
        frames = []
        for pf in files:
            df = pd.read_parquet(pf)
            if not df.empty:
                frames.append(df)

        if not frames:
            continue

        merged = pd.concat(frames, ignore_index=True)
        merged = merged.sort_values("ts").reset_index(drop=True)

        # Filter by from/to on actual timestamps
        if date_from:
            merged = merged[merged["ts"] >= pd.Timestamp(date_from, tz="UTC")]
        if date_to:
            merged = merged[merged["ts"] < pd.Timestamp(date_to, tz="UTC") + pd.Timedelta(days=1)]

        # Select and rename columns (or keep all if columns is empty)
        if columns_map:
            missing = [c for c in columns_map if c not in merged.columns]
            if missing:
                print(f"  [exportparquet] {device_id}: skipping missing columns: {missing}")
            available = {k: v for k, v in columns_map.items() if k in merged.columns}
            if not available:
                print(f"  [exportparquet] {device_id}: no columns available, skipping")
                continue
            result = merged[["ts"] + list(available.keys())].copy()
            result = result.rename(columns=available)
        else:
            result = merged.copy()

        # Build output filename with actual date range from data
        first_date = result["ts"].iloc[0].strftime("%Y-%m-%d")
        last_date = result["ts"].iloc[-1].strftime("%Y-%m-%d")
        filename = f"{experience}_{device_id}_aggregated_{aggregation}_{first_date}_{last_date}.parquet"
        out_file = out_path / filename

        result.to_parquet(out_file, index=False)
        print(f"  [exportparquet] {device_id}: {len(result)} rows -> {out_file.name}")
