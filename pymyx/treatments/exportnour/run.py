from collections import defaultdict
from pathlib import Path

import pandas as pd

from pymyx.core.filename import list_parquet_files, parse_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    aggregation = params["aggregation"]
    domain = params["domain"]
    tz = params["tz"]
    date_from = params.get("from")
    date_to = params.get("to")
    columns_raw = params["columns"]
    # Support both str and dict values: {"name": "c0", "dtype": "int"} or "c0"
    col_names = {src: (v["name"] if isinstance(v, dict) else v) for src, v in columns_raw.items()}
    col_dtypes = {(v["name"] if isinstance(v, dict) else v): v["dtype"] for src, v in columns_raw.items() if isinstance(v, dict) and "dtype" in v}

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
    print(f"  [exportnour] Found {total_files} files, {len(by_device)} devices (domain={domain}, agg={aggregation})")

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

        # Check that all requested source columns exist
        missing = [c for c in col_names if c not in merged.columns]
        if missing:
            raise ValueError(f"Columns not found in data: {missing}")

        # Select and rename columns
        result = merged[["ts"] + list(col_names.keys())].copy()
        result = result.rename(columns=col_names)

        # Cast columns with dtype specified
        for col, dtype in col_dtypes.items():
            if dtype == "int":
                result[col] = result[col].round().astype("Int64")

        # Convert timezone and format Time column
        result["ts"] = result["ts"].dt.tz_convert(tz)
        result["Time"] = result["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
        result = result.drop(columns=["ts"])

        # Reorder: Time first, then columns in declared order
        output_cols = ["Time"] + list(col_names.values())
        result = result[output_cols]

        # Build output filename with actual date range from data
        first_date = result["Time"].iloc[0][:10]
        last_date = result["Time"].iloc[-1][:10]
        filename = f"{experience}_{device_id}_aggregated_{aggregation}_{first_date}_{last_date}.csv"
        out_file = out_path / filename

        result.to_csv(out_file, sep=";", index=False)
        print(f"  [exportnour] {device_id}: {len(result)} rows -> {out_file.name}")
