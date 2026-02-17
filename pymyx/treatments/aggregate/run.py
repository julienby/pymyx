from pathlib import Path

import pandas as pd

from pymyx.core.filename import build_parquet_path, list_parquet_files, parse_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    windows = params["windows"]
    metrics = params["metrics"]

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [aggregate] Found {len(parquet_files)} parquet files")
    print(f"  [aggregate] Windows: {windows}, Metrics: {metrics}")

    stats = {"files_in": 0, "files_out": 0}

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        df = pd.read_parquet(pf)
        stats["files_in"] += 1

        if df.empty:
            continue

        data_cols = [c for c in df.columns if c != "ts"]
        df = df.set_index("ts")

        for window in windows:
            agg_df = df[data_cols].resample(window).agg(metrics)

            # Flatten MultiIndex columns: m0__raw__mean, m0__sqrt_inv__mean, ...
            agg_df.columns = [
                f"{col}__{metric}" if "__" in col else f"{col}__raw__{metric}"
                for col, metric in agg_df.columns
            ]

            agg_df = agg_df.reset_index()

            out_parts = parts.with_aggregation("aggregated", window)
            agg_df.to_parquet(build_parquet_path(out_parts, out_path), index=False)
            stats["files_out"] += 1

    print(f"  [aggregate] {stats['files_in']} files -> {stats['files_out']} aggregated files")
