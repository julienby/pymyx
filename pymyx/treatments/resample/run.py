from pathlib import Path

import pandas as pd

from pymyx.core.filename import build_parquet_path, list_parquet_files, parse_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    freq = params["freq"]
    max_gap_fill_s = params["max_gap_fill_s"]
    agg_method = params["agg_method"]

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [resample] Found {len(parquet_files)} parquet files")

    stats = {"files": 0, "rows_in": 0, "rows_out": 0, "filled": 0}

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        domain_name = parts.domain
        method = agg_method.get(domain_name, "mean")

        df = pd.read_parquet(pf)
        stats["rows_in"] += len(df)

        out_parts = parts.with_step("resampled")

        if df.empty:
            df.to_parquet(build_parquet_path(out_parts, out_path), index=False)
            stats["files"] += 1
            continue

        data_cols = [c for c in df.columns if c != "ts"]
        original_dtypes = {c: str(df[c].dtype) for c in data_cols}

        # Floor timestamps to second precision
        df["ts"] = df["ts"].dt.floor("s")

        # Aggregate duplicates created by flooring
        df = df.groupby("ts", as_index=False).agg(
            {c: agg_func(method) for c in data_cols}
        )

        # Drop leading all-null rows (sensor connected but not yet sending data)
        first_valid = df[data_cols].first_valid_index()
        if first_valid is None:
            # Entire file is null — write empty
            df = df.reset_index()
            df.to_parquet(build_parquet_path(out_parts, out_path), index=False)
            stats["files"] += 1
            continue
        df = df.loc[first_valid:]

        # Build 1s grid from first valid data point to last
        data_start = df["ts"].min().floor(freq)
        data_end = df["ts"].max()
        full_index = pd.date_range(start=data_start, end=data_end, freq=freq, tz="UTC")

        # Reindex onto the regular grid
        df = df.set_index("ts").reindex(full_index)
        df.index.name = "ts"

        # Forward-fill small gaps only (up to max_gap_fill_s consecutive NaNs)
        if max_gap_fill_s > 0:
            before_na = df[data_cols].isna().sum().sum()
            df[data_cols] = df[data_cols].ffill(limit=max_gap_fill_s)
            after_na = df[data_cols].isna().sum().sum()
            stats["filled"] += int(before_na - after_na)

        # Preserve original dtypes: round Int64 columns back to int, keep Float64 as-is
        for c in data_cols:
            if original_dtypes[c] == "Int64":
                df[c] = df[c].round().astype("Int64")
            else:
                df[c] = df[c].astype("Float64")

        df = df.reset_index()
        stats["rows_out"] += len(df)
        stats["files"] += 1
        df.to_parquet(build_parquet_path(out_parts, out_path), index=False)

    print(f"  [resample] {stats['files']} files, {stats['rows_in']:,} -> {stats['rows_out']:,} rows")
    print(f"  [resample] gap-filled: {stats['filled']:,} values (ffill limit={max_gap_fill_s}s)")


def agg_func(method: str):
    if method == "nearest":
        return "first"
    return method
