from pathlib import Path

import pandas as pd

from pymyx.core.filename import build_parquet_path, list_parquet_files, parse_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    drop_duplicates = params["drop_duplicates"]
    domains = params["domains"]

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [clean] Found {len(parquet_files)} parquet files")

    stats = {"files": 0, "rows_in": 0, "rows_out": 0, "dupes_dropped": 0, "bounds_nan": 0, "spikes_nan": 0}

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        domain_name = parts.domain
        domain_cfg = domains.get(domain_name)

        out_parts = parts.with_step("clean")

        if domain_cfg is None:
            # Unknown domain, pass through unchanged
            df = pd.read_parquet(pf)
            df.to_parquet(build_parquet_path(out_parts, out_path), index=False)
            stats["files"] += 1
            continue

        df = pd.read_parquet(pf)
        stats["rows_in"] += len(df)
        data_cols = [c for c in df.columns if c != "ts"]

        # 1. Drop duplicate timestamps
        if drop_duplicates:
            before = len(df)
            df = df.drop_duplicates(subset="ts", keep="first")
            stats["dupes_dropped"] += before - len(df)

        # 2. Min/max bounds -> NaN
        min_val = domain_cfg.get("min_value")
        max_val = domain_cfg.get("max_value")
        if min_val is not None or max_val is not None:
            for c in data_cols:
                mask = pd.Series(False, index=df.index)
                if min_val is not None:
                    mask |= df[c] < min_val
                if max_val is not None:
                    mask |= df[c] > max_val
                # Only count non-NaN values that get clipped
                mask &= df[c].notna()
                n = mask.sum()
                if n > 0:
                    df.loc[mask, c] = pd.NA
                    stats["bounds_nan"] += n

        # 3. Spike detection via rolling median
        spike_window = domain_cfg.get("spike_window", 7)
        spike_threshold = domain_cfg.get("spike_threshold")
        if spike_threshold is not None:
            for c in data_cols:
                rolling_med = df[c].rolling(window=spike_window, center=True, min_periods=1).median()
                deviation = (df[c] - rolling_med).abs()
                spike_mask = (deviation > spike_threshold) & df[c].notna()
                n = spike_mask.sum()
                if n > 0:
                    df.loc[spike_mask, c] = pd.NA
                    stats["spikes_nan"] += n

        stats["rows_out"] += len(df)
        stats["files"] += 1
        df.to_parquet(build_parquet_path(out_parts, out_path), index=False)

    print(f"  [clean] {stats['files']} files, {stats['rows_in']:,} -> {stats['rows_out']:,} rows")
    print(f"  [clean] dupes dropped: {stats['dupes_dropped']}, bounds->NaN: {stats['bounds_nan']}, spikes->NaN: {stats['spikes_nan']}")
