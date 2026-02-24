from pathlib import Path

import numpy as np
import pandas as pd

from pymyx.core.filename import build_parquet_path, list_parquet_files, parse_parquet_path


TRANSFORMS = {
    "sqrt_inv": lambda s: 1.0 / np.sqrt(s.where(s > 0, other=np.nan)),
    "cbrt_inv": lambda s: 1.0 / np.cbrt(s.where(s > 0, other=np.nan)),
    "log": lambda s: np.log(s.where(s > 0, other=np.nan)),
}


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    transforms = params["transforms"]

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [transform] Found {len(parquet_files)} parquet files")

    stats = {"files": 0, "cols_added": 0, "cols_replaced": 0}

    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        out_parts = parts.with_step("transform")

        df = pd.read_parquet(pf)

        if not transforms:
            df.to_parquet(build_parquet_path(out_parts, out_path), index=False)
            stats["files"] += 1
            continue

        # Capture original columns before any transforms add new ones
        original_columns = df.columns.tolist()

        for spec in transforms:
            func_name = spec["function"]
            if func_name not in TRANSFORMS:
                raise ValueError(
                    f"Unknown transform function '{func_name}'. "
                    f"Available: {sorted(TRANSFORMS)}"
                )

            target_cols = _resolve_target(spec["target"], original_columns, parts.domain)
            if not target_cols:
                continue

            mode = spec.get("mode", "add")
            func = TRANSFORMS[func_name]

            for col in target_cols:
                transformed = func(df[col].astype("Float64")).astype("Float64")

                if mode == "replace":
                    df[col] = transformed
                    stats["cols_replaced"] += 1
                else:
                    new_col = f"{col}__{func_name}"
                    df[new_col] = transformed
                    stats["cols_added"] += 1

        if any(spec.get("mode", "add") == "add" for spec in transforms):
            df = _reorder_columns(df, transforms, parts.domain)

        stats["files"] += 1
        df.to_parquet(build_parquet_path(out_parts, out_path), index=False)

    print(f"  [transform] {stats['files']} files, {stats['cols_added']} cols added, {stats['cols_replaced']} cols replaced")


def _resolve_target(target: dict, columns: list | pd.Index, file_domain: str) -> list[str]:
    col_set = set(columns)
    if "columns" in target:
        return [c for c in target["columns"] if c in col_set]
    if "domain" in target:
        if target["domain"] != file_domain:
            return []
        return [c for c in columns if c != "ts"]
    return []


def _reorder_columns(df: pd.DataFrame, transforms: list[dict], file_domain: str) -> pd.DataFrame:
    original_cols = [c for c in df.columns if c == "ts" or not any(
        c.endswith(f"__{spec['function']}") for spec in transforms if spec.get("mode", "add") == "add"
    )]

    ordered = []
    for col in original_cols:
        ordered.append(col)
        for spec in transforms:
            if spec.get("mode", "add") != "add":
                continue
            suffix_col = f"{col}__{spec['function']}"
            if suffix_col in df.columns:
                ordered.append(suffix_col)

    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]
