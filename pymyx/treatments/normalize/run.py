from __future__ import annotations

import fnmatch
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PARAMS_FILE = "normalize_params.json"


def _domain_files(directory: Path, domain: str) -> list[Path]:
    domain_dir = directory / f"domain={domain}"
    if not domain_dir.exists():
        return []
    return sorted(domain_dir.glob("*.parquet"))


def _date_from(path: Path) -> date | None:
    """Extract date from filename: source__domain__YYYY-MM-DD.parquet"""
    parts = path.stem.split("__")
    if len(parts) >= 3:
        try:
            return datetime.strptime(parts[-1], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _apply_window(files: list[Path], window_days: int) -> list[Path]:
    """Keep only files within the last `window_days` days of available data."""
    if window_days <= 0:
        return files
    dates = [_date_from(f) for f in files]
    valid_dates = [d for d in dates if d is not None]
    if not valid_dates:
        return files
    cutoff = max(valid_dates) - timedelta(days=window_days - 1)
    return [f for f, d in zip(files, dates) if d is not None and d >= cutoff]


def _source_from(path: Path) -> str:
    """Extract source (device) from filename: source__domain__date.parquet"""
    return path.stem.split("__")[0]


def _resolve_output_col(in_col: str, in_pattern: str, out_pattern: str) -> str:
    """Compute output column name from matched input column and patterns.

    Example: in_col="m0__sqrt_inv", in_pattern="*__sqrt_inv", out_pattern="*__sqrt_inv__norm"
             -> "m0__sqrt_inv__norm"
    """
    if "*" not in in_pattern:
        return out_pattern
    suffix = in_pattern.lstrip("*")
    prefix = in_col[:-len(suffix)] if suffix else in_col
    return out_pattern.replace("*", prefix, 1)


def _resolve_column_pairs(df: pd.DataFrame, columns_spec) -> list[tuple[str, str]]:
    """Return list of (input_col, output_col) pairs to normalize.

    columns_spec can be:
    - [] or None : all numeric columns, in-place replacement
    - ["m0", "m1"] : explicit list, in-place replacement
    - {"*__sqrt_inv": "*__sqrt_inv__norm"} : wildcard patterns, new columns added
    """
    numeric_cols = list(df.select_dtypes(include="number").columns)

    if not columns_spec:
        return [(c, c) for c in numeric_cols]

    if isinstance(columns_spec, list):
        return [(c, c) for c in columns_spec if c in df.columns]

    if isinstance(columns_spec, dict):
        result = []
        seen_out = set()
        for in_pattern, out_pattern in columns_spec.items():
            for col in numeric_cols:
                if fnmatch.fnmatch(col, in_pattern):
                    out_col = _resolve_output_col(col, in_pattern, out_pattern)
                    if out_col not in seen_out:
                        result.append((col, out_col))
                        seen_out.add(out_col)
        return result

    return []


def _fit_params(
    files: list[Path],
    columns_spec,
    method: str,
    p_min: float,
    p_max: float,
) -> dict:
    """Compute normalization bounds per source per input column."""
    raw: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for f in files:
        source = _source_from(f)
        df = pd.read_parquet(f)
        for in_col, _ in _resolve_column_pairs(df, columns_spec):
            vals = df[in_col].dropna().astype(float).values
            if len(vals):
                raw[source][in_col].append(vals)

    params = {}
    for source, col_data in raw.items():
        params[source] = {}
        for col, arrays in col_data.items():
            all_vals = np.concatenate(arrays)
            if method == "percentile":
                lo = float(np.percentile(all_vals, p_min))
                hi = float(np.percentile(all_vals, p_max))
            else:  # minmax
                lo = float(np.min(all_vals))
                hi = float(np.max(all_vals))
            params[source][col] = {"p2": round(lo, 6), "p98": round(hi, 6)}

    return params


def _check_ranges(norm_params: dict, min_range: float) -> None:
    """Warn for any (device, column) whose fitting range is suspiciously small."""
    if min_range <= 0:
        return
    issues = []
    for source, col_data in norm_params.items():
        for col, bounds in col_data.items():
            rng = bounds["p98"] - bounds["p2"]
            if rng < min_range:
                issues.append(
                    f"    {source}/{col}: range={rng:.2f} "
                    f"(p2={bounds['p2']:.2f}, p98={bounds['p98']:.2f})"
                )
    if issues:
        print(
            f"  [normalize] WARNING: {len(issues)} column(s) have range < {min_range} "
            f"— window may not capture full behavioral range:"
        )
        for msg in issues:
            print(msg)


def _apply(df: pd.DataFrame, source_params: dict, columns_spec, clip: bool) -> pd.DataFrame:
    """Apply normalization. Adds new columns when out_col != in_col, replaces in-place otherwise."""
    df = df.copy()
    for in_col, out_col in _resolve_column_pairs(df, columns_spec):
        if in_col not in source_params:
            continue
        lo, hi = source_params[in_col]["p2"], source_params[in_col]["p98"]
        denom = hi - lo
        if denom == 0:
            normalized = pd.Series(0.0, index=df.index)
        else:
            normalized = (df[in_col].astype(float) - lo) / denom
        if clip:
            normalized = normalized.clip(0.0, 1.0)
        df[out_col] = normalized
    return df


def run(input_dir: str, output_dir: str, params: dict) -> None:
    inp = Path(input_dir)
    out = Path(output_dir)

    domain          = params.get("domain", "bio_signal")
    fit             = bool(params.get("fit", False))
    method          = params.get("method", "percentile")
    p_min           = float(params.get("percentile_min", 2.0))
    p_max           = float(params.get("percentile_max", 98.0))
    columns         = params.get("columns", [])
    clip            = bool(params.get("clip", True))
    fit_window_days = int(params.get("fit_window_days", 0))
    min_range_warn  = float(params.get("min_range_warn", 0))

    files = _domain_files(inp, domain)
    if not files:
        raise ValueError(f"No parquet files found for domain '{domain}' in {inp}")

    params_file = out / PARAMS_FILE

    if fit:
        fit_files = _apply_window(files, fit_window_days)
        if not fit_files:
            raise ValueError(
                f"fit_window_days={fit_window_days} excluded all files. "
                f"Reduce the window or check available data dates."
            )
        if fit_window_days > 0:
            dates = sorted(set(d for f in fit_files if (d := _date_from(f))))
            print(f"  [normalize] Fit window: {dates[0]} -> {dates[-1]} ({len(fit_files)} files)")

        norm_params = _fit_params(fit_files, columns, method, p_min, p_max)
        _check_ranges(norm_params, min_range_warn)

        out.mkdir(parents=True, exist_ok=True)
        payload = {
            "_meta": {
                "method": method,
                "percentile_min": p_min,
                "percentile_max": p_max,
                "fit_window_days": fit_window_days if fit_window_days > 0 else "all",
                "fitted_at": datetime.now(timezone.utc).isoformat(),
                "n_files": len(fit_files),
                "n_devices": len(norm_params),
            },
            **norm_params,
        }
        params_file.write_text(json.dumps(payload, indent=2))
        print(f"  [normalize] Fit: {len(norm_params)} devices, {len(fit_files)} files -> {params_file.name}")
    else:
        if not params_file.exists():
            raise FileNotFoundError(
                f"{PARAMS_FILE} not found in {out}. "
                f"Run with fit=true first to compute normalization params."
            )
        full = json.loads(params_file.read_text())
        norm_params = {k: v for k, v in full.items() if not k.startswith("_")}

    # Apply normalization to all input files
    out_domain = out / f"domain={domain}"
    out_domain.mkdir(parents=True, exist_ok=True)

    for f in files:
        source = _source_from(f)
        if source not in norm_params:
            raise KeyError(
                f"No params for source '{source}' in {PARAMS_FILE}. "
                f"Re-run with fit=true to include this device."
            )
        df = pd.read_parquet(f)
        df = _apply(df, norm_params[source], columns, clip)
        df.to_parquet(out_domain / f.name, index=True)

    print(f"  [normalize] Applied to {len(files)} files ({domain})")
