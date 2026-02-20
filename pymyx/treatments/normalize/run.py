from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PARAMS_FILE = "normalize_params.json"


def _domain_files(directory: Path, domain: str) -> list[Path]:
    domain_dir = directory / f"domain={domain}"
    if not domain_dir.exists():
        return []
    return sorted(domain_dir.glob("*.parquet"))


def _source_from(path: Path) -> str:
    """Extract source (device) from filename: source__domain__date.parquet"""
    return path.stem.split("__")[0]


def _cols_to_normalize(df: pd.DataFrame, columns: list[str]) -> list[str]:
    """Return columns to normalize: explicit list or all numeric."""
    if columns:
        return [c for c in columns if c in df.columns]
    return list(df.select_dtypes(include="number").columns)


def _fit_params(
    files: list[Path],
    columns: list[str],
    method: str,
    p_min: float,
    p_max: float,
) -> dict:
    """Compute normalization bounds per source per column from all input files."""
    raw: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for f in files:
        source = _source_from(f)
        df = pd.read_parquet(f)
        for col in _cols_to_normalize(df, columns):
            vals = df[col].dropna().astype(float).values
            if len(vals):
                raw[source][col].append(vals)

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


def _apply(df: pd.DataFrame, source_params: dict, clip: bool) -> pd.DataFrame:
    """Apply normalization to a dataframe using pre-computed per-column bounds."""
    df = df.copy()
    for col, bounds in source_params.items():
        if col not in df.columns:
            continue
        lo, hi = bounds["p2"], bounds["p98"]
        denom = hi - lo
        if denom == 0:
            df[col] = 0.0
        else:
            df[col] = (df[col].astype(float) - lo) / denom
        if clip:
            df[col] = df[col].clip(0.0, 1.0)
    return df


def run(input_dir: str, output_dir: str, params: dict) -> None:
    inp = Path(input_dir)
    out = Path(output_dir)

    domain       = params.get("domain", "bio_signal")
    fit          = bool(params.get("fit", False))
    method       = params.get("method", "percentile")
    p_min        = float(params.get("percentile_min", 2.0))
    p_max        = float(params.get("percentile_max", 98.0))
    columns      = params.get("columns", [])
    clip         = bool(params.get("clip", True))

    files = _domain_files(inp, domain)
    if not files:
        raise ValueError(f"No parquet files found for domain '{domain}' in {inp}")

    params_file = out / PARAMS_FILE

    if fit:
        norm_params = _fit_params(files, columns, method, p_min, p_max)
        out.mkdir(parents=True, exist_ok=True)
        payload = {
            "_meta": {
                "method": method,
                "percentile_min": p_min,
                "percentile_max": p_max,
                "fitted_at": datetime.now(timezone.utc).isoformat(),
                "n_files": len(files),
                "n_devices": len(norm_params),
            },
            **norm_params,
        }
        params_file.write_text(json.dumps(payload, indent=2))
        print(f"  [normalize] Fit: {len(norm_params)} devices, {len(files)} files -> {params_file.name}")
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
        df = _apply(df, norm_params[source], clip)
        df.to_parquet(out_domain / f.name, index=True)

    print(f"  [normalize] Applied to {len(files)} files ({domain})")
