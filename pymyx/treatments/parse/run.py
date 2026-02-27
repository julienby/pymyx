import re
from pathlib import Path

import pandas as pd

from pymyx.core.filename import FileParts, build_parquet_path, parse_raw_stem


def run(input_dir: str, output_dir: str, params: dict) -> None:
    fmt = params["format"]
    delimiter = params["delimiter"]
    tz = params["tz"]
    ts_col = params["timestamp_column"]
    domains = params["domains"]
    substitutions = params.get("file_name_substitute", [])

    # Intra-day trimming from framework-injected __time_range
    time_range = params.get("__time_range")
    trim_from = pd.Timestamp(time_range["from"]) if time_range and time_range.get("from") else None
    trim_to = pd.Timestamp(time_range["to"]) if time_range and time_range.get("to") else None

    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(in_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    print(f"  [parse] Found {len(csv_files)} CSV files")

    for csv_file in csv_files:
        df = parse_file(csv_file, fmt, delimiter, ts_col, tz)
        if df.empty:
            continue

        experience, device_id, _ = parse_raw_stem(csv_file.stem, substitutions)

        # Write parquet per domain per day
        for domain_name, domain_spec in domains.items():
            cols = resolve_columns(df.columns.tolist(), domain_spec)
            if not cols:
                continue

            domain_df = df[["ts"] + cols].copy()
            dtype = domain_spec.get("dtype", "float")
            for c in cols:
                numeric = pd.to_numeric(domain_df[c], errors="coerce")
                if dtype == "int":
                    non_int = numeric.notna() & (numeric != numeric.round(0))
                    numeric[non_int] = pd.NA
                    domain_df[c] = numeric.astype("Int64")
                else:
                    domain_df[c] = numeric.astype("Float64")

            rename = domain_spec.get("rename", {})
            if rename:
                domain_df = domain_df.rename(columns=rename)

            # Trim rows on boundary days
            if trim_from is not None:
                domain_df = domain_df[domain_df["ts"] >= trim_from]
            if trim_to is not None:
                domain_df = domain_df[domain_df["ts"] <= trim_to]
            if domain_df.empty:
                continue

            for day, day_df in domain_df.groupby(domain_df["ts"].dt.date):
                parts = FileParts(
                    experience=experience,
                    device_id=device_id,
                    step="parsed",
                    day=str(day),
                    domain=domain_name,
                )
                out_file = build_parquet_path(parts, out_path)
                day_df.to_parquet(out_file, index=False)

    count = len(list(out_path.rglob("*.parquet")))
    print(f"  [parse] Wrote {count} parquet files to {out_path}")


def parse_file(csv_file: Path, fmt: str, delimiter: str, ts_col: str, tz: str) -> pd.DataFrame:
    if fmt == "kv_csv":
        return parse_kv_csv(csv_file, delimiter, ts_col, tz)
    raise ValueError(f"Unknown parse format: {fmt!r}")


def parse_kv_csv(csv_file: Path, delimiter: str, ts_col: str, tz: str) -> pd.DataFrame:
    """KV-CSV format: timestamp (Zulu) followed by x key:value columns.

    The first field is the timestamp (identified by ts_col in the source CSV).
    Output timestamp column is always named 'ts' for pipeline consistency.
    Rule: lines without at least one key:value pair are silently discarded.
    """
    rows = []
    with open(csv_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(delimiter)
            record = {"ts": parts[0]}
            for part in parts[1:]:
                if ":" not in part:
                    continue
                key, val = part.split(":", 1)
                record[key] = val
            if len(record) > 1:
                rows.append(record)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", utc=(tz == "UTC"))
    return df


def resolve_columns(all_cols: list[str], domain_spec: dict) -> list[str]:
    if "columns" in domain_spec:
        return [c for c in domain_spec["columns"] if c in all_cols]
    if "prefix" in domain_spec:
        prefix = domain_spec["prefix"]
        return sorted(
            [c for c in all_cols if re.match(rf"^{re.escape(prefix)}\d+$", c)],
            key=lambda c: int(c[len(prefix):]),
        )
    return []
