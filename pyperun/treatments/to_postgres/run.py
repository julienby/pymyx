from __future__ import annotations

import fnmatch
import io
import re
from collections import defaultdict
from itertools import product
from pathlib import Path

import pandas as pd
import psycopg2

from pyperun.core.filename import list_parquet_files, parse_parquet_path

_PG_TYPE_MAP = {
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
    "Int64": "BIGINT",
    "Float64": "DOUBLE PRECISION",
    "float64": "DOUBLE PRECISION",
    "int64": "BIGINT",
}


def _pg_type(dtype) -> str:
    key = str(dtype)
    if key in _PG_TYPE_MAP:
        return _PG_TYPE_MAP[key]
    if "datetime" in key:
        return "TIMESTAMPTZ"
    if "int" in key.lower():
        return "BIGINT"
    if "float" in key.lower():
        return "DOUBLE PRECISION"
    return "TEXT"


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "_", name)


def _render_table_name(template: str, parts: dict) -> str:
    rendered = template
    for key, val in parts.items():
        placeholder = "{" + key + "}"
        if val is None:
            rendered = rendered.replace("_" + placeholder, "")
            rendered = rendered.replace(placeholder + "_", "")
            rendered = rendered.replace(placeholder, "")
        else:
            rendered = rendered.replace(placeholder, val)
    return _sanitize(rendered).upper()


def _resolve_allowed_columns(source: dict) -> list[str] | set | None:
    if "columns" in source:
        return source["columns"]
    sensors = source.get("sensors")
    transforms = source.get("transforms")
    metrics = source.get("metrics")
    if not any([sensors, transforms, metrics]):
        return None
    parts_lists = [sensors or [None], transforms or [None], metrics or [None]]
    allowed = set()
    for combo in product(*parts_lists):
        allowed.add(tuple(combo))
    return allowed


def _matches_structured_filter(col: str, allowed) -> bool:
    parts = col.split("__")
    if len(parts) != 3:
        return False
    sensor, transform, metric = parts
    for pattern in allowed:
        if (
            (pattern[0] is None or fnmatch.fnmatch(sensor, pattern[0]))
            and (pattern[1] is None or fnmatch.fnmatch(transform, pattern[1]))
            and (pattern[2] is None or fnmatch.fnmatch(metric, pattern[2]))
        ):
            return True
    return False


def _find_source(sources: list[dict], domain: str, device_id: str) -> dict | None:
    for s in sources:
        if s["domain"] != domain:
            continue
        devices = s.get("devices")
        if devices and device_id not in devices:
            continue
        return s
    return None


def _pivot_wide(files: list[Path], sources: list[dict]) -> pd.DataFrame:
    """Read parquet files, prefix columns with device_id, merge on ts (outer join)."""
    frames = []

    for f in files:
        parts = parse_parquet_path(f)
        source = _find_source(sources, parts.domain, parts.device_id)
        if source is None:
            continue

        df = pd.read_parquet(f)
        if df.empty:
            continue

        allowed = _resolve_allowed_columns(source)
        data_cols = [c for c in df.columns if c != "ts"]

        if allowed is not None:
            if isinstance(allowed, list):
                data_cols = [c for c in data_cols if c in allowed]
            else:
                data_cols = [c for c in data_cols if _matches_structured_filter(c, allowed)]

        if not data_cols:
            continue

        device_prefix = _sanitize(parts.device_id)
        rename_map = {c: f"{device_prefix}__{c}" for c in data_cols}
        df = df[["ts"] + data_cols].rename(columns=rename_map)
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for other in frames[1:]:
        result = result.merge(other, on="ts", how="outer")

    return result.sort_values("ts").reset_index(drop=True)


def _ensure_table(conn, table_name: str, df: pd.DataFrame) -> None:
    cols = []
    for col in df.columns:
        if col == "ts":
            cols.append("ts TIMESTAMPTZ PRIMARY KEY")
        else:
            cols.append(f'"{col}" {_pg_type(df[col].dtype)}')
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)})'
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _ensure_columns(conn, table_name: str, df: pd.DataFrame) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        existing = {row[0] for row in cur.fetchall()}

    added = []
    for col in df.columns:
        if col not in existing:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {_pg_type(df[col].dtype)}'
                )
            added.append(col)

    if added:
        conn.commit()
    return added


def _copy_to_postgres(conn, table_name: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    columns = ", ".join(f'"{c}"' for c in df.columns)
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY "{table_name}" ({columns}) FROM STDIN WITH (FORMAT csv, NULL \'\')',
            buf,
        )
    conn.commit()
    return len(df)


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    sources = params["sources"]
    table_prefix = params.get("table_prefix", "")
    table_template = params["table_template"]
    aggregations = params.get("aggregations", [])

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [to_postgres] Found {len(parquet_files)} parquet files")

    # Group files by table (experience, step, aggregation) → day → [files]
    groups: dict[tuple, dict[str, list[Path]]] = defaultdict(lambda: defaultdict(list))
    for pf in parquet_files:
        parts = parse_parquet_path(pf)
        group_key = (parts.experience, parts.step, parts.aggregation)
        groups[group_key][parts.day].append(pf)

    conn = psycopg2.connect(
        host=params["host"],
        port=params["port"],
        dbname=params["dbname"],
        user=params["user"],
        password=params["password"],
    )

    total_rows = 0
    total_days = 0

    try:
        for (experience, step, aggregation), days in sorted(groups.items()):
            if aggregations and aggregation not in aggregations:
                continue

            table_name = _render_table_name(
                table_prefix + table_template,
                {"experience": experience, "step": step, "aggregation": aggregation},
            )

            print(f"  [to_postgres] Table: {table_name} ({len(days)} days)")

            for day in sorted(days):
                day_files = days[day]
                df = _pivot_wide(day_files, sources)
                if df.empty:
                    continue

                _ensure_table(conn, table_name, df)
                added = _ensure_columns(conn, table_name, df)
                if added:
                    print(f"  [to_postgres]   added columns: {added}")

                ts_min = df["ts"].min()
                ts_max = df["ts"].max()

                with conn.cursor() as cur:
                    cur.execute(
                        f'DELETE FROM "{table_name}" WHERE ts >= %s AND ts <= %s',
                        (ts_min, ts_max),
                    )
                conn.commit()

                rows = _copy_to_postgres(conn, table_name, df)
                total_rows += rows
                total_days += 1

        print(
            f"  [to_postgres] Done: {total_rows} rows inserted across {total_days} day(s)"
        )

    finally:
        conn.close()
