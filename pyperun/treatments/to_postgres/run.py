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

# pandas dtype string → PostgreSQL type
_PG_TYPE_MAP = {
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
    "Int64": "BIGINT",
    "Float64": "DOUBLE PRECISION",
    "float64": "DOUBLE PRECISION",
    "int64": "BIGINT",
}


def _pg_type(dtype) -> str:
    """Map a pandas dtype to a PostgreSQL column type."""
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
    """Replace non-alphanumeric chars with underscores."""
    return re.sub(r"[^a-zA-Z0-9]", "_", name)


def _render_table_name(template: str, parts: dict) -> str:
    """Render the table name from template and parts, sanitize, uppercase."""
    # Remove placeholders for missing keys (e.g. {aggregation} when None)
    rendered = template
    for key, val in parts.items():
        placeholder = "{" + key + "}"
        if val is None:
            # Remove placeholder and surrounding underscores
            rendered = rendered.replace("_" + placeholder, "")
            rendered = rendered.replace(placeholder + "_", "")
            rendered = rendered.replace(placeholder, "")
        else:
            rendered = rendered.replace(placeholder, val)
    return _sanitize(rendered).upper()


def _resolve_allowed_columns(source: dict) -> list[str] | None:
    """Resolve allowed columns from a source spec.

    Priority:
    1. If 'columns' is present, return it directly (manual mode).
    2. If any of 'sensors', 'transforms', 'metrics' are present, build
       allowed patterns from their cartesian product.
    3. Otherwise return None (no filter, all columns pass).
    """
    if "columns" in source:
        return source["columns"]
    sensors = source.get("sensors")
    transforms = source.get("transforms")
    metrics = source.get("metrics")
    if not any([sensors, transforms, metrics]):
        return None
    # Build cartesian product patterns: {sensor}__{transform}__{metric}
    # Each missing axis becomes a wildcard (None)
    parts_lists = [sensors or [None], transforms or [None], metrics or [None]]
    allowed = set()
    for combo in product(*parts_lists):
        allowed.add(tuple(combo))
    return allowed  # set of tuples, handled specially in _matches_structured_filter


def _matches_structured_filter(col: str, allowed) -> bool:
    """Check if a column name matches the structured filter.

    Column format: {sensor}__{transform}__{metric}
    allowed is a set of (sensor|None, transform|None, metric|None) tuples.
    None in a position means 'match anything'. Strings support fnmatch wildcards (e.g. 'm*').
    """
    parts = col.split("__")
    if len(parts) != 3:
        return False
    sensor, transform, metric = parts
    for pattern in allowed:
        if ((pattern[0] is None or fnmatch.fnmatch(sensor, pattern[0]))
                and (pattern[1] is None or fnmatch.fnmatch(transform, pattern[1]))
                and (pattern[2] is None or fnmatch.fnmatch(metric, pattern[2]))):
            return True
    return False


def _find_source(sources: list[dict], domain: str, device_id: str) -> dict | None:
    """Find the first source entry matching domain and device_id.

    Supports multiple entries with the same domain (for per-device column filters).
    A source with no 'devices' list matches any device.
    """
    for s in sources:
        if s["domain"] != domain:
            continue
        devices = s.get("devices")
        if devices and device_id not in devices:
            continue
        return s
    return None


def _pivot_wide(
    files: list[Path], sources: list[dict]
) -> pd.DataFrame:
    """Read parquet files, prefix columns with device_id, merge on ts.

    Args:
        files: list of parquet file paths (all same experience/step/aggregation/day)
        sources: list of source specs. Multiple entries with the same domain are
            allowed to apply different filters per device.

    Returns:
        A single wide DataFrame with ts + prefixed columns from all devices/domains.
    """
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

        # Filter columns
        if allowed is not None:
            if isinstance(allowed, list):
                # Explicit column list
                data_cols = [c for c in data_cols if c in allowed]
            else:
                # Structured filter (set of tuples)
                data_cols = [c for c in data_cols if _matches_structured_filter(c, allowed)]

        if not data_cols:
            continue

        # Prefix columns with sanitized device_id
        device_prefix = _sanitize(parts.device_id)
        rename_map = {c: f"{device_prefix}__{c}" for c in data_cols}
        df = df[["ts"] + data_cols].rename(columns=rename_map)
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    # Merge all frames on ts (outer join)
    result = frames[0]
    for other in frames[1:]:
        result = result.merge(other, on="ts", how="outer")

    result = result.sort_values("ts").reset_index(drop=True)
    return result


def _ensure_table(conn, table_name: str, df: pd.DataFrame) -> bool:
    """CREATE TABLE IF NOT EXISTS. Returns True if table was created."""
    cols = []
    for col in df.columns:
        if col == "ts":
            cols.append("ts TIMESTAMPTZ PRIMARY KEY")
        else:
            cols.append(f"{col} {_pg_type(df[col].dtype)}")

    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)})'
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    # Check if table was just created (approximation: return True always)
    return True


def _ensure_columns(conn, table_name: str, df: pd.DataFrame) -> list[str]:
    """Add missing columns to the table. Returns list of added column names."""
    with conn.cursor() as cur:
        # Quoted identifiers preserve case, so match exact table_name
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,),
        )
        existing = {row[0] for row in cur.fetchall()}

    added = []
    for col in df.columns:
        if col not in existing:
            pg_type = _pg_type(df[col].dtype)
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {pg_type}'
                )
            added.append(col)

    if added:
        conn.commit()
    return added


def _copy_to_postgres(conn, table_name: str, df: pd.DataFrame) -> int:
    """Bulk insert via COPY FROM stdin (CSV). Returns number of rows copied."""
    if df.empty:
        return 0

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    columns = ", ".join(f'"{c}"' for c in df.columns)
    sql = f"""COPY "{table_name}" ({columns}) FROM STDIN WITH (FORMAT csv, NULL '')"""

    with conn.cursor() as cur:
        cur.copy_expert(sql, buf)
    conn.commit()
    return len(df)


def _upsert_from_staging(conn, table_name: str, df: pd.DataFrame) -> int:
    """UPSERT df into table via a temporary staging table.

    - Rows whose ts does not exist in the table: INSERT.
    - Rows whose ts already exists: fill NULL columns using COALESCE
      (existing non-NULL values are preserved, NULLs are filled from df).

    Returns total rows affected (inserted + updated).
    """
    if df.empty:
        return 0

    staging = "_pyperun_stage"
    data_cols = [c for c in df.columns if c != "ts"]
    col_list = ", ".join(f'"{c}"' for c in df.columns)

    # Temp staging table (dropped at end of transaction)
    col_defs = ["ts TIMESTAMPTZ"] + [
        f'"{c}" {_pg_type(df[c].dtype)}' for c in data_cols
    ]
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
        cur.execute(f'CREATE TEMP TABLE "{staging}" ({", ".join(col_defs)})')

    # Bulk copy new data to staging
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY "{staging}" ({col_list}) FROM STDIN WITH (FORMAT csv, NULL \'\')',
            buf,
        )

    # UPSERT: INSERT new ts rows; on conflict fill NULL columns only (COALESCE)
    if data_cols:
        set_clause = ", ".join(
            f'"{c}" = COALESCE("{table_name}"."{c}", EXCLUDED."{c}")'
            for c in data_cols
        )
        conflict_action = f"DO UPDATE SET {set_clause}"
    else:
        conflict_action = "DO NOTHING"

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO "{table_name}" ({col_list})
            SELECT {col_list} FROM "{staging}"
            ON CONFLICT (ts) {conflict_action}
        """)
        affected = cur.rowcount

    conn.commit()

    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
    conn.commit()

    return affected


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    sources = params["sources"]
    mode = params["mode"]
    table_prefix = params.get("table_prefix", "")
    table_template = params["table_template"]
    aggregations = params.get("aggregations", [])

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [to_postgres] Found {len(parquet_files)} parquet files")

    # Group files by (experience, step, aggregation, day)
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

    stats = {"tables": 0, "columns_added": 0, "rows_upserted": 0}
    truncated_tables = set()

    try:
        for (experience, step, aggregation), days in sorted(groups.items()):
            # Filter by aggregation window if specified
            if aggregations and aggregation not in aggregations:
                continue
            table_name = _render_table_name(
                table_prefix + table_template,
                {"experience": experience, "step": step, "aggregation": aggregation},
            )
            print(f"  [to_postgres] Table: {table_name} ({len(days)} days)")

            # Mode replace: truncate once per table (keeps schema)
            # Mode reset: drop once per table (recreates schema from scratch)
            if mode in ("replace", "reset") and table_name not in truncated_tables:
                try:
                    with conn.cursor() as cur:
                        if mode == "reset":
                            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                        else:
                            cur.execute(f'TRUNCATE TABLE "{table_name}"')
                    conn.commit()
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                truncated_tables.add(table_name)

            for day in sorted(days.keys()):
                day_files = days[day]
                df = _pivot_wide(day_files, sources)
                if df.empty:
                    continue

                # Ensure table and columns exist
                _ensure_table(conn, table_name, df)
                added = _ensure_columns(conn, table_name, df)
                stats["columns_added"] += len(added)
                stats["tables"] += 1

                if mode == "append":
                    # UPSERT: INSERT new timestamps, fill NULL columns for existing ones
                    rows = _upsert_from_staging(conn, table_name, df)
                else:
                    # replace/reset: table already truncated/dropped, plain bulk insert
                    rows = _copy_to_postgres(conn, table_name, df)
                stats["rows_upserted"] += rows

        print(
            f"  [to_postgres] Done: {stats['rows_upserted']} rows upserted, "
            f"{stats['columns_added']} columns added"
        )
    finally:
        conn.close()
