from __future__ import annotations

import io
import re
from collections import defaultdict
from itertools import product
from pathlib import Path

import pandas as pd
import psycopg2

from pymyx.core.filename import list_parquet_files, parse_parquet_path

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
    None in a position means 'match anything'.
    """
    parts = col.split("__")
    if len(parts) != 3:
        return False
    sensor, transform, metric = parts
    for pattern in allowed:
        if ((pattern[0] is None or pattern[0] == sensor)
                and (pattern[1] is None or pattern[1] == transform)
                and (pattern[2] is None or pattern[2] == metric)):
            return True
    return False


def _pivot_wide(
    files: list[Path], sources: list[dict]
) -> pd.DataFrame:
    """Read parquet files, prefix columns with device_id, merge on ts.

    Args:
        files: list of parquet file paths (all same experience/step/aggregation/day)
        sources: list of source specs, e.g. [{"domain": "bio_signal"}, {"domain": "environment", "columns": ["outdoor_temp_mean"]}]

    Returns:
        A single wide DataFrame with ts + prefixed columns from all devices/domains.
    """
    source_map = {s["domain"]: s for s in sources}
    frames = []

    for f in files:
        parts = parse_parquet_path(f)

        # Filter by domain
        if parts.domain not in source_map:
            continue

        source = source_map[parts.domain]

        # Filter by device
        devices = source.get("devices")
        if devices and parts.device_id not in devices:
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


def _get_max_ts(conn, table_name: str):
    """Get MAX(ts) from the table, or None if empty/nonexistent."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX(ts) FROM "{table_name}"')
            row = cur.fetchone()
            return row[0] if row else None
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return None


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

    stats = {"tables": 0, "columns_added": 0, "rows_inserted": 0}
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

            # Mode replace: truncate once per table
            if mode == "replace" and table_name not in truncated_tables:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f'TRUNCATE TABLE "{table_name}"')
                    conn.commit()
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()
                truncated_tables.add(table_name)

            # Mode append: get max ts for filtering
            max_ts = None
            if mode == "append":
                max_ts = _get_max_ts(conn, table_name)

            for day in sorted(days.keys()):
                day_files = days[day]
                df = _pivot_wide(day_files, sources)
                if df.empty:
                    continue

                # Ensure table and columns exist
                _ensure_table(conn, table_name, df)
                added = _ensure_columns(conn, table_name, df)
                stats["columns_added"] += len(added)
                stats["tables"] += 1  # counted per ensure_table call

                # Mode append: filter out already-inserted rows
                if mode == "append" and max_ts is not None:
                    df = df[df["ts"] > max_ts]
                    if df.empty:
                        continue

                rows = _copy_to_postgres(conn, table_name, df)
                stats["rows_inserted"] += rows

        print(
            f"  [to_postgres] Done: {stats['rows_inserted']} rows inserted, "
            f"{stats['columns_added']} columns added"
        )
    finally:
        conn.close()
