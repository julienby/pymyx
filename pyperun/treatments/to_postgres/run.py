from __future__ import annotations

import fnmatch
import io
import json
import re
from collections import defaultdict
from datetime import date, timedelta
from itertools import product
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow.parquet as pq

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


def _resolve_allowed_columns(source: dict) -> list[str] | None:
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
        if ((pattern[0] is None or fnmatch.fnmatch(sensor, pattern[0]))
                and (pattern[1] is None or fnmatch.fnmatch(transform, pattern[1]))
                and (pattern[2] is None or fnmatch.fnmatch(metric, pattern[2]))):
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
    """Bulk insert via COPY FROM stdin. Returns number of rows inserted."""
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


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------

def _load_stats(path: Path) -> dict:
    """Load stats.json from output dir. Returns empty dict if not found."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _save_stats(path: Path, stats: dict) -> None:
    with open(path, "w") as f:
        json.dump(stats, f, indent=2, sort_keys=True)


def _count_table(conn, table_name: str) -> int:
    """Return COUNT(*) for table, or 0 if table does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            return cur.fetchone()[0]
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return 0


def _count_by_day(conn, table_name: str) -> dict[str, int]:
    """Return {day_str: row_count} for every day present in the table."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT (ts AT TIME ZONE 'UTC')::date::text, COUNT(*)
                FROM "{table_name}"
                GROUP BY 1
            """)
            return {row[0]: row[1] for row in cur.fetchall()}
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return {}


def _delete_day(conn, table_name: str, day: str) -> None:
    """Delete all rows for a given UTC day from the table."""
    d0 = date.fromisoformat(day)
    d1 = d0 + timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{table_name}" WHERE ts >= %s AND ts < %s',
            (d0.isoformat(), d1.isoformat()),
        )
    conn.commit()


def _sources_changed(day_files: list[Path], day_stats: dict) -> bool:
    """Return True if any source parquet changed, appeared, or disappeared.

    Comparison uses row count from parquet footer metadata (fast, no data read).
    day_stats keys: device_id → row_count, plus "db_rows" (ignored here).
    """
    current = {
        parse_parquet_path(f).device_id: pq.read_metadata(f).num_rows
        for f in day_files
    }
    # New or changed file
    for device_id, n_rows in current.items():
        if day_stats.get(device_id) != n_rows:
            return True
    # Disappeared file
    for key in day_stats:
        if key != "db_rows" and key not in current:
            return True
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    sources = params["sources"]
    mode = params["mode"]
    table_prefix = params.get("table_prefix", "")
    table_template = params["table_template"]
    aggregations = params.get("aggregations", [])

    stats_path = out_path / "stats.json"
    stats = _load_stats(stats_path)

    parquet_files = list_parquet_files(in_path)
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    print(f"  [to_postgres] Found {len(parquet_files)} parquet files")

    # Group files by (experience, step, aggregation) → day → [files]
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
    truncated_tables: set[str] = set()

    try:
        for (experience, step, aggregation), days in sorted(groups.items()):
            if aggregations and aggregation not in aggregations:
                continue

            table_name = _render_table_name(
                table_prefix + table_template,
                {"experience": experience, "step": step, "aggregation": aggregation},
            )
            table_stats: dict[str, dict] = stats.get(table_name, {})

            print(f"  [to_postgres] Table: {table_name} ({len(days)} days)")

            # --- replace / reset: truncate once, replay all days, clear stats ---
            if mode in ("replace", "reset"):
                if table_name not in truncated_tables:
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
                table_stats = {}
                days_to_replay = set(days.keys())

            # --- append: Phase 1 (global sanity) + Phase 2 (per-file check) ---
            else:
                days_to_replay: set[str] = set()

                # Phase 1 — one COUNT(*) to detect external DB corruption
                expected_total = sum(v.get("db_rows", 0) for v in table_stats.values())
                db_total = _count_table(conn, table_name)

                if db_total != expected_total:
                    print(
                        f"  [to_postgres]   DB count mismatch "
                        f"({db_total} in DB vs {expected_total} expected) — checking by day"
                    )
                    db_per_day = _count_by_day(conn, table_name)
                    for day in days:
                        expected_rows = table_stats.get(day, {}).get("db_rows", 0)
                        if db_per_day.get(day, 0) != expected_rows:
                            days_to_replay.add(day)

                # Phase 2 — parquet footer read per file (no data read, ~0.1 ms/file)
                for day, day_files in days.items():
                    if day not in days_to_replay:
                        day_stats = table_stats.get(day, {})
                        if _sources_changed(day_files, day_stats):
                            days_to_replay.add(day)

                if not days_to_replay:
                    print(f"  [to_postgres]   all days up-to-date, skipping")
                    continue

                print(f"  [to_postgres]   {len(days_to_replay)} day(s) to replay")

            # --- Phase 3: replay ---
            for day in sorted(days_to_replay):
                day_files = days.get(day, [])
                df = _pivot_wide(day_files, sources)
                if df.empty:
                    continue

                _ensure_table(conn, table_name, df)
                added = _ensure_columns(conn, table_name, df)
                if added:
                    print(f"  [to_postgres]   added columns: {added}")

                if mode == "append":
                    _delete_day(conn, table_name, day)

                rows = _copy_to_postgres(conn, table_name, df)
                total_rows += rows
                total_days += 1

                # Update stats for this day
                day_entry: dict = {"db_rows": rows}
                for f in day_files:
                    device_id = parse_parquet_path(f).device_id
                    day_entry[device_id] = pq.read_metadata(f).num_rows
                table_stats[day] = day_entry

            stats[table_name] = table_stats

        _save_stats(stats_path, stats)
        print(
            f"  [to_postgres] Done: {total_rows} rows inserted across "
            f"{total_days} day(s)"
        )

    finally:
        conn.close()
