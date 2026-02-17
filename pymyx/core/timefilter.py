from __future__ import annotations

import re
from datetime import datetime, date, timezone, timedelta
from pathlib import Path

import pyarrow.parquet as pq


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def parse_iso_utc(s: str) -> datetime:
    """Parse an ISO 8601 string to a timezone-aware UTC datetime."""
    # Python 3.10 doesn't support 'Z' suffix in fromisoformat
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_date_from_filename(name: str) -> date | None:
    """Extract the first YYYY-MM-DD date found in a filename."""
    m = _DATE_RE.search(name)
    if not m:
        return None
    return date.fromisoformat(m.group(1))


def filter_files_by_date_range(
    files: list[Path],
    time_from: datetime | None = None,
    time_to: datetime | None = None,
) -> list[Path]:
    """Keep files whose embedded date falls within [from.date(), to.date()] (inclusive).

    Files without an embedded date are always excluded.
    """
    date_from = time_from.date() if time_from else None
    date_to = time_to.date() if time_to else None

    result = []
    for f in files:
        d = extract_date_from_filename(f.name)
        if d is None:
            continue
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        result.append(f)
    return result


def compute_last_timestamp(directory: Path) -> datetime | None:
    """Return the max timestamp found across all files in *directory*.

    For parquet files: reads the actual timestamp column.
    For other files (CSV, etc.): falls back to the date embedded in the filename.
    Returns None if the directory is empty or contains no timestamps.
    """
    parquets = sorted(directory.rglob("*.parquet"))
    last_ts: datetime | None = None

    for p in parquets:
        table = pq.read_table(p)
        for col_name in table.column_names:
            col = table.column(col_name)
            if hasattr(col.type, "tz") or str(col.type).startswith("timestamp"):
                series = col.to_pandas()
                if series.empty:
                    continue
                ts_max = series.max()
                if last_ts is None or ts_max > last_ts:
                    last_ts = ts_max

    if last_ts is not None:
        return last_ts

    # Fallback: extract date from any file name (CSV, etc.)
    all_files = sorted(directory.rglob("*"))
    last_date: date | None = None
    for f in all_files:
        if not f.is_file():
            continue
        d = extract_date_from_filename(f.name)
        if d is not None and (last_date is None or d > last_date):
            last_date = d

    if last_date is not None:
        return datetime(last_date.year, last_date.month, last_date.day,
                        23, 59, 59, tzinfo=timezone.utc)
    return None


def _last_file_date(directory: Path) -> date | None:
    """Return the max date found in filenames across all files in directory."""
    last: date | None = None
    for f in directory.rglob("*"):
        if not f.is_file():
            continue
        d = extract_date_from_filename(f.name)
        if d is not None and (last is None or d > last):
            last = d
    return last


def resolve_last_range(
    input_dir: Path, output_dir: Path
) -> tuple[datetime | None, datetime | None]:
    """Compute (time_from, time_to) for --last incremental mode.

    - First run (output empty): returns (None, None) meaning process everything.
    - Output already up-to-date: raises ValueError.
    - Otherwise: from = floor(last_output_ts, hour), to = last_input_ts.
      Minimum window is 1 hour.
    """
    last_input = compute_last_timestamp(input_dir)
    if last_input is None:
        return None, None

    last_output = compute_last_timestamp(output_dir)
    if last_output is None:
        # First run — process everything
        return None, None

    # When input has no parquet (CSV only), compare by file dates to avoid
    # false deltas from the 23:59:59 fallback
    has_parquet_input = any(input_dir.rglob("*.parquet"))
    if not has_parquet_input:
        input_date = _last_file_date(input_dir)
        output_date = _last_file_date(output_dir)
        if input_date and output_date and output_date >= input_date:
            raise ValueError("already up-to-date")

    if last_output >= last_input:
        raise ValueError("already up-to-date")

    # Floor to the hour
    time_from = last_output.replace(minute=0, second=0, microsecond=0)
    time_to = last_input

    # Enforce minimum 1h window
    if time_to - time_from < timedelta(hours=1):
        time_from = time_to - timedelta(hours=1)

    # Ensure timezone-aware UTC
    if time_from.tzinfo is None:
        time_from = time_from.replace(tzinfo=timezone.utc)
    if time_to.tzinfo is None:
        time_to = time_to.replace(tzinfo=timezone.utc)

    return time_from, time_to
