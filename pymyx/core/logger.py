from __future__ import annotations

import time
from pathlib import Path

import jsonlines


LOG_PATH = Path("pymyx.log")


def log_event(
    treatment: str,
    status: str,
    input_dir: str,
    output_dir: str,
    duration_ms: float | None = None,
    error: str | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
) -> None:
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "treatment": treatment,
        "status": status,
        "input_dir": input_dir,
        "output_dir": output_dir,
    }
    if duration_ms is not None:
        entry["duration_ms"] = round(duration_ms, 1)
    if error is not None:
        entry["error"] = error
    if time_from is not None:
        entry["time_from"] = time_from
    if time_to is not None:
        entry["time_to"] = time_to
    with jsonlines.open(LOG_PATH, mode="a") as writer:
        writer.write(entry)
