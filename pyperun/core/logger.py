from __future__ import annotations

import time
from pathlib import Path

import jsonlines


LOG_PATH = Path("logs/pyperun.log")


_REDACT_KEYS = {"password"}


def new_run_id() -> str:
    """Generate a short unique run identifier (8 hex chars)."""
    import os
    return os.urandom(4).hex()


def log_event(
    treatment: str,
    status: str,
    input_dir: str,
    output_dir: str,
    duration_ms: float | None = None,
    error: str | None = None,
    time_from: str | None = None,
    time_to: str | None = None,
    params: dict | None = None,
    flow: str | None = None,
    run_id: str | None = None,
) -> None:
    entry = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "treatment": treatment,
        "status": status,
        "input_dir": input_dir,
        "output_dir": output_dir,
    }
    if run_id is not None:
        entry["run_id"] = run_id
    if flow is not None:
        entry["flow"] = flow
    if time_from is not None:
        entry["time_from"] = time_from
    if time_to is not None:
        entry["time_to"] = time_to
    if params is not None:
        entry["params"] = {k: ("***" if k in _REDACT_KEYS else v) for k, v in params.items()}
    if duration_ms is not None:
        entry["duration_ms"] = round(duration_ms, 1)
    if error is not None:
        entry["error"] = error
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with jsonlines.open(LOG_PATH, mode="a") as writer:
        writer.write(entry)
