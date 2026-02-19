from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from pymyx.core.logger import log_event
from pymyx.core.timefilter import (
    extract_date_from_filename,
    filter_files_by_date_range,
    parse_iso_utc,
    resolve_last_range,
)
from pymyx.core.validator import load_treatment, merge_params, validate_input_dir


TREATMENTS_ROOT = Path(__file__).resolve().parent.parent / "treatments"
def resolve_treatment_dir(name: str) -> Path:
    """Return treatment directory: local ./treatments/<name> takes priority over built-ins."""
    local = Path.cwd() / "treatments" / name
    if local.is_dir():
        return local
    builtin = TREATMENTS_ROOT / name
    if builtin.is_dir():
        return builtin
    raise FileNotFoundError(
        f"Treatment '{name}' not found in {Path.cwd() / 'treatments'} nor {TREATMENTS_ROOT}"
    )


def _cleanup_tmpdir(tmpdir: Path) -> None:
    """Remove a temporary directory with possible subdirectories (symlinks only)."""
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


def load_run_module(treatment_dir: Path):
    run_path = treatment_dir / "run.py"
    if not run_path.exists():
        raise FileNotFoundError(f"run.py not found in {treatment_dir}")
    spec = importlib.util.spec_from_file_location(f"treatment_{treatment_dir.name}.run", run_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if not hasattr(mod, "run"):
        raise AttributeError(f"run.py in {treatment_dir} has no 'run' function")
    return mod


def _scoped_delete(output_dir: Path, time_from, time_to):
    """Delete output files whose date falls within the time range.

    Handles both flat and domain=*/ subdirectory layouts.
    """
    date_from = time_from.date() if time_from else None
    date_to = time_to.date() if time_to else None
    for f in output_dir.rglob("*"):
        if not f.is_file():
            continue
        d = extract_date_from_filename(f.name)
        if d is None:
            continue
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        f.unlink()


def _build_filtered_input(input_dir: Path, time_from, time_to) -> Path:
    """Create a tmpdir with symlinks to input files matching the date range.

    Preserves domain=*/ subdirectory structure for parquet files.
    """
    all_files = sorted(
        f for f in input_dir.rglob("*")
        if f.is_file() and f.suffix in (".csv", ".parquet")
    )
    selected = filter_files_by_date_range(all_files, time_from, time_to)
    tmpdir = Path(tempfile.mkdtemp(prefix="pymyx_filter_"))
    for f in selected:
        rel = f.relative_to(input_dir)
        target = tmpdir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(f.resolve(), target)
    return tmpdir


def run_treatment(
    name: str,
    input_dir: str,
    output_dir: str,
    params: dict | None = None,
    time_from=None,
    time_to=None,
    output_mode: str = "append",
) -> None:
    treatment_dir = resolve_treatment_dir(name)
    schema = load_treatment(treatment_dir)

    # Inject __time_range for intra-day trimming (parse only)
    effective_params = dict(params or {})
    if time_from or time_to:
        effective_params["__time_range"] = {
            "from": time_from.isoformat() if time_from else None,
            "to": time_to.isoformat() if time_to else None,
        }

    merged = merge_params(schema, effective_params)
    validate_input_dir(input_dir)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Output-mode replace
    if output_mode == "replace":
        if time_from or time_to:
            _scoped_delete(out, time_from, time_to)
        else:
            for f in out.rglob("*"):
                if f.is_file():
                    f.unlink()

    # File filtering via symlinks
    filtered_dir = None
    effective_input = input_dir
    if time_from or time_to:
        in_path = Path(input_dir)
        filtered_dir = _build_filtered_input(in_path, time_from, time_to)
        if not any(filtered_dir.rglob("*")):
            log_event(name, "skip", input_dir, output_dir,
                      time_from=time_from.isoformat() if time_from else None,
                      time_to=time_to.isoformat() if time_to else None)
            print(f"  [{name}] No files in time range, skipping")
            _cleanup_tmpdir(filtered_dir)
            return
        effective_input = str(filtered_dir)

    mod = load_run_module(treatment_dir)

    log_kwargs = {}
    if time_from:
        log_kwargs["time_from"] = time_from.isoformat()
    if time_to:
        log_kwargs["time_to"] = time_to.isoformat()

    log_event(name, "start", input_dir, output_dir, **log_kwargs)
    t0 = time.perf_counter()
    try:
        mod.run(effective_input, output_dir, merged)
        duration_ms = (time.perf_counter() - t0) * 1000
        log_event(name, "success", input_dir, output_dir, duration_ms=duration_ms, **log_kwargs)
    except Exception as exc:
        duration_ms = (time.perf_counter() - t0) * 1000
        log_event(name, "error", input_dir, output_dir, duration_ms=duration_ms, error=str(exc), **log_kwargs)
        raise
    finally:
        if filtered_dir is not None:
            _cleanup_tmpdir(filtered_dir)


def main():
    parser = argparse.ArgumentParser(description="Run a PyMyx treatment")
    parser.add_argument("--treatment", required=True, help="Treatment name")
    parser.add_argument("--input", required=True, help="Input directory")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--params", default="{}", help="JSON params string")
    parser.add_argument("--from", dest="time_from", default=None,
                        help="Start of time window (ISO 8601)")
    parser.add_argument("--to", dest="time_to", default=None,
                        help="End of time window (ISO 8601)")
    parser.add_argument("--output-mode", default="append", choices=["replace", "append"],
                        help="Output mode: replace or append (default: append)")
    parser.add_argument("--last", action="store_true",
                        help="Incremental: process only the delta since last output")
    args = parser.parse_args()

    # Validate mutual exclusion
    if args.last and (args.time_from or args.time_to):
        parser.error("--last is mutually exclusive with --from/--to")

    params = json.loads(args.params)

    time_from = parse_iso_utc(args.time_from) if args.time_from else None
    time_to = parse_iso_utc(args.time_to) if args.time_to else None

    # Validate from < to when both provided
    if time_from and time_to and time_from > time_to:
        parser.error("--from must be before --to")

    # Resolve --last
    if args.last:
        try:
            time_from, time_to = resolve_last_range(
                Path(args.input), Path(args.output)
            )
        except ValueError as exc:
            print(f"  [{args.treatment}] {exc}")
            sys.exit(0)
        if time_from is not None:
            print(f"  [{args.treatment}] --last resolved to {time_from.isoformat()} .. {time_to.isoformat()}")

    run_treatment(args.treatment, args.input, args.output, params,
                  time_from=time_from, time_to=time_to, output_mode=args.output_mode)


if __name__ == "__main__":
    main()
