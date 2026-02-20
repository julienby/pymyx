import argparse
import json
import sys
from pathlib import Path

from pymyx.core.pipeline import DATASETS_PREFIX, resolve_paths
from pymyx.core.runner import run_treatment
from pymyx.core.timefilter import parse_iso_utc, resolve_last_range


FLOWS_ROOT = Path(__file__).resolve().parent.parent.parent / "flows"

# Params that control execution context, not passed as treatment params
_META_PARAMS = {"from", "to"}


def _filter_steps(steps, from_step=None, to_step=None, step=None):
    """Filter steps by --from-step, --to-step, or --step.

    Returns the filtered list of steps.
    """
    names = [s["treatment"] for s in steps]

    if step:
        if step not in names:
            raise ValueError(f"Step '{step}' not found in flow. Available: {names}")
        return [s for s in steps if s["treatment"] == step]

    start = 0
    end = len(steps)

    if from_step:
        if from_step not in names:
            raise ValueError(f"Step '{from_step}' not found in flow. Available: {names}")
        start = names.index(from_step)

    if to_step:
        if to_step not in names:
            raise ValueError(f"Step '{to_step}' not found in flow. Available: {names}")
        end = names.index(to_step) + 1

    return steps[start:end]


def _resolve_path(dataset: str, path: str) -> str:
    """Prefix a relative path with datasets/<dataset>/. Absolute paths are unchanged."""
    if Path(path).is_absolute():
        return path
    return f"{DATASETS_PREFIX}/{dataset}/{path}"


def run_flow(
    name: str,
    time_from=None,
    time_to=None,
    output_mode: str = "append",
    last: bool = False,
    from_step: str | None = None,
    to_step: str | None = None,
    step: str | None = None,
) -> None:
    flow_path = FLOWS_ROOT / f"{name}.json"
    if not flow_path.exists():
        raise FileNotFoundError(f"Flow not found: {flow_path}")

    with open(flow_path) as f:
        flow = json.load(f)

    dataset = flow.get("dataset")

    # Flow-level params: inherited by all steps (hierarchy: flow > treatment defaults)
    flow_params = dict(flow.get("params", {}))
    # Backward compat: from/to at top level
    for key in ("from", "to"):
        if key in flow and key not in flow_params:
            flow_params[key] = flow[key]

    # Extract time range from flow params (CLI overrides)
    if time_from is None and "from" in flow_params:
        time_from = parse_iso_utc(flow_params["from"])
    if time_to is None and "to" in flow_params:
        time_to = parse_iso_utc(flow_params["to"])

    # Inherited treatment params = flow params minus meta-params (from/to)
    inherited = {k: v for k, v in flow_params.items() if k not in _META_PARAMS}

    steps = flow.get("steps", [])
    if not steps:
        raise ValueError(f"Flow '{name}' has no steps")

    # Resolve paths and merge params per step
    for s in steps:
        # Merge: inherited flow params + step params (step overrides flow)
        s["params"] = {**inherited, **s.get("params", {})}

        if dataset:
            # Resolve relative paths to datasets/<dataset>/<path>
            if "input" in s:
                s["input"] = _resolve_path(dataset, s["input"])
            if "output" in s:
                s["output"] = _resolve_path(dataset, s["output"])
            # Fallback to registry if paths not declared in step
            if "input" not in s or "output" not in s:
                inp, out = resolve_paths(dataset, s["treatment"])
                s.setdefault("input", inp)
                s.setdefault("output", out)

    # Filter steps
    steps = _filter_steps(steps, from_step=from_step, to_step=to_step, step=step)

    # full-replace: wipe all output directories before running
    if output_mode == "full-replace":
        for s in steps:
            if "output" not in s:
                continue
            out = Path(s["output"])
            if out.exists():
                count = sum(1 for f in out.rglob("*") if f.is_file())
                if count:
                    for f in out.rglob("*"):
                        if f.is_file():
                            f.unlink()
                    print(f"[flow] Cleared {s['output']} ({count} files)")
        output_mode = "replace"

    # Resolve --last from the first step (parse input/output)
    if last:
        first = steps[0]
        try:
            time_from, time_to = resolve_last_range(
                Path(first["input"]), Path(first["output"])
            )
        except ValueError as exc:
            print(f"[flow] {exc}")
            return
        if time_from is not None:
            print(f"[flow] --last resolved to {time_from.isoformat()} .. {time_to.isoformat()}")

    print(f"[flow] Starting '{name}' ({len(steps)} steps)")
    for i, s in enumerate(steps, 1):
        treatment = s["treatment"]
        input_dir = s["input"]
        output_dir = s.get("output", "")
        params = s.get("params", {})

        print(f"[flow] Step {i}/{len(steps)}: {treatment}")
        try:
            run_treatment(treatment, input_dir, output_dir, params,
                          time_from=time_from, time_to=time_to,
                          output_mode=output_mode)
        except Exception as exc:
            print(f"[flow] FAILED at step {i} ({treatment}): {exc}", file=sys.stderr)
            raise SystemExit(1)

    print(f"[flow] Completed '{name}' successfully")


def main():
    parser = argparse.ArgumentParser(description="Run a PyMyx flow")
    parser.add_argument("--flow", required=True, help="Flow name")
    parser.add_argument("--from", dest="time_from", default=None,
                        help="Start of time window (ISO 8601)")
    parser.add_argument("--to", dest="time_to", default=None,
                        help="End of time window (ISO 8601)")
    parser.add_argument("--output-mode", default="append", choices=["append", "replace", "full-replace"],
                        help="Output mode: replace or append (default: append)")
    parser.add_argument("--last", action="store_true",
                        help="Incremental: process only the delta since last output")
    parser.add_argument("--from-step", default=None,
                        help="Start from this treatment step (inclusive)")
    parser.add_argument("--to-step", default=None,
                        help="Stop at this treatment step (inclusive)")
    parser.add_argument("--step", default=None,
                        help="Run a single step from the flow")
    args = parser.parse_args()

    # Validate mutual exclusion
    if args.last and (args.time_from or args.time_to):
        parser.error("--last is mutually exclusive with --from/--to")
    if args.step and (args.from_step or args.to_step):
        parser.error("--step is mutually exclusive with --from-step/--to-step")

    time_from = parse_iso_utc(args.time_from) if args.time_from else None
    time_to = parse_iso_utc(args.time_to) if args.time_to else None

    # Validate from < to when both provided
    if time_from and time_to and time_from > time_to:
        parser.error("--from must be before --to")

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last,
             from_step=args.from_step, to_step=args.to_step, step=args.step)


if __name__ == "__main__":
    main()
