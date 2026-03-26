import argparse
import json
import sys
from pathlib import Path

from pyperun.core.pipeline import DATASETS_PREFIX, resolve_paths
from pyperun.core.runner import run_treatment
from pyperun.core.timefilter import parse_iso_utc, resolve_last_range


_BUILTIN_FLOWS_ROOT = Path(__file__).resolve().parent.parent.parent / "flows"

# Params that control execution context, not passed as treatment params
_META_PARAMS = {"from", "to"}


def _print_dry_run(name: str, steps: list, time_from, time_to, last: bool) -> None:
    import json
    from pyperun.core.runner import resolve_treatment_dir
    from pyperun.core.validator import load_treatment, merge_params

    print(f"\n\033[1m[dry-run] Flow: {name} ({len(steps)} steps)\033[0m")
    if last:
        print(f"  Global filter: --last (resolved at runtime)")
    elif time_from or time_to:
        tf = time_from.isoformat() if time_from else "..."
        tt = time_to.isoformat() if time_to else "..."
        print(f"  Global filter: {tf} → {tt}")
    print()

    for i, s in enumerate(steps, 1):
        treatment = s["treatment"]
        step_id = _step_id(s)
        label = treatment if step_id == treatment else f"{treatment} [{step_id}]"
        input_dir = s.get("input", "")
        output_dir = s.get("output", "")
        step_time_from = s.get("_time_from", time_from)
        step_time_to = s.get("_time_to", time_to)
        params = s.get("params", {})

        # Resolve full params including treatment defaults
        try:
            t_dir = resolve_treatment_dir(treatment)
            schema = load_treatment(t_dir)
            full_params = merge_params(schema, {k: v for k, v in params.items() if not k.startswith("__")})
        except Exception:
            full_params = params

        print(f"  \033[1mStep {i}/{len(steps)}: {label}\033[0m")
        print(f"    Input:   {input_dir}")
        print(f"    Output:  {output_dir}")
        if step_time_from or step_time_to:
            tf = step_time_from.isoformat() if step_time_from else "..."
            tt = step_time_to.isoformat() if step_time_to else "..."
            marker = "  \033[33m← funnel\033[0m" if s.get("_time_from") or s.get("_time_to") else ""
            print(f"    Filter:  {tf} → {tt}{marker}")
        else:
            print(f"    Filter:  all dates")
        if full_params:
            params_str = json.dumps(full_params, ensure_ascii=False)
            # Truncate very long params for readability
            if len(params_str) > 120:
                params_str = params_str[:117] + "..."
            print(f"    Params:  {params_str}")
        print()


def get_flows_root() -> Path:
    """Return the flows directory: local ./flows/ takes priority over built-ins."""
    local = Path.cwd() / "flows"
    return local if local.is_dir() else _BUILTIN_FLOWS_ROOT


def _find_flow(name: str) -> Path:
    """Find a flow file: local ./flows/<name>.json takes priority over built-ins."""
    local = Path.cwd() / "flows" / f"{name}.json"
    if local.exists():
        return local
    builtin = _BUILTIN_FLOWS_ROOT / f"{name}.json"
    if builtin.exists():
        return builtin
    raise FileNotFoundError(
        f"Flow '{name}' not found in {Path.cwd() / 'flows'} nor {_BUILTIN_FLOWS_ROOT}"
    )


# Keep FLOWS_ROOT as an alias for backward compat (cmd_list, cmd_status import it)
FLOWS_ROOT = _BUILTIN_FLOWS_ROOT


def _step_id(s):
    """Return the identifier of a step: its 'name' if set, otherwise its 'treatment'."""
    return s.get("name") or s["treatment"]


def _filter_steps(steps, from_step=None, to_step=None, step=None):
    """Filter steps by --from-step, --to-step, or --step.

    Steps can be identified by their 'name' field (if set) or by 'treatment'.
    Returns the filtered list of steps.
    """
    ids = [_step_id(s) for s in steps]

    if step:
        if step not in ids:
            raise ValueError(f"Step '{step}' not found in flow. Available: {ids}")
        return [s for s in steps if _step_id(s) == step]

    start = 0
    end = len(steps)

    if from_step:
        if from_step not in ids:
            raise ValueError(f"Step '{from_step}' not found in flow. Available: {ids}")
        start = ids.index(from_step)

    if to_step:
        if to_step not in ids:
            raise ValueError(f"Step '{to_step}' not found in flow. Available: {ids}")
        end = ids.index(to_step) + 1

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
    dry_run: bool = False,
) -> None:
    flow_path = _find_flow(name)

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
        step_raw = s.get("params", {})

        # Extract step-level from/to for per-step file filtering (funnel)
        if step_raw.get("from"):
            s["_time_from"] = parse_iso_utc(step_raw["from"])
        if step_raw.get("to"):
            s["_time_to"] = parse_iso_utc(step_raw["to"])

        # Merge: inherited flow params + step params
        # Strip meta-params (from/to) from flow-level inherited params,
        # but keep step-level from/to so treatments can use them for row filtering
        merged = {**inherited, **step_raw}
        flow_meta = _META_PARAMS - set(step_raw.keys())
        s["params"] = {k: v for k, v in merged.items() if k not in flow_meta}

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

    if dry_run:
        _print_dry_run(name, steps, time_from, time_to, last)
        return

    print(f"[flow] Starting '{name}' ({len(steps)} steps)")
    for i, s in enumerate(steps, 1):
        treatment = s["treatment"]
        input_dir = s["input"]
        output_dir = s.get("output", "")
        params = s.get("params", {})

        # Per-step time range: step-level from/to overrides flow-level (funnel)
        step_time_from = s.get("_time_from", time_from)
        step_time_to = s.get("_time_to", time_to)

        print(f"[flow] Step {i}/{len(steps)}: {treatment}")
        try:
            run_treatment(treatment, input_dir, output_dir, params,
                          time_from=step_time_from, time_to=step_time_to,
                          output_mode=output_mode, flow=name)
        except Exception as exc:
            print(f"[flow] FAILED at step {i} ({treatment}): {exc}", file=sys.stderr)
            raise SystemExit(1)

    print(f"[flow] Completed '{name}' successfully")


def main():
    parser = argparse.ArgumentParser(description="Run a Pyperun flow")
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
