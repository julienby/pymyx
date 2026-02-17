import argparse
import json
import sys
from pathlib import Path

from pymyx.core.runner import run_treatment
from pymyx.core.timefilter import parse_iso_utc, resolve_last_range


FLOWS_ROOT = Path(__file__).resolve().parent.parent.parent / "flows"


def run_flow(
    name: str,
    time_from=None,
    time_to=None,
    output_mode: str = "append",
    last: bool = False,
) -> None:
    flow_path = FLOWS_ROOT / f"{name}.json"
    if not flow_path.exists():
        raise FileNotFoundError(f"Flow not found: {flow_path}")

    with open(flow_path) as f:
        flow = json.load(f)

    steps = flow.get("steps", [])
    if not steps:
        raise ValueError(f"Flow '{name}' has no steps")

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
    for i, step in enumerate(steps, 1):
        treatment = step["treatment"]
        input_dir = step["input"]
        output_dir = step["output"]
        params = step.get("params", {})

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
    parser.add_argument("--output-mode", default="append", choices=["replace", "append"],
                        help="Output mode: replace or append (default: append)")
    parser.add_argument("--last", action="store_true",
                        help="Incremental: process only the delta since last output")
    args = parser.parse_args()

    # Validate mutual exclusion
    if args.last and (args.time_from or args.time_to):
        parser.error("--last is mutually exclusive with --from/--to")

    time_from = parse_iso_utc(args.time_from) if args.time_from else None
    time_to = parse_iso_utc(args.time_to) if args.time_to else None

    # Validate from < to when both provided
    if time_from and time_to and time_from > time_to:
        parser.error("--from must be before --to")

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last)


if __name__ == "__main__":
    main()
