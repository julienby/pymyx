import argparse
import json
import sys

from pymyx.core.timefilter import parse_iso_utc


def _add_common_args(parser):
    """Add time filtering and output-mode args shared by flow and run."""
    parser.add_argument("--from", dest="time_from", default=None,
                        help="Start of time window (ISO 8601)")
    parser.add_argument("--to", dest="time_to", default=None,
                        help="End of time window (ISO 8601)")
    parser.add_argument("--output-mode", default="append", choices=["append", "replace", "full-replace"],
                        help="Output mode: replace or append (default: append)")
    parser.add_argument("--last", action="store_true",
                        help="Incremental: process only the delta since last output")


def _validate_common_args(parser, args):
    """Validate common args and return (time_from, time_to)."""
    if args.last and (args.time_from or args.time_to):
        parser.error("--last is mutually exclusive with --from/--to")

    time_from = parse_iso_utc(args.time_from) if args.time_from else None
    time_to = parse_iso_utc(args.time_to) if args.time_to else None

    if time_from and time_to and time_from > time_to:
        parser.error("--from must be before --to")

    return time_from, time_to


def cmd_flow(args, parser):
    from pymyx.core.flow import run_flow

    if args.step and (args.from_step or args.to_step):
        parser.error("--step is mutually exclusive with --from-step/--to-step")

    time_from, time_to = _validate_common_args(parser, args)

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last,
             from_step=args.from_step, to_step=args.to_step, step=args.step)


def cmd_run(args, parser):
    from pymyx.core.runner import run_treatment

    time_from, time_to = _validate_common_args(parser, args)
    params = json.loads(args.params)

    run_treatment(args.treatment, args.input, args.output, params,
                  time_from=time_from, time_to=time_to, output_mode=args.output_mode)


def cmd_list(args, _parser):
    from pymyx.core.flow import FLOWS_ROOT
    from pymyx.core.runner import TREATMENTS_ROOT

    if args.what == "flows":
        for f in sorted(FLOWS_ROOT.glob("*.json")):
            print(f"  {f.stem}")
    elif args.what == "treatments":
        for d in sorted(TREATMENTS_ROOT.iterdir()):
            if (d / "treatment.json").exists():
                print(f"  {d.name}")
    elif args.what == "steps":
        if not args.flow:
            print("Error: --flow required with 'pymyx list steps'", file=sys.stderr)
            raise SystemExit(1)
        flow_path = FLOWS_ROOT / f"{args.flow}.json"
        if not flow_path.exists():
            print(f"Error: flow '{args.flow}' not found", file=sys.stderr)
            raise SystemExit(1)
        with open(flow_path) as f:
            flow = json.load(f)
        for i, s in enumerate(flow.get("steps", []), 1):
            print(f"  {i}. {s['treatment']}")


def cmd_init(args, _parser):
    from pathlib import Path
    from pymyx.core.pipeline import DATASETS_PREFIX, PIPELINE_STEPS

    dataset = args.dataset
    raw_dir = Path(DATASETS_PREFIX) / dataset / "00_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Generate a declarative flow template with explicit input/output per step
    from pymyx.core.flow import FLOWS_ROOT
    flow_path = FLOWS_ROOT / f"{dataset.lower()}.json"
    if flow_path.exists():
        print(f"Flow already exists: {flow_path}")
        raise SystemExit(1)

    def _step_entry(s):
        entry = {"treatment": s["treatment"], "input": s["input"]}
        if "output" in s:
            entry["output"] = s["output"]
        return entry

    flow = {
        "name": dataset.lower(),
        "description": f"Pipeline for dataset {dataset}",
        "dataset": dataset,
        "params": {},
        "steps": [_step_entry(s) for s in PIPELINE_STEPS],
    }
    FLOWS_ROOT.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(json.dumps(flow, indent=4) + "\n")

    print(f"Created {raw_dir}/")
    print(f"Created {flow_path}")
    print()
    print(f"Next steps:")
    print(f"  1. Copy your CSV files into {raw_dir}/")
    print(f"  2. Run: pymyx flow {dataset.lower()}")


def cmd_status(_args, _parser):
    from datetime import datetime
    from pathlib import Path
    from pymyx.core.flow import FLOWS_ROOT, _resolve_path
    from pymyx.core.pipeline import DATASETS_PREFIX, PIPELINE_STEPS, resolve_paths

    external = {s["treatment"] for s in PIPELINE_STEPS if s.get("external")}

    flows = sorted(FLOWS_ROOT.glob("*.json"))
    if not flows:
        print("No flows found.")
        return

    for flow_path in flows:
        with open(flow_path) as f:
            flow = json.load(f)

        name = flow.get("name", flow_path.stem)
        dataset = flow.get("dataset")
        if not dataset:
            print(f"{name} (no dataset)")
            continue

        print(f"{name} ({dataset})")

        all_ok = True
        for s in flow.get("steps", []):
            treatment = s["treatment"]
            if "output" in s:
                out_dir = Path(_resolve_path(dataset, s["output"]))
            else:
                _, out_str = resolve_paths(dataset, treatment)
                out_dir = Path(out_str)

            out_name = out_dir.name

            if out_dir.exists():
                files = [f for f in out_dir.rglob("*") if f.is_file()]
                n_files = len(files)
                if files:
                    last_mod = max(f.stat().st_mtime for f in files)
                    last_date = datetime.fromtimestamp(last_mod).strftime("%Y-%m-%d")
                else:
                    last_date = "-"
            else:
                n_files = 0
                last_date = "-"

            if n_files == 0 and treatment not in external:
                all_ok = False

            print(f"  {treatment:<14s} {out_name:<18s} {n_files:>4d} files   last: {last_date}")

        if all_ok:
            print("  -> up-to-date")
        else:
            print("  -> incomplete")
        print()


def main():
    parser = argparse.ArgumentParser(
        prog="pymyx",
        description="PyMyx — IoT time-series processing pipeline",
    )
    sub = parser.add_subparsers(dest="command")

    # pymyx flow
    p_flow = sub.add_parser("flow", help="Run a flow (multi-step pipeline)")
    p_flow.add_argument("flow", help="Flow name (e.g. valvometry_daily)")
    p_flow.add_argument("--from-step", default=None,
                        help="Start from this step (inclusive)")
    p_flow.add_argument("--to-step", default=None,
                        help="Stop at this step (inclusive)")
    p_flow.add_argument("--step", default=None,
                        help="Run a single step from the flow")
    _add_common_args(p_flow)

    # pymyx run
    p_run = sub.add_parser("run", help="Run a single treatment")
    p_run.add_argument("treatment", help="Treatment name")
    p_run.add_argument("--input", required=True, help="Input directory")
    p_run.add_argument("--output", required=True, help="Output directory")
    p_run.add_argument("--params", default="{}", help="JSON params string")
    _add_common_args(p_run)

    # pymyx list
    p_list = sub.add_parser("list", help="List available flows, treatments, or steps")
    p_list.add_argument("what", choices=["flows", "treatments", "steps"],
                        help="What to list")
    p_list.add_argument("--flow", default=None,
                        help="Flow name (required for 'steps')")

    # pymyx init
    p_init = sub.add_parser("init", help="Initialize a new dataset")
    p_init.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")

    # pymyx status
    p_status = sub.add_parser("status", help="Show status of all datasets")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        raise SystemExit(0)

    if args.command == "flow":
        cmd_flow(args, p_flow)
    elif args.command == "run":
        cmd_run(args, p_run)
    elif args.command == "list":
        cmd_list(args, p_list)
    elif args.command == "init":
        cmd_init(args, p_init)
    elif args.command == "status":
        cmd_status(args, p_status)


if __name__ == "__main__":
    main()
