import argparse
import json
import sys

from pyperun.core.timefilter import parse_iso_utc

_BANNER = """\
\033[1;36m  _ __  _   _ _ __   ___ _ __ _   _ _ __
 | '_ \\| | | | '_ \\ / _ \\ '__| | | | '_ \\
 | |_) | |_| | |_) |  __/ |  | |_| | | | |
 | .__/ \\__, | .__/ \\___|_|   \\__,_|_| |_|
 |_|    |___/|_|                           \033[0m
\033[2m  IoT time-series processing pipeline\033[0m
"""


def _print_banner():
    if sys.stdout.isatty():
        print(_BANNER)


class _Parser(argparse.ArgumentParser):
    def print_help(self, file=None):
        _print_banner()
        super().print_help(file)


def _add_common_args(parser):
    """Add time filtering and output-mode args shared by flow commands."""
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
    from pyperun.core.flow import run_flow

    if args.step and (args.from_step or args.to_step):
        parser.error("--step is mutually exclusive with --from-step/--to-step")

    time_from, time_to = _validate_common_args(parser, args)

    params_override = None
    if args.params:
        try:
            params_override = json.loads(args.params)
        except json.JSONDecodeError as e:
            parser.error(f"--params: invalid JSON: {e}")

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last,
             from_step=args.from_step, to_step=args.to_step, step=args.step,
             dry_run=args.dry_run, params_override=params_override)


def cmd_new(args, _parser):
    from pathlib import Path
    from pyperun.core.runner import TREATMENTS_ROOT

    name = args.name
    # Local treatments/ takes priority
    local_dir = Path.cwd() / "treatments" / name
    builtin_dir = TREATMENTS_ROOT / name

    if local_dir.exists() or builtin_dir.exists():
        print(f"Error: treatment '{name}' already exists", file=sys.stderr)
        raise SystemExit(1)

    target = local_dir
    target.mkdir(parents=True)

    treatment_json = target / "treatment.json"
    treatment_json.write_text(json.dumps({
        "name": name,
        "description": "TODO: describe what this treatment does",
        "input_format": "TODO: describe expected input (e.g. Parquet files: `<source>__<domain>__<YYYY-MM-DD>.parquet`)",
        "output_format": "TODO: describe output produced",
        "params": {}
    }, indent=4) + "\n")

    run_py = target / "run.py"
    run_py.write_text(f'''\
from pathlib import Path

import pandas as pd

from pyperun.core.filename import list_parquet_files, parse_parquet_path, build_parquet_path


def run(input_dir: str, output_dir: str, params: dict) -> None:
    in_path = Path(input_dir)
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    for pf in list_parquet_files(in_path):
        parts = parse_parquet_path(pf)

        df = pd.read_parquet(pf)
        if df.empty:
            continue

        # --- your logic here ---

        out_file = build_parquet_path(parts.with_step("{name}"), out_path)
        df.to_parquet(out_file, index=False)
        print(f"  [{name}] {{parts.device_id}} {{parts.day}}: {{len(df)}} rows -> {{out_file.name}}")
''')

    print(f"Created treatments/{name}/treatment.json")
    print(f"Created treatments/{name}/run.py")
    print()
    print("Next steps:")
    print(f"  1. Edit treatments/{name}/treatment.json  (add params + descriptions)")
    print(f"  2. Edit treatments/{name}/run.py          (implement logic)")
    print(f"  3. Add to your flow:  {{\"treatment\": \"{name}\", \"input\": \"...\", \"output\": \"...\"}}")



def cmd_describe(args, _parser):
    from pyperun.core.api import describe_treatment

    try:
        t = describe_treatment(args.treatment)
    except FileNotFoundError:
        print(f"Error: treatment '{args.treatment}' not found", file=sys.stderr)
        raise SystemExit(1)

    if args.format == "json":
        print(json.dumps(t, indent=2, ensure_ascii=False))
        return

    print(f"\n\033[1m{t['name']}\033[0m — {t['description']}")
    print()
    if t.get("input_format"):
        print(f"  Input:   {t['input_format']}")
    if t.get("output_format"):
        print(f"  Output:  {t['output_format']}")
    print()
    print("  Params:")
    for p in t.get("params", []):
        default = json.dumps(p["default"], ensure_ascii=False)
        print(f"    \033[1m{p['name']}\033[0m  ({p['type']})  default: {default}")
        if p.get("description"):
            print(f"      {p['description']}")
    print()


def cmd_list(args, _parser):
    from pyperun.core.api import list_flows, list_treatments, list_steps

    fmt = args.format

    if args.what == "flows":
        data = list_flows()
        if fmt == "json":
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return
        for f in data:
            print(f"  {f['name']}")

    elif args.what == "treatments":
        data = list_treatments()
        if fmt == "json":
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return
        for t in data:
            print(f"  {t['name']:<16s}  {t['description']}")

    elif args.what == "presets":
        from pathlib import Path
        from pyperun.core.api import list_presets
        data = list_presets()
        if fmt == "json":
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return
        source = "presets.json" if (Path.cwd() / _PRESETS_FILENAME).exists() else "built-in only"
        print(f"  ({source})")
        print()
        for p in data:
            steps_str = " → ".join(p["steps"]) if p["steps"] else "all steps"
            print(f"  {p['name']:<16s}  {p['description']}")
            print(f"  {'':16s}  {steps_str}")
            print()

    elif args.what == "steps":
        if not args.flow:
            print("Error: --flow required with 'pyperun list steps'", file=sys.stderr)
            raise SystemExit(1)
        try:
            data = list_steps(args.flow)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            raise SystemExit(1)
        if fmt == "json":
            print(json.dumps(data, indent=2, ensure_ascii=False))
            return
        for s in data:
            print(f"  {s['index']}. {s['name']}")


_BUILTIN_PRESETS = {
    "csv": {
        "description": "Core pipeline → exportcsv",
        "steps": ["parse", "clean", "resample", "transform", "normalize", "aggregate", "exportcsv"],
    },
    "parquet": {
        "description": "Core pipeline → exportparquet",
        "steps": ["parse", "clean", "resample", "transform", "normalize", "aggregate", "exportparquet"],
    },
    "full": {
        "description": "Full pipeline (all steps)",
        "steps": None,  # None = all PIPELINE_STEPS
    },
}

_PRESETS_FILENAME = "presets.json"


def _load_presets() -> dict:
    """Merge built-in presets with project-level presets.json (project wins on conflicts)."""
    from pathlib import Path
    presets = dict(_BUILTIN_PRESETS)
    project_file = Path.cwd() / _PRESETS_FILENAME
    if project_file.exists():
        try:
            with open(project_file) as f:
                project_presets = json.load(f)
            for name, spec in project_presets.items():
                # Accept both {"steps": [...], "description": "..."} and ["step1", ...]
                if isinstance(spec, list):
                    spec = {"steps": spec, "description": ""}
                presets[name] = spec
        except Exception as e:
            print(f"Warning: could not load {_PRESETS_FILENAME}: {e}", file=sys.stderr)
    return presets


def cmd_init(args, _parser):
    from pathlib import Path
    from pyperun.core.api import init_dataset

    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    # Interactive confirmation when --force overwrites an existing flow
    if args.force:
        flow_name = args.flow or args.dataset.lower()
        flow_path = project_dir / "flows" / f"{flow_name}.json"
        if flow_path.exists():
            answer = input(f"Overwrite {flow_path.name} with preset '{args.preset}'? [y/N] ").strip().lower()
            if answer != "y":
                print("Cancelled.")
                raise SystemExit(0)

    try:
        result = init_dataset(
            dataset=args.dataset,
            preset=args.preset,
            flow_name=args.flow,
            raw=args.raw,
            force=args.force,
            project_dir=str(project_dir),
        )
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except FileExistsError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)

    action = "Regenerated flow" if result["action"] == "regenerated" else "Initialized project"
    print(f"{action} at {project_dir}/  (preset: {args.preset})")
    print()
    print(f"  flows/")
    print(f"    {result['flow']}.json")
    print(f"  treatments/              (custom treatments, optional)")
    print(f"  datasets/{result['dataset']}/")
    for d in result["created_dirs"]:
        stage = d.split("/")[-1]
        suffix = "  <- symlink" if stage == "00_raw" and result["raw_symlink"] else ""
        print(f"    {stage}/{suffix}")
    print()
    if result["raw_symlink"]:
        print(f"  00_raw -> {result['raw_symlink']}")
        print()
    edit_hint = "configure params" if args.preset == "full" else "configure columns, tz, aggregation window"
    print("Next steps:")
    print(f"  1. Edit flows/{result['flow']}.json  ({edit_hint})")
    print(f"  2. pyperun flow {result['flow']}")


def cmd_delete(args, _parser):
    from pathlib import Path
    from pyperun.core.api import delete_dataset
    from pyperun.core.flow import get_flows_root

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    # Preview before deleting
    dataset_dir = project_dir / "datasets" / dataset
    flows_root  = get_flows_root()
    flow_files  = []
    for fp in sorted(flows_root.glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            if flow.get("dataset") == dataset:
                flow_files.append(fp)
        except Exception:
            pass

    if not dataset_dir.exists() and not flow_files:
        print(f"Nothing found for dataset '{dataset}'.", file=sys.stderr)
        raise SystemExit(1)

    print("Will delete:")
    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            print(f"  datasets/{dataset}/  (00_raw is a symlink -> {raw_dir.resolve()}, source kept)")
        else:
            print(f"  datasets/{dataset}/")
    for fp in flow_files:
        print(f"  flows/{fp.name}")

    if not args.yes:
        answer = input("\nConfirm deletion? [y/N] ").strip().lower()
        if answer != "y":
            print("Cancelled.")
            return

    try:
        result = delete_dataset(dataset=dataset, project_dir=str(project_dir))
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)

    for d in result["deleted_dirs"]:
        print(f"Deleted {d}")
    for f in result["deleted_flows"]:
        print(f"Deleted {f}")


def _pyperun_version() -> str:
    try:
        from importlib.metadata import version
        return version("pyperun")
    except Exception:
        return "unknown"


def cmd_export(args, _parser):
    import io
    import tarfile
    from datetime import datetime, timezone
    from pathlib import Path
    from pyperun.core.flow import get_flows_root

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()
    dataset_dir = project_dir / "datasets" / dataset
    flows_root = get_flows_root()

    if not dataset_dir.exists():
        print(f"Error: dataset directory not found: {dataset_dir}", file=sys.stderr)
        raise SystemExit(1)

    # Find flows referencing this dataset
    flow_files = []
    for fp in sorted(flows_root.glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            if flow.get("dataset") == dataset:
                flow_files.append(fp)
        except Exception:
            pass

    if not flow_files:
        print(f"Error: no flow found referencing dataset '{dataset}'", file=sys.stderr)
        raise SystemExit(1)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archive_name = f"{dataset}_{date_str}.tar.gz"
    archive_path = Path.cwd() / archive_name

    treatments_dir = project_dir / "treatments"

    manifest = {
        "pyperun_version": _pyperun_version(),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset,
        "flows": [fp.name for fp in flow_files],
        "includes_processed_stages": args.full,
    }
    manifest_bytes = json.dumps(manifest, indent=2).encode()

    print(f"Exporting {dataset} → {archive_name}")
    print()

    with tarfile.open(archive_path, "w:gz") as tar:
        # manifest
        info = tarfile.TarInfo("manifest.json")
        info.size = len(manifest_bytes)
        tar.addfile(info, io.BytesIO(manifest_bytes))

        # flows
        for fp in flow_files:
            tar.add(fp, arcname=f"flows/{fp.name}")
            print(f"  + flows/{fp.name}")

        # local treatments (all, small, version-controlled with project)
        if treatments_dir.exists():
            treatment_files = [f for f in sorted(treatments_dir.rglob("*")) if f.is_file()]
            if treatment_files:
                for f in treatment_files:
                    tar.add(f, arcname=str(f.relative_to(project_dir)))
                n_treatments = sum(1 for d in treatments_dir.iterdir() if d.is_dir())
                print(f"  + treatments/ ({n_treatments} custom treatment(s))")

        # raw data (follow symlinks)
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.exists() or raw_dir.is_symlink():
            actual_raw = raw_dir.resolve()
            raw_files = sorted(f for f in actual_raw.rglob("*") if f.is_file())
            for f in raw_files:
                tar.add(f, arcname=f"datasets/{dataset}/00_raw/{f.relative_to(actual_raw)}")
            print(f"  + datasets/{dataset}/00_raw/ ({len(raw_files)} files)")

        # processed stages (optional)
        if args.full:
            for stage_dir in sorted(dataset_dir.iterdir()):
                if stage_dir.name == "00_raw" or not stage_dir.is_dir():
                    continue
                stage_files = [f for f in sorted(stage_dir.rglob("*")) if f.is_file()]
                if stage_files:
                    for f in stage_files:
                        tar.add(f, arcname=str(f.relative_to(project_dir)))
                    print(f"  + datasets/{dataset}/{stage_dir.name}/ ({len(stage_files)} files)")

    size_mb = archive_path.stat().st_size / 1024 / 1024
    print()
    print(f"Done: {archive_path}  ({size_mb:.1f} MB)")
    print()
    print("To import on another server:")
    print(f"  pyperun import {archive_name}")
    for fp in flow_files:
        print(f"  pyperun flow {fp.stem}")


def cmd_import(args, _parser):
    import tarfile
    from pathlib import Path

    archive = Path(args.archive)
    if not archive.exists():
        print(f"Error: archive not found: {archive}", file=sys.stderr)
        raise SystemExit(1)

    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    print(f"Importing {archive.name} → {project_dir}/")
    print()

    with tarfile.open(archive, "r:gz") as tar:
        # Read manifest first
        manifest = {}
        try:
            manifest = json.loads(tar.extractfile("manifest.json").read())
        except Exception:
            pass

        # Safety check: only extract expected paths
        safe_prefixes = ("flows/", "treatments/", "datasets/", "manifest.json")
        members = [m for m in tar.getmembers()
                   if any(m.name.startswith(p) for p in safe_prefixes)]
        tar.extractall(project_dir, members=members)

    dataset = manifest.get("dataset", "?")
    flows = manifest.get("flows", [])
    exported_at = manifest.get("exported_at", "?")[:10]
    pyperun_ver = manifest.get("pyperun_version", "?")
    has_processed = manifest.get("includes_processed_stages", False)

    print(f"  Dataset:   {dataset}")
    print(f"  Exported:  {exported_at}  (pyperun {pyperun_ver})")
    print(f"  Flows:     {', '.join(f.replace('.json','') for f in flows)}")
    print(f"  Stages:    {'raw + processed' if has_processed else 'raw only'}")
    print()
    if not has_processed:
        print("Pipeline not yet run on this machine. Next steps:")
    else:
        print("Processed stages included. To re-run or continue:")
    for f in flows:
        print(f"  pyperun flow {f.replace('.json', '')}")


def cmd_help(_args, _parser):
    _print_banner()
    print("""\
Commands:

  pyperun flow <flow>             Run a full flow
    --step <name>                 Run a single named step
    --from-step <name>            Start from this step (inclusive)
    --to-step <name>              Stop at this step (inclusive)
    --from / --to                 Time window (ISO 8601)
    --output-mode                 append | replace | full-replace
    --last                        Incremental: process only new data
    --dry-run                     Print execution plan without running
    --params <JSON>               Override params for every step (e.g. '{"mode": "reset"}')

  pyperun new <name>              Scaffold a new treatment (treatment.json + run.py)

  pyperun describe <treatment>    Show description, input/output and params of a treatment

  pyperun list flows              List available flows
  pyperun list treatments         List available treatments (with descriptions)
  pyperun list presets            List available presets (built-in + presets.json)
  pyperun list steps --flow <f>   List steps of a flow

  pyperun init <DATASET>          Scaffold a new dataset
    --preset <name>               Pipeline preset (default: full — see pyperun list presets)
    --flow <name>                 Flow file name (default: dataset name)
    --force                       Overwrite existing flow (dirs untouched)
    --path <dir>                  Target directory (default: cwd)
    --raw <dir>                   Symlink to existing raw CSV dir

  pyperun export <DATASET>        Export dataset to portable archive (flow + treatments + raw)
    --path <dir>                  Project directory (default: cwd)
    --full                        Also include processed stages

  pyperun import <archive>        Import a dataset archive
    --path <dir>                  Target project directory (default: cwd)

  pyperun delete <DATASET>        Delete a dataset and its flow(s)
    --path <dir>                  Project directory (default: cwd)
    -y, --yes                     Skip confirmation prompt

  pyperun status                  Show status of all datasets
  pyperun upgrade                 Update pyperun via git + pip
    --path <dir>                  Path to pyperun git repo (if auto-detect fails)
  pyperun help                    Show this help
""")


def cmd_upgrade(args, _parser):
    import subprocess
    from pathlib import Path

    # Use --path if provided, otherwise walk up from __file__
    if args.path:
        project_dir = Path(args.path).resolve()
        if not (project_dir / ".git").exists():
            print(f"Error: no git repository found at {project_dir}", file=sys.stderr)
            raise SystemExit(1)
    else:
        project_dir = None
        for parent in Path(__file__).resolve().parents:
            if (parent / ".git").exists():
                project_dir = parent
                break
        if project_dir is None:
            print(
                "Error: could not find pyperun git repository.\n"
                "Hint: use --path to specify it:\n"
                "  pyperun upgrade --path /path/to/pyperun",
                file=sys.stderr,
            )
            raise SystemExit(1)

    # Show current version
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "-1"],
            cwd=project_dir, capture_output=True, text=True, check=True,
        )
        print(f"Current version: {result.stdout.strip()}")
        print(f"Project directory: {project_dir}")
    except subprocess.CalledProcessError:
        print(f"Project directory: {project_dir}")

    answer = input("Upgrade pyperun? [y/N] ").strip().lower()
    if answer != "y":
        print("Upgrade cancelled.")
        return

    print("Pulling latest changes...")
    subprocess.run(["git", "pull"], cwd=project_dir, check=True)

    print("Reinstalling...")
    subprocess.run([sys.executable, "-m", "pip", "install", "--break-system-packages", "."],
                   cwd=project_dir, check=True)

    print("Done.")


def cmd_status(args, _parser):
    from pyperun.core.api import get_status

    data = get_status()

    if args.format == "json":
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return

    if not data:
        print("No flows found.")
        return

    for entry in data:
        name = entry["flow"]
        dataset = entry["dataset"]

        if entry["status"] == "no-dataset":
            print(f"{name} (no dataset)")
            continue

        print(f"{name} ({dataset})")
        for s in entry["steps"]:
            last = s["last_modified"] or "-"
            print(f"  {s['treatment']:<14s} {s['output']:<18s} {s['n_files']:>4d} files   last: {last}")

        print(f"  -> {entry['status']}")
        print()


def main():
    parser = _Parser(
        prog="pyperun",
        description="Pyperun — IoT time-series processing pipeline",
    )
    sub = parser.add_subparsers(dest="command")

    # pyperun flow
    p_flow = sub.add_parser("flow", help="Run a flow (multi-step pipeline)")
    p_flow.add_argument("flow", help="Flow name (e.g. valvometry_daily)")
    p_flow.add_argument("--from-step", default=None,
                        help="Start from this step (inclusive)")
    p_flow.add_argument("--to-step", default=None,
                        help="Stop at this step (inclusive)")
    p_flow.add_argument("--step", default=None,
                        help="Run a single step from the flow")
    p_flow.add_argument("--dry-run", action="store_true",
                        help="Print the execution plan without running anything")
    p_flow.add_argument("--params", default=None, metavar="JSON",
                        help="JSON object of params to override for every step (e.g. '{\"mode\": \"reset\"}')")
    _add_common_args(p_flow)

    # pyperun new
    p_new = sub.add_parser("new", help="Scaffold a new treatment (treatment.json + run.py)")
    p_new.add_argument("name", help="Treatment name (e.g. smooth, normalize_temp)")

    # pyperun describe
    p_describe = sub.add_parser("describe", help="Show description, input/output format and params of a treatment")
    p_describe.add_argument("treatment", help="Treatment name (e.g. parse, aggregate)")
    p_describe.add_argument("--format", choices=["text", "json"], default="text",
                            help="Output format (default: text)")

    # pyperun list
    p_list = sub.add_parser("list", help="List available flows, treatments, or steps")
    p_list.add_argument("what", choices=["flows", "treatments", "steps", "presets"],
                        help="What to list")
    p_list.add_argument("--flow", default=None,
                        help="Flow name (required for 'steps')")
    p_list.add_argument("--format", choices=["text", "json"], default="text",
                        help="Output format (default: text)")

    # pyperun init
    p_init = sub.add_parser("init", help="Initialize a new dataset project skeleton")
    p_init.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")
    p_init.add_argument("--preset", default="full", metavar="PRESET",
                        help="Pipeline preset name (built-in: csv, parquet, full — or defined in presets.json)")
    p_init.add_argument("--flow", default=None, metavar="NAME",
                        help="Flow file name (default: dataset name). Use to create multiple flows for the same dataset.")
    p_init.add_argument("--path", default=None,
                        help="Project directory to create skeleton in (default: current directory)")
    p_init.add_argument("--raw", default=None,
                        help="Path to existing raw CSV directory (creates a symlink as 00_raw)")
    p_init.add_argument("--force", action="store_true",
                        help="Overwrite existing flow (directories are left untouched)")

    # pyperun delete
    p_delete = sub.add_parser("delete", help="Delete a dataset and its associated flow(s)")
    p_delete.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")
    p_delete.add_argument("--path", default=None,
                          help="Project directory (default: current directory)")
    p_delete.add_argument("-y", "--yes", action="store_true",
                          help="Skip confirmation prompt")

    # pyperun export
    p_export = sub.add_parser("export", help="Export a dataset (flow + treatments + raw data) to a portable archive")
    p_export.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")
    p_export.add_argument("--path", default=None,
                          help="Project directory (default: current directory)")
    p_export.add_argument("--full", action="store_true",
                          help="Include processed stages (not just raw data)")

    # pyperun import
    p_import = sub.add_parser("import", help="Import a dataset archive exported with pyperun export")
    p_import.add_argument("archive", help="Path to the .tar.gz archive")
    p_import.add_argument("--path", default=None,
                          help="Target project directory (default: current directory)")

    # pyperun status
    p_status = sub.add_parser("status", help="Show status of all datasets")
    p_status.add_argument("--format", choices=["text", "json"], default="text",
                          help="Output format (default: text)")

    # pyperun upgrade
    p_upgrade = sub.add_parser("upgrade", help="Pull latest changes and reinstall pyperun")
    p_upgrade.add_argument("--path", default=None,
                           help="Path to the pyperun git repository (auto-detected if omitted)")

    # pyperun help
    p_help = sub.add_parser("help", help="Show detailed help for all commands")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        raise SystemExit(0)

    if args.command == "flow":
        cmd_flow(args, p_flow)
    elif args.command == "new":
        cmd_new(args, p_new)
    elif args.command == "describe":
        cmd_describe(args, p_describe)
    elif args.command == "list":
        cmd_list(args, p_list)
    elif args.command == "init":
        cmd_init(args, p_init)
    elif args.command == "delete":
        cmd_delete(args, p_delete)
    elif args.command == "export":
        cmd_export(args, p_export)
    elif args.command == "import":
        cmd_import(args, p_import)
    elif args.command == "status":
        cmd_status(args, p_status)
    elif args.command == "upgrade":
        cmd_upgrade(args, p_upgrade)
    elif args.command == "help":
        cmd_help(args, p_help)


if __name__ == "__main__":
    main()
