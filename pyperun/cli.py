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

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last,
             from_step=args.from_step, to_step=args.to_step, step=args.step,
             dry_run=args.dry_run)


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
    import json
    from pyperun.core.runner import TREATMENTS_ROOT

    name = args.treatment
    path = TREATMENTS_ROOT / name / "treatment.json"
    if not path.exists():
        print(f"Error: treatment '{name}' not found", file=sys.stderr)
        raise SystemExit(1)

    with open(path) as f:
        t = json.load(f)

    print(f"\n\033[1m{t['name']}\033[0m — {t['description']}")
    print()
    if "input_format" in t:
        print(f"  Input:   {t['input_format']}")
    if "output_format" in t:
        print(f"  Output:  {t['output_format']}")
    print()
    print("  Params:")
    for pname, pdef in t.get("params", {}).items():
        ptype = pdef.get("type", "")
        default = json.dumps(pdef.get("default"), ensure_ascii=False)
        desc = pdef.get("description", "")
        print(f"    \033[1m{pname}\033[0m  ({ptype})  default: {default}")
        if desc:
            print(f"      {desc}")
    print()


def cmd_list(args, _parser):
    import json as _json
    from pyperun.core.flow import get_flows_root, _find_flow as find_flow
    from pyperun.core.runner import TREATMENTS_ROOT

    if args.what == "flows":
        for f in sorted(get_flows_root().glob("*.json")):
            print(f"  {f.stem}")
    elif args.what == "treatments":
        for d in sorted(TREATMENTS_ROOT.iterdir()):
            p = d / "treatment.json"
            if p.exists():
                with open(p) as f:
                    t = _json.load(f)
                desc = t.get("description", "")
                print(f"  {d.name:<16s}  {desc}")
    elif args.what == "presets":
        presets = _load_presets()
        from pathlib import Path
        source = "presets.json" if (Path.cwd() / _PRESETS_FILENAME).exists() else "built-in only"
        print(f"  ({source})")
        print()
        for name, spec in presets.items():
            desc = spec.get("description", "")
            steps = spec.get("steps") or ["all steps"]
            steps_str = " → ".join(steps) if isinstance(steps, list) else "all steps"
            print(f"  {name:<16s}  {desc}")
            print(f"  {'':16s}  {steps_str}")
            print()
    elif args.what == "steps":
        if not args.flow:
            print("Error: --flow required with 'pyperun list steps'", file=sys.stderr)
            raise SystemExit(1)
        try:
            flow_path = find_flow(args.flow)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            raise SystemExit(1)
        with open(flow_path) as f:
            flow = json.load(f)
        for i, s in enumerate(flow.get("steps", []), 1):
            print(f"  {i}. {s['treatment']}")


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
    import os
    from pathlib import Path
    from pyperun.core.pipeline import PIPELINE_STEPS

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()
    preset_name = args.preset

    presets = _load_presets()
    if preset_name not in presets:
        print(f"Error: unknown preset '{preset_name}'. Available: {', '.join(presets)}", file=sys.stderr)
        raise SystemExit(1)

    allowed = presets[preset_name]["steps"]  # None = all steps

    datasets_dir = project_dir / "datasets" / dataset
    flows_dir = project_dir / "flows"
    treatments_dir = project_dir / "treatments"

    # Filter steps by preset
    steps = [s for s in PIPELINE_STEPS if allowed is None or s["treatment"] in allowed]

    flow_name = args.flow if args.flow else dataset.lower()
    flow_path = flows_dir / f"{flow_name}.json"
    flow_exists = flow_path.exists()

    if flow_exists and not args.force:
        print(f"Flow already exists: {flow_path}")
        print(f"Use --force to regenerate it from preset '{preset_name}'.")
        raise SystemExit(1)

    if flow_exists and args.force:
        answer = input(f"Overwrite {flow_path.name} with preset '{preset_name}'? [y/N] ").strip().lower()
        if answer != "y":
            print("Cancelled.")
            raise SystemExit(0)

    # Create pipeline stage directories (idempotent — safe if dirs already exist)
    for s in steps:
        for key in ("input", "output"):
            if key in s and not s.get("external"):
                (datasets_dir / s[key]).mkdir(parents=True, exist_ok=True)

    # 00_raw: symlink to existing data or create empty
    raw_dir = datasets_dir / "00_raw"
    if args.raw:
        raw_src = Path(args.raw).resolve()
        if not raw_src.exists():
            print(f"Error: --raw path does not exist: {raw_src}", file=sys.stderr)
            raise SystemExit(1)
        if raw_dir.exists() and not raw_dir.is_symlink():
            raw_dir.rmdir()
        if raw_dir.is_symlink():
            raw_dir.unlink()
        os.symlink(raw_src, raw_dir)

    # Create treatments/ placeholder
    treatments_dir.mkdir(parents=True, exist_ok=True)

    # Generate flow with all params explicit
    from pyperun.core.runner import TREATMENTS_ROOT

    def _step_entry(s):
        entry = {"treatment": s["treatment"], "input": s["input"]}
        if "output" in s:
            entry["output"] = s["output"]
        t_path = TREATMENTS_ROOT / s["treatment"] / "treatment.json"
        if t_path.exists():
            with open(t_path) as f:
                t = json.load(f)
            params = {k: v["default"] for k, v in t.get("params", {}).items()}
            if params:
                entry["params"] = params
        return entry

    flow = {
        "name": flow_name,
        "description": f"Pipeline for dataset {dataset}",
        "dataset": dataset,
        "params": {},
        "steps": [_step_entry(s) for s in steps],
    }
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(json.dumps(flow, indent=4) + "\n")

    # Print created structure
    action = "Regenerated flow" if flow_exists else "Initialized project"
    print(f"{action} at {project_dir}/  (preset: {preset_name})")
    print()
    print(f"  flows/")
    print(f"    {flow_name}.json")
    print(f"  treatments/              (custom treatments, optional)")
    print(f"  datasets/{dataset}/")
    seen = []
    for s in steps:
        for key in ("input", "output"):
            stage = s.get(key)
            if stage and stage not in seen and not s.get("external"):
                seen.append(stage)
    for stage in seen:
        suffix = "  <- symlink" if stage == "00_raw" and args.raw else ""
        print(f"    {stage}/{suffix}")
    print()
    if args.raw:
        print(f"  00_raw -> {Path(args.raw).resolve()}")
        print()
    edit_hint = "configure params" if preset_name == "full" else "configure columns, tz, aggregation window"
    print("Next steps:")
    print(f"  1. Edit flows/{flow_name}.json  ({edit_hint})")
    print(f"  2. pyperun flow {flow_name}")


def cmd_delete(args, _parser):
    import shutil
    from pathlib import Path
    from pyperun.core.flow import get_flows_root

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    dataset_dir = project_dir / "datasets" / dataset
    flows_root = get_flows_root()

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

    # Bail early if nothing to delete
    if not dataset_dir.exists() and not flow_files:
        print(f"Nothing found for dataset '{dataset}'.", file=sys.stderr)
        raise SystemExit(1)

    # Show what will be deleted
    print(f"Will delete:")
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

    # Delete dataset directory
    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            raw_dir.unlink()
        shutil.rmtree(dataset_dir)
        print(f"Deleted datasets/{dataset}/")

    # Delete flow files
    for fp in flow_files:
        fp.unlink()
        print(f"Deleted flows/{fp.name}")


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


def cmd_status(_args, _parser):
    from datetime import datetime
    from pathlib import Path
    from pyperun.core.flow import get_flows_root, _resolve_path
    from pyperun.core.pipeline import DATASETS_PREFIX, PIPELINE_STEPS, resolve_paths

    external = {s["treatment"] for s in PIPELINE_STEPS if s.get("external")}

    flows = sorted(get_flows_root().glob("*.json"))
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
    _add_common_args(p_flow)

    # pyperun new
    p_new = sub.add_parser("new", help="Scaffold a new treatment (treatment.json + run.py)")
    p_new.add_argument("name", help="Treatment name (e.g. smooth, normalize_temp)")

    # pyperun describe
    p_describe = sub.add_parser("describe", help="Show description, input/output format and params of a treatment")
    p_describe.add_argument("treatment", help="Treatment name (e.g. parse, aggregate)")

    # pyperun list
    p_list = sub.add_parser("list", help="List available flows, treatments, or steps")
    p_list.add_argument("what", choices=["flows", "treatments", "steps", "presets"],
                        help="What to list")
    p_list.add_argument("--flow", default=None,
                        help="Flow name (required for 'steps')")

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
