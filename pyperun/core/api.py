"""Pure Python API for pyperun — returns dicts/lists, never prints.

Designed to be imported directly by Flask or other external tools
without going through the CLI subprocess.

Quick Flask integration example::

    from pyperun.core.api import get_status, list_flows, get_run_events, list_runs
    from pyperun.core.flow import run_flow

    @app.get("/api/status")
    def api_status():
        return jsonify(get_status())

    @app.get("/api/flows")
    def api_flows():
        return jsonify(list_flows())

    @app.post("/api/run/<flow_name>")
    def api_run(flow_name):
        # Run in a background thread and return run_id immediately
        from threading import Thread
        from pyperun.core.logger import new_run_id
        # run_id is printed to stdout; alternatively parse it from logs
        t = Thread(target=run_flow, args=(flow_name,), daemon=True)
        t.start()
        return jsonify({"status": "started"})

    @app.get("/api/runs")
    def api_runs():
        return jsonify(list_runs())

    @app.get("/api/runs/<run_id>")
    def api_run_events(run_id):
        return jsonify(get_run_events(run_id))
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

def _get_flows_root() -> Path:
    from pyperun.core.flow import get_flows_root
    return get_flows_root()


def list_flows() -> list[dict]:
    """Return available flows as a list of dicts.

    Each dict: {name, description, dataset, n_steps}
    """
    result = []
    for fp in sorted(_get_flows_root().glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            result.append({
                "name": flow.get("name", fp.stem),
                "description": flow.get("description", ""),
                "dataset": flow.get("dataset"),
                "n_steps": len(flow.get("steps", [])),
            })
        except Exception:
            result.append({"name": fp.stem, "description": "", "dataset": None, "n_steps": 0})
    return result


def list_steps(flow_name: str) -> list[dict]:
    """Return steps of a flow as a list of dicts.

    Each dict: {index, treatment, name, input, output, params}
    Raises FileNotFoundError if the flow does not exist.
    """
    from pyperun.core.flow import _find_flow
    flow_path = _find_flow(flow_name)
    with open(flow_path) as f:
        flow = json.load(f)

    result = []
    for i, s in enumerate(flow.get("steps", []), 1):
        result.append({
            "index": i,
            "treatment": s["treatment"],
            "name": s.get("name", s["treatment"]),
            "input": s.get("input", ""),
            "output": s.get("output", ""),
            "params": s.get("params", {}),
        })
    return result


# ---------------------------------------------------------------------------
# Treatments
# ---------------------------------------------------------------------------

def list_treatments() -> list[dict]:
    """Return available treatments as a list of dicts.

    Each dict: {name, description}
    """
    from pyperun.core.runner import TREATMENTS_ROOT
    result = []
    for d in sorted(TREATMENTS_ROOT.iterdir()):
        p = d / "treatment.json"
        if not p.exists():
            continue
        try:
            with open(p) as f:
                t = json.load(f)
            result.append({"name": d.name, "description": t.get("description", "")})
        except Exception:
            result.append({"name": d.name, "description": ""})
    return result


def describe_treatment(name: str) -> dict:
    """Return full description of a treatment.

    Returns a dict with keys: name, description, input_format, output_format, params.
    Each param: {name, type, default, description}
    Raises FileNotFoundError if the treatment does not exist.
    """
    from pyperun.core.runner import TREATMENTS_ROOT
    path = TREATMENTS_ROOT / name / "treatment.json"
    if not path.exists():
        raise FileNotFoundError(f"Treatment '{name}' not found")
    with open(path) as f:
        t = json.load(f)

    params = []
    for pname, pdef in t.get("params", {}).items():
        params.append({
            "name": pname,
            "type": pdef.get("type", ""),
            "default": pdef.get("default"),
            "description": pdef.get("description", ""),
        })

    return {
        "name": t.get("name", name),
        "description": t.get("description", ""),
        "input_format": t.get("input_format", ""),
        "output_format": t.get("output_format", ""),
        "params": params,
    }


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def get_status() -> list[dict]:
    """Return pipeline status for all flows.

    Each entry: {flow, dataset, status, steps}
    Each step: {treatment, output, n_files, last_modified}
    status is 'up-to-date' | 'incomplete' | 'no-dataset'
    """
    from pyperun.core.flow import get_flows_root, _resolve_path
    from pyperun.core.pipeline import PIPELINE_STEPS, resolve_paths

    external = {s["treatment"] for s in PIPELINE_STEPS if s.get("external")}
    result = []

    for flow_path in sorted(get_flows_root().glob("*.json")):
        try:
            with open(flow_path) as f:
                flow = json.load(f)
        except Exception:
            continue

        name = flow.get("name", flow_path.stem)
        dataset = flow.get("dataset")

        if not dataset:
            result.append({"flow": name, "dataset": None, "status": "no-dataset", "steps": []})
            continue

        steps_out = []
        all_ok = True

        for s in flow.get("steps", []):
            treatment = s["treatment"]
            if "output" in s:
                out_dir = Path(_resolve_path(dataset, s["output"]))
            else:
                _, out_str = resolve_paths(dataset, treatment)
                out_dir = Path(out_str)

            if out_dir.exists():
                files = [f for f in out_dir.rglob("*") if f.is_file()]
                n_files = len(files)
                if files:
                    last_mod = max(f.stat().st_mtime for f in files)
                    last_modified = datetime.fromtimestamp(last_mod).strftime("%Y-%m-%d")
                else:
                    last_modified = None
            else:
                n_files = 0
                last_modified = None

            if n_files == 0 and treatment not in external:
                all_ok = False

            steps_out.append({
                "treatment": treatment,
                "output": out_dir.name,
                "n_files": n_files,
                "last_modified": last_modified,
                "external": treatment in external,
            })

        result.append({
            "flow": name,
            "dataset": dataset,
            "status": "up-to-date" if all_ok else "incomplete",
            "steps": steps_out,
        })

    return result


# ---------------------------------------------------------------------------
# Run history (log polling — Option A)
# ---------------------------------------------------------------------------

def _read_log() -> list[dict]:
    """Read all entries from pyperun.log. Returns [] if the file does not exist."""
    from pyperun.core.logger import LOG_PATH
    if not LOG_PATH.exists():
        return []
    entries = []
    with open(LOG_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return entries


def list_runs(limit: int = 50) -> list[dict]:
    """Return recent pipeline runs as a list of summary dicts.

    Each entry: {run_id, flow, started_at, finished_at, status, n_steps, error}
    status is 'running' | 'success' | 'error'
    Results are sorted by start time descending (most recent first).
    """
    entries = _read_log()

    # Group by run_id
    runs: dict[str, dict] = {}
    for e in entries:
        rid = e.get("run_id")
        if not rid:
            continue
        if rid not in runs:
            runs[rid] = {
                "run_id": rid,
                "flow": e.get("flow"),
                "started_at": e.get("ts"),
                "finished_at": None,
                "status": "running",
                "n_steps_done": 0,
                "error": None,
            }
        status = e.get("status")
        if status == "success":
            runs[rid]["n_steps_done"] += 1
            runs[rid]["finished_at"] = e.get("ts")
            runs[rid]["status"] = "running"  # updated below when all done
        elif status == "error":
            runs[rid]["status"] = "error"
            runs[rid]["error"] = e.get("error")
            runs[rid]["finished_at"] = e.get("ts")

    # A run with no error and a finished_at is 'success'
    for r in runs.values():
        if r["status"] == "running" and r["finished_at"] is not None:
            r["status"] = "success"

    # Sort by started_at descending
    sorted_runs = sorted(runs.values(), key=lambda r: r["started_at"] or "", reverse=True)
    return sorted_runs[:limit]


def get_run_events(run_id: str) -> list[dict]:
    """Return all log events for a specific run_id.

    Each event: {ts, treatment, status, input_dir, output_dir, duration_ms, error, ...}
    Returns [] if run_id is not found or log does not exist.
    """
    return [e for e in _read_log() if e.get("run_id") == run_id]


# ---------------------------------------------------------------------------
# Dataset lifecycle — init, delete, presets
# ---------------------------------------------------------------------------

def list_presets(project_dir: str | None = None) -> list[dict]:
    """Return available presets (built-in + project-level presets.json).

    Each entry: {name, description, steps}  (steps is None for 'full' = all steps)
    """
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
            "steps": None,
        },
    }

    presets = dict(_BUILTIN_PRESETS)
    base = Path(project_dir).resolve() if project_dir else Path.cwd()
    project_file = base / "presets.json"
    if project_file.exists():
        try:
            with open(project_file) as f:
                for name, spec in json.load(f).items():
                    if isinstance(spec, list):
                        spec = {"steps": spec, "description": ""}
                    presets[name] = spec
        except Exception:
            pass

    return [
        {"name": name, "description": spec.get("description", ""), "steps": spec.get("steps")}
        for name, spec in presets.items()
    ]


def init_dataset(
    dataset: str,
    preset: str = "full",
    flow_name: str | None = None,
    raw: str | None = None,
    force: bool = False,
    project_dir: str | None = None,
) -> dict:
    """Scaffold a new dataset: create stage directories and generate a flow JSON.

    Parameters
    ----------
    dataset     : Dataset name (e.g. "MY-EXPERIMENT")
    preset      : Preset name — see list_presets() (default: "full")
    flow_name   : Flow file name; defaults to dataset.lower()
    raw         : Path to existing raw CSV directory (creates a symlink as 00_raw)
    force       : If True, overwrite an existing flow without prompting
    project_dir : Project root directory (default: cwd)

    Returns
    -------
    {
        "dataset":      "MY-EXPERIMENT",
        "flow":         "my-experiment",
        "flow_path":    "flows/my-experiment.json",
        "action":       "created" | "regenerated",
        "created_dirs": ["datasets/MY-EXPERIMENT/00_raw", ...],
        "raw_symlink":  "/abs/path/to/raw" | null,
    }

    Raises
    ------
    ValueError       if preset is unknown
    FileExistsError  if flow already exists and force=False
    FileNotFoundError if raw path does not exist
    """
    import os
    from pyperun.core.pipeline import PIPELINE_STEPS
    from pyperun.core.runner import TREATMENTS_ROOT

    presets = {p["name"]: p for p in list_presets(project_dir)}
    if preset not in presets:
        raise ValueError(f"Unknown preset '{preset}'. Available: {', '.join(presets)}")

    allowed = presets[preset]["steps"]  # None = all steps
    base = Path(project_dir).resolve() if project_dir else Path.cwd()

    datasets_dir = base / "datasets" / dataset
    flows_dir    = base / "flows"
    treatments_dir = base / "treatments"

    steps = [s for s in PIPELINE_STEPS if allowed is None or s["treatment"] in allowed]

    _flow_name = flow_name or dataset.lower()
    flow_path  = flows_dir / f"{_flow_name}.json"
    flow_exists = flow_path.exists()

    if flow_exists and not force:
        raise FileExistsError(
            f"Flow '{_flow_name}.json' already exists. Use force=True to overwrite."
        )

    # Create pipeline stage directories (idempotent)
    created_dirs = []
    for s in steps:
        for key in ("input", "output"):
            if key in s and not s.get("external"):
                d = datasets_dir / s[key]
                d.mkdir(parents=True, exist_ok=True)
                rel = str(d.relative_to(base))
                if rel not in created_dirs:
                    created_dirs.append(rel)

    # 00_raw symlink
    raw_symlink = None
    if raw:
        raw_src = Path(raw).resolve()
        if not raw_src.exists():
            raise FileNotFoundError(f"--raw path does not exist: {raw_src}")
        raw_dir = datasets_dir / "00_raw"
        if raw_dir.exists() and not raw_dir.is_symlink():
            raw_dir.rmdir()
        if raw_dir.is_symlink():
            raw_dir.unlink()
        os.symlink(raw_src, raw_dir)
        raw_symlink = str(raw_src)

    # treatments/ placeholder
    treatments_dir.mkdir(parents=True, exist_ok=True)

    # Build flow JSON with all treatment defaults explicit
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

    flow_json = {
        "name": _flow_name,
        "description": f"Pipeline for dataset {dataset}",
        "dataset": dataset,
        "params": {},
        "steps": [_step_entry(s) for s in steps],
    }
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(json.dumps(flow_json, indent=4) + "\n")

    return {
        "dataset":      dataset,
        "flow":         _flow_name,
        "flow_path":    str(flow_path.relative_to(base)),
        "action":       "regenerated" if flow_exists else "created",
        "created_dirs": created_dirs,
        "raw_symlink":  raw_symlink,
    }


def delete_dataset(
    dataset: str,
    project_dir: str | None = None,
) -> dict:
    """Delete a dataset directory and all flow files that reference it.

    Parameters
    ----------
    dataset     : Dataset name (e.g. "MY-EXPERIMENT")
    project_dir : Project root directory (default: cwd)

    Returns
    -------
    {
        "deleted_dataset": "MY-EXPERIMENT",
        "deleted_dirs":    ["datasets/MY-EXPERIMENT"],
        "deleted_flows":   ["flows/my-experiment.json"],
        "raw_symlink_kept": "/abs/path" | null,
    }

    Raises
    ------
    FileNotFoundError if neither dataset directory nor flow files are found
    """
    import shutil
    from pyperun.core.flow import get_flows_root

    base = Path(project_dir).resolve() if project_dir else Path.cwd()
    dataset_dir = base / "datasets" / dataset
    flows_root  = get_flows_root()

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

    if not dataset_dir.exists() and not flow_files:
        raise FileNotFoundError(f"Nothing found for dataset '{dataset}'")

    deleted_dirs  = []
    deleted_flows = []
    raw_symlink_kept = None

    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            raw_symlink_kept = str(raw_dir.resolve())
            raw_dir.unlink()
        shutil.rmtree(dataset_dir)
        deleted_dirs.append(str(dataset_dir.relative_to(base)))

    for fp in flow_files:
        fp.unlink()
        deleted_flows.append(str(fp.relative_to(base)))

    return {
        "deleted_dataset":  dataset,
        "deleted_dirs":     deleted_dirs,
        "deleted_flows":    deleted_flows,
        "raw_symlink_kept": raw_symlink_kept,
    }
