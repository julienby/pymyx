# Pyperun — LLM Reference Guide

> **For AI assistants working on or with this codebase.**
> This file is gitignored — local only. Keep it updated as the codebase evolves.

---

## What is Pyperun?

Pyperun is a CLI pipeline tool for processing IoT time-series sensor data (valvometric / bio-signal). It transforms raw sensor CSV files into aggregated parquet, CSV exports, and PostgreSQL tables through a chain of declarative, composable **treatments**.

The core unit of work is a **flow**: a JSON file that sequences treatments, maps directories, and merges parameters.

Pyperun also exposes a **Python API** (`pyperun.core.api`) so external tools (Flask, scripts, AI agents) can query state and trigger runs without subprocess or stdout parsing.

---

## Repository Layout

```
pyperun/                        ← Python package (the framework)
  cli.py                        ← pyperun CLI entry point (subcommands: flow, run, init, status, list, describe, ...)
  core/
    flow.py                     ← resolves paths, runs steps via runner, generates run_id
    runner.py                   ← loads a treatment, validates params, calls run()
    pipeline.py                 ← PIPELINE_STEPS registry (treatment → input/output dirs)
    validator.py                ← pydantic validation + param merging (defaults → flow → step → CLI)
    timefilter.py               ← time filtering, --last incremental logic
    filename.py                 ← parquet filename parse/build/list conventions
    logger.py                   ← jsonlines event log → logs/pyperun.log, new_run_id()
    api.py                      ← pure Python API: list_flows, get_status, list_runs, get_run_events, ...
  treatments/
    parse/                      ← treatment.json + run.py  (× all treatments)
    clean/
    resample/
    transform/
    normalize/
    aggregate/
    to_postgres/
    exportcsv/
    exportparquet/

flows/                          ← flow JSON files (user-defined)
datasets/                       ← data (gitignored)
  <DATASET>/
    00_raw/                     ← raw CSV input
    10_parsed/ 20_clean/ 25_resampled/ 30_transform/ 35_normalized/ 40_aggregated/
    60_postgres/  61_exportcsv/  62_exportparquet/
logs/                           ← pyperun.log (gitignored, auto-created)
scripts/
  hourly_sync.sh                ← cron wrapper for one flow (--last)
  run_scheduled_flows.sh        ← cron wrapper for all flows in scheduled_flows.txt
  run_flow_hourly.sh            ← loop runner (sleep N seconds between runs)
  update.sh                     ← git pull + pip install -e .
api_server.py                   ← Flask REST API server (optional, at project root)
tests/
```

---

## Data Model

### Raw input format

Files live in `datasets/<DATASET>/00_raw/`. Semicolon-delimited, no header, key:value pairs:

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;m3:450;outdoor_temp:18.94
```

### Domains

Data is split into **domains** at parse time. Defaults:

| Domain | Columns | dtype |
|--------|---------|-------|
| `bio_signal` | `m0`–`m11` (prefix `m`) | `int` |
| `environment` | `outdoor_temp` | `float` |

Domains are configurable in `parse` params. Omit a domain key to exclude it.

### Parquet naming convention

```
<source>__<domain>__<YYYY-MM-DD>.parquet           # parse → aggregate input
<source>__<domain>__<YYYY-MM-DD>__<window>.parquet # aggregate output
```

`<source>` = stem of the raw device filename (e.g. `pil98`, `DREISSENE-MERCURE__pil-78`).

### Column naming convention (post-aggregate)

```
<sensor>__<transform>__<metric>
```

Examples: `m0__raw__mean`, `m3__cbrt_inv__std`, `outdoor_temp__raw__mean`.

---

## CLI Reference

```bash
pyperun flow <name>                                # run all steps
pyperun flow <name> --step clean                   # single step
pyperun flow <name> --from-step resample           # from step to end
pyperun flow <name> --to-step aggregate            # from start to step
pyperun flow <name> --from-step clean --to-step aggregate  # range

pyperun flow <name> --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z
pyperun flow <name> --last                         # incremental: delta since last run
pyperun flow <name> --params '{"aggregation":"60s"}'  # runtime param override
pyperun flow <name> --output-mode replace          # overwrite output for the time range
pyperun flow <name> --output-mode full-replace     # wipe all outputs and reprocess
pyperun flow <name> --dry-run                      # preview without running

pyperun run <treatment> --input <dir> --output <dir> [--params '{}']
pyperun init MY-EXPERIMENT
pyperun status
pyperun status --format json                       # machine-readable
pyperun list flows | treatments | steps --flow <name>
pyperun list flows --format json                   # all list subcommands accept --format json
pyperun describe <treatment>
pyperun describe <treatment> --format json
pyperun export MY-EXPERIMENT [--full]
pyperun import archive.tar.gz
pyperun delete MY-EXPERIMENT [-y]
pyperun upgrade
```

### `--format json`

The commands `list`, `status`, and `describe` accept `--format json` to output valid JSON on stdout instead of human-formatted text. Useful for scripting, piping, and API wrappers. The `--format` flag has no effect on `flow`, `run`, `init`, or other write commands.

---

## Flow Format

```json
{
    "name": "my-flow",
    "description": "...",
    "dataset": "MY-DATASET",
    "params": {
        "from": "2026-01-01T00:00:00Z",
        "to":   "2026-02-01T00:00:00Z"
    },
    "steps": [
        {"treatment": "parse",     "input": "00_raw",       "output": "10_parsed"},
        {"treatment": "clean",     "input": "10_parsed",    "output": "20_clean"},
        {"treatment": "resample",  "input": "20_clean",     "output": "25_resampled"},
        {"treatment": "transform", "input": "25_resampled", "output": "30_transform"},
        {"treatment": "aggregate", "input": "30_transform", "output": "40_aggregated"},
        {
            "treatment": "exportcsv",
            "input": "40_aggregated",
            "output": "61_exportcsv",
            "params": {
                "aggregation": "10s",
                "domain": "bio_signal",
                "tz": "Europe/Paris",
                "columns": {
                    "m0__raw__mean": {"name": "c0", "dtype": "int"},
                    "m1__raw__mean": {"name": "c1", "dtype": "int"}
                }
            }
        }
    ]
}
```

### Params priority (lowest → highest)

```
treatment.json defaults  →  flow.params  →  step.params  →  CLI --params / --from / --to
```

`from`/`to` in `flow.params` are extracted for time filtering — they are **not** passed to individual treatments as params.

### Named steps (required for duplicate treatments)

```json
{"treatment": "exportcsv", "name": "exportcsv_10s", "input": "40_aggregated", "output": "61_csv_10s", "params": {"aggregation": "10s"}},
{"treatment": "exportcsv", "name": "exportcsv_60s", "input": "40_aggregated", "output": "62_csv_60s", "params": {"aggregation": "60s"}}
```

Use `--step exportcsv_10s` or `--from-step exportcsv_10s` to target a specific named step.

---

## Python API — `pyperun.core.api`

All functions return plain dicts/lists. They never print or have side effects. Designed to be imported directly by Flask or other tools — no subprocess needed.

```python
from pyperun.core.api import (
    # Discovery
    list_flows,
    list_steps,
    list_treatments,
    describe_treatment,
    list_presets,
    # State
    get_status,
    # Dataset lifecycle
    init_dataset,
    delete_dataset,
    # Run history
    list_runs,
    get_run_events,
)
```

### `list_flows() → list[dict]`

Returns all available flows.

```python
[
    {"name": "my-flow", "description": "...", "dataset": "MY-DATASET", "n_steps": 6},
    ...
]
```

### `list_steps(flow_name: str) → list[dict]`

Returns the steps of a flow. Raises `FileNotFoundError` if the flow does not exist.

```python
[
    {"index": 1, "treatment": "parse",  "name": "parse",  "input": "...", "output": "...", "params": {}},
    {"index": 2, "treatment": "clean",  "name": "clean",  "input": "...", "output": "...", "params": {}},
    ...
]
```

### `list_treatments() → list[dict]`

```python
[{"name": "parse", "description": "..."}, ...]
```

### `describe_treatment(name: str) → dict`

Raises `FileNotFoundError` if the treatment does not exist.

```python
{
    "name": "aggregate",
    "description": "...",
    "input_format": "...",
    "output_format": "...",
    "params": [
        {"name": "windows", "type": "list", "default": ["10s","60s","5min","1h"], "description": "..."},
        ...
    ]
}
```

### `get_status() → list[dict]`

Returns pipeline state for all flows. `status` is `"up-to-date"`, `"incomplete"`, or `"no-dataset"`.

```python
[
    {
        "flow": "my-flow",
        "dataset": "MY-DATASET",
        "status": "up-to-date",
        "steps": [
            {"treatment": "parse", "output": "10_parsed", "n_files": 84, "last_modified": "2026-02-17", "external": false},
            ...
        ]
    }
]
```

### `list_runs(limit: int = 50) → list[dict]`

Returns recent runs from `logs/pyperun.log`, sorted most recent first. Returns `[]` if the log does not exist.

```python
[
    {
        "run_id": "a3f9b2c1",
        "flow": "my-flow",
        "started_at": "2026-04-08T10:00:00Z",
        "finished_at": "2026-04-08T10:02:34Z",
        "status": "success",    # "running" | "success" | "error"
        "n_steps_done": 6,
        "error": null
    },
    ...
]
```

### `get_run_events(run_id: str) → list[dict]`

Returns all log events for a specific run. Returns `[]` if not found.

```python
[
    {"ts": "2026-04-08T10:00:00Z", "run_id": "a3f9b2c1", "treatment": "parse",  "status": "start",   "flow": "my-flow", ...},
    {"ts": "2026-04-08T10:00:01Z", "run_id": "a3f9b2c1", "treatment": "parse",  "status": "success", "duration_ms": 1240.0},
    {"ts": "2026-04-08T10:00:01Z", "run_id": "a3f9b2c1", "treatment": "clean",  "status": "start",   ...},
    ...
]
```

### `list_presets(project_dir: str | None = None) → list[dict]`

Returns built-in presets merged with project-level `presets.json` (project wins on conflicts).

```python
[
    {"name": "csv",     "description": "Core pipeline → exportcsv",     "steps": ["parse", "clean", ...]},
    {"name": "parquet", "description": "Core pipeline → exportparquet",  "steps": ["parse", "clean", ...]},
    {"name": "full",    "description": "Full pipeline (all steps)",      "steps": None},  # None = all PIPELINE_STEPS
]
```

### `init_dataset(dataset, preset="full", flow_name=None, raw=None, force=False, project_dir=None) → dict`

Scaffolds a new dataset: creates stage directories and generates a flow JSON with all treatment defaults explicit.

```python
{
    "dataset":      "MY-EXPERIMENT",
    "flow":         "my-experiment",
    "flow_path":    "flows/my-experiment.json",
    "action":       "created",        # or "regenerated" if flow existed and force=True
    "created_dirs": ["datasets/MY-EXPERIMENT/00_raw", "datasets/MY-EXPERIMENT/10_parsed", ...],
    "raw_symlink":  "/abs/path/to/raw" or None,
}
```

Raises:
- `ValueError` — unknown preset
- `FileExistsError` — flow already exists and `force=False`
- `FileNotFoundError` — `raw` path does not exist

No interactive prompt — `force=True` overwrites without asking (unlike the CLI which prompts).

### `delete_dataset(dataset, project_dir=None) → dict`

Deletes the dataset directory and all flow files that reference it.

```python
{
    "deleted_dataset":  "MY-EXPERIMENT",
    "deleted_dirs":     ["datasets/MY-EXPERIMENT"],
    "deleted_flows":    ["flows/my-experiment.json"],
    "raw_symlink_kept": "/abs/path" or None,   # raw source is never deleted
}
```

Raises `FileNotFoundError` if neither the dataset directory nor any flow referencing it is found.

No interactive prompt — always deletes immediately (unlike the CLI which prompts unless `-y`).

---

## Flask REST API — `api_server.py`

`api_server.py` (project root) is a standalone Flask server that wraps `pyperun.core.api` and `run_flow`. It must run on the **same server** as pyperun (same Python environment) — it imports pyperun directly, no subprocess.

```bash
pip install flask
flask --app api_server run --host 0.0.0.0 --port 5000

# Production (1 worker only — runs are threads, not processes):
gunicorn -w 1 -b 0.0.0.0:5000 api_server:app

# With authentication:
export PYPERUN_API_KEY=my-secret-key
flask --app api_server run ...
# All requests must carry: Authorization: Bearer my-secret-key
```

### Endpoints

| Method | Endpoint | Wraps | Returns |
|--------|----------|-------|---------|
| `GET` | `/health` | — | `{"status": "ok"}` |
| `GET` | `/api/flows` | `list_flows()` | list of flows |
| `GET` | `/api/flows/<flow>/steps` | `list_steps(flow)` | steps — passwords masked |
| `GET` | `/api/treatments` | `list_treatments()` | list of treatments |
| `GET` | `/api/treatments/<name>` | `describe_treatment(name)` | treatment detail |
| `GET` | `/api/presets` | `list_presets()` | list of presets |
| `GET` | `/api/status` | `get_status()` | pipeline state |
| `POST` | `/api/datasets` | `init_dataset(...)` | `{dataset, flow, action, created_dirs}` — 201 |
| `DELETE` | `/api/datasets/<dataset>` | `delete_dataset(...)` | `{deleted_dirs, deleted_flows}` — 200 |
| `POST` | `/api/run/<flow>` | `run_flow(...)` in thread | `{"run_id": "...", "status": "started"}` — 202 |
| `GET` | `/api/runs?limit=50` | `list_runs(limit)` | run history |
| `GET` | `/api/runs/<run_id>` | `get_run_events(run_id)` | `{run_id, status, n_steps_total, n_steps_done, events}` |

### POST `/api/datasets` — body

```json
{
    "dataset":   "MY-EXPERIMENT",
    "preset":    "full",
    "flow_name": null,
    "raw":       null,
    "force":     false
}
```

Returns 201 on success, 400 on bad input, 409 if flow exists and `force=false`.

### DELETE `/api/datasets/<dataset>`

No body. Returns 404 if dataset not found.

### POST `/api/run/<flow>` — optional body

```json
{
    "from":        "2026-01-01T00:00:00Z",
    "to":          "2026-04-01T00:00:00Z",
    "last":        false,
    "step":        null,
    "from_step":   null,
    "to_step":     null,
    "output_mode": "append"
}
```

`last` and `from`/`to` are mutually exclusive. `step` and `from_step`/`to_step` are mutually exclusive.

### GET `/api/runs/<run_id>` — polling response

```json
{
    "run_id": "a3f9b2c1",
    "flow": "my-flow",
    "status": "running",
    "n_steps_total": 6,
    "n_steps_done": 3,
    "events": [
        {"ts": "...", "treatment": "parse",  "status": "start"},
        {"ts": "...", "treatment": "parse",  "status": "success", "duration_ms": 1240.0},
        ...
    ]
}
```

Poll every 2s. Stop when `status` is `"success"` or `"error"`.

---

## Core Module Behaviours

### `logger.py`

- `new_run_id() → str` — generates an 8-char hex ID (`os.urandom(4).hex()`).
- `log_event(treatment, status, input_dir, output_dir, ..., run_id=None)` — appends a jsonlines entry to `logs/pyperun.log`. Accepts `run_id` to group all events of a flow run. `password` keys are redacted to `***` in logged params.

Each event in the log:

```json
{
    "ts": "2026-04-08T10:00:00Z",
    "treatment": "parse",
    "status": "start",
    "input_dir": "datasets/MY-DATASET/00_raw",
    "output_dir": "datasets/MY-DATASET/10_parsed",
    "run_id": "a3f9b2c1",
    "flow": "my-flow",
    "params": {...},
    "duration_ms": 1240.0,
    "error": null
}
```

`status` values: `start`, `success`, `error`, `skip`.

### `flow.py` — `run_flow()`

```python
def run_flow(
    name: str,
    time_from=None, time_to=None,
    output_mode: str = "append",
    last: bool = False,
    from_step=None, to_step=None, step=None,
    dry_run: bool = False,
    run_id: str | None = None,   # ← pass externally for tracking; auto-generated if None
) -> str:                        # ← returns the run_id
```

- Generates a `run_id` at startup if none provided, propagates it to all `run_treatment()` calls.
- Prints `run_id` at start and end: `[flow] Starting 'name' (N steps)  run_id=a3f9b2c1`.
- Returns the `run_id` string — useful for Flask to return it to the client before the run finishes.

### `runner.py` — `run_treatment()`

```python
def run_treatment(
    name, input_dir, output_dir,
    params=None, time_from=None, time_to=None,
    output_mode="append",
    flow=None,
    run_id=None,   # ← propagated from run_flow
) -> None:
```

Logs `start`, then `success` or `error`. All log entries carry `run_id` when provided.

### `timefilter.py` — incremental logic (`--last`)

`resolve_last_range(input_dir, output_dir)` returns `(time_from, time_to)`:

- **Output empty** → `(None, None)` — process everything.
- **Already up-to-date** → raises `ValueError("already up-to-date")` — flow exits cleanly.
- **Delta exists** → `time_from = floor(last_output_ts, hour)`, `time_to = last_input_ts`. Minimum window: 1 hour.

**CSV-only input** (e.g. `parse` step, `00_raw`): uses the **mtime** of the most recent input file as `last_input`, not the filename date. This detects intra-day CSV updates (new data appended same day, mtime newer than last output data timestamp).

### `pipeline.py` — PIPELINE_STEPS registry

Defines the canonical treatment → directory mapping and `external: True` marker for steps that don't produce files (like `to_postgres`). Steps not in the registry can still be used in flows — the registry is only used by `pyperun status` and `get_status()`.

### `validator.py` — param merging

Merges params in priority order: treatment defaults → flow params → step params → CLI. `from`/`to` are extracted at flow level and used for time filtering; they are stripped before being passed to treatments.

---

## Treatments Reference

### `parse` — Raw CSV → Typed Parquet

| Param | Default | Description |
|-------|---------|-------------|
| `delimiter` | `";"` | Field delimiter |
| `tz` | `"UTC"` | Timezone of raw timestamps (IANA) |
| `timestamp_column` | `"ts"` | Name of timestamp column in output |
| `domains` | bio_signal + environment | Domain definitions: `prefix` (all matching columns) or `columns` (explicit list) + `dtype` |
| `file_name_substitute` | `[]` | `[{"src":"...","target":"..."}]` — normalise raw filenames for source extraction |

---

### `clean` — Remove Bad Data

| Param | Default | Description |
|-------|---------|-------------|
| `drop_duplicates` | `true` | Remove rows with duplicate timestamps |
| `domains` | see below | Per-domain: `min_value`, `max_value`, `spike_window`, `spike_threshold` |

Default domain config:
```json
{
    "bio_signal":  {"min_value": 0,    "max_value": 800, "spike_window": 10, "spike_threshold": 100},
    "environment": {"min_value": -10.0,"max_value": 50.0,"spike_window": 7,  "spike_threshold": 5.0}
}
```

---

### `resample` — Regular Time Grid

| Param | Default | Description |
|-------|---------|-------------|
| `freq` | `"1s"` | Target frequency (pandas offset alias) |
| `max_gap_fill_s` | `20` | Max gap (seconds) to forward-fill; larger gaps → NaN |
| `agg_method` | `{bio_signal:"nearest", environment:"mean"}` | Per-domain method when multiple samples land in the same bin |

---

### `transform` — Column Transforms

| Param | Default | Description |
|-------|---------|-------------|
| `transforms` | `[{function:"cbrt_inv", target:{domain:"bio_signal"}, mode:"add"}]` | List of transform specs |

Each spec:
- `function`: `cbrt_inv` (x^-1/3), `sqrt_inv` (x^-1/2), `log` (ln), `identity`
- `target`: `{"domain":"bio_signal"}` or restrict with `"columns":["m0","m1"]`
- `mode`: `"add"` (new column `<col>__<function>`) or `"replace"` (overwrite in place)

---

### `normalize` — Percentile Normalization to [0, 1]

**Two-phase treatment** — requires two named steps in the flow:

1. **`fit=true`** — scans all input files, computes per-device percentile bounds, writes `normalize_params.json` to `output/`. No parquet written yet.
2. **`fit=false`** — reads `normalize_params.json` from `output/`, applies normalization to all input files, writes parquet.

```json
{"treatment":"normalize","name":"normalize_fit",   "input":"30_transform","output":"35_normalized","params":{"fit":true, "domain":"bio_signal","columns":{"*__cbrt_inv":"*__cbrt_inv__norm"}}},
{"treatment":"normalize","name":"normalize_apply", "input":"30_transform","output":"35_normalized","params":{"fit":false,"domain":"bio_signal","columns":{"*__cbrt_inv":"*__cbrt_inv__norm"}}}
```

> **Running `fit=false` without a prior `fit=true` crashes with `FileNotFoundError`.**
> For incremental runs, only run `normalize_apply`. Re-run `normalize_fit` only when the fitting data changes.

Formula: `normalized = (value - p2) / (p98 - p2)` clamped to [0,1] when `clip=true`.

| Param | Default | Description |
|-------|---------|-------------|
| `fit` | `false` | `true` = compute + save, `false` = load + apply |
| `method` | `"percentile"` | `"percentile"` or `"minmax"` (fit only) |
| `percentile_min/max` | `2.0` / `98.0` | Bounds (fit only) |
| `domain` | `"bio_signal"` | Domain to normalize |
| `columns` | `[]` | Column selection: `[]`=all in-place, list=selected in-place, dict=rename/add |
| `clip` | `true` | Clamp output to [0,1] |
| `fit_window_days` | `0` | Use last N days for fitting (0 = all data, fit only) |

---

### `aggregate` — Multi-Window Aggregation

| Param | Default | Description |
|-------|---------|-------------|
| `windows` | `["1s","10s","60s","5min","1h"]` | Time windows (pandas offset aliases) |
| `metrics` | `["mean","std"]` | `mean`, `std`, `min`, `max`, `median` |
| `decimals` | `-1` | Rounding (-1 = none) |

Produces one parquet file per domain × day × window. Column names: `<sensor>__<transform>__<metric>`.

---

### `to_postgres` — Export to PostgreSQL

| Param | Default | Description |
|-------|---------|-------------|
| `host` | `"localhost"` | Server hostname |
| `port` | `5432` | Port |
| `dbname` | `""` | Database |
| `user` / `password` | `""` | Credentials (`password` is redacted in logs and API responses) |
| `table_prefix` | `""` | Prefix prepended to all table names |
| `table_template` | `"{experience}__{step}__{aggregation}"` | Variables: `{experience}`, `{step}` (domain), `{aggregation}` |
| `mode` | `"append"` | `append`, `replace`, `reset` — see below |
| `aggregations` | `[]` | Windows to export (`[]` = all) |
| `sources` | all domains | Domain/column filter list |
| `from` / `to` | `null` | Date range filter for this step only |

#### mode details

| Mode | Effect |
|------|--------|
| `append` | Inserts only rows with `ts > MAX(ts)` in the table |
| `replace` | `TRUNCATE` — clears rows, keeps schema |
| `reset` | `DROP TABLE` — destroys and recreates (use when columns change) |

This step is marked **external** in `pipeline.py` — it writes to PostgreSQL, not disk. `pyperun status` and `get_status()` exclude it from staleness checks.

---

### `exportcsv` — Export to CSV

| Param | Default | Description |
|-------|---------|-------------|
| `aggregation` | `"10s"` | Window to export (must exist in aggregate output) |
| `domain` | `"bio_signal"` | Domain |
| `tz` | `"Europe/Paris"` | Output timezone (IANA) |
| `from` / `to` | `null` | Optional date range |
| `columns` | m0–m11 as int | `source_col → export_name` or `{"name":"...","dtype":"int","decimals":N}` |

Column spec:
```json
"m0__raw__mean": "c0"                                    // rename only
"m0__raw__mean": {"name": "c0", "dtype": "int"}          // rename + cast to int
"outdoor_temp__raw__mean": {"name": "T", "decimals": 2}  // rename + round
```

Output: one tab-separated CSV per source device. Column order = key order in `columns`.

---

### `exportparquet` — Export to Parquet

| Param | Default | Description |
|-------|---------|-------------|
| `aggregation` | `"10s"` | Window to export |
| `domain` | `"bio_signal"` | Domain |
| `columns` | `{}` | `source_col → output_name` (`{}` = export all as-is) |

Output: one parquet file per source device.

---

## Common Patterns

### Minimal flow (parse → aggregate → CSV)

```json
{
    "name": "minimal",
    "dataset": "MY-DATASET",
    "steps": [
        {"treatment": "parse",     "input": "00_raw",       "output": "10_parsed"},
        {"treatment": "clean",     "input": "10_parsed",    "output": "20_clean"},
        {"treatment": "resample",  "input": "20_clean",     "output": "25_resampled"},
        {"treatment": "transform", "input": "25_resampled", "output": "30_transform"},
        {"treatment": "aggregate", "input": "30_transform", "output": "40_aggregated"},
        {
            "treatment": "exportcsv",
            "input": "40_aggregated",
            "output": "61_exportcsv",
            "params": {
                "aggregation": "10s",
                "columns": {"m0__raw__mean": "c0", "m1__raw__mean": "c1"}
            }
        }
    ]
}
```

### Multiple export steps

```json
{"treatment":"exportcsv","name":"export_10s","input":"40_aggregated","output":"61_csv_10s","params":{"aggregation":"10s",...}},
{"treatment":"exportcsv","name":"export_60s","input":"40_aggregated","output":"62_csv_60s","params":{"aggregation":"60s",...}}
```

### Custom domain (fewer sensors)

```json
"params": {
    "domains": {
        "bio_signal": {"prefix": "m", "dtype": "int"}
    }
}
```

Omit `environment` if not present in raw files.

### Normalize with fit

```json
{"treatment":"normalize","name":"normalize_fit",   "input":"30_transform","output":"35_normalized","params":{"fit":true, "columns":{"*__cbrt_inv":"*__cbrt_inv__norm"}}},
{"treatment":"normalize","name":"normalize_apply", "input":"30_transform","output":"35_normalized","params":{"fit":false,"columns":{"*__cbrt_inv":"*__cbrt_inv__norm"}}}
```

Run `--step normalize_fit` once on the full dataset, then use `normalize_apply` for daily incremental runs.

### Date-filtered run

```json
{"params": {"from": "2026-03-01T00:00:00Z", "to": "2026-04-01T00:00:00Z"}}
```

Or at runtime: `pyperun flow my-flow --from 2026-03-01T00:00:00Z --to 2026-04-01T00:00:00Z`

### Incremental cron

```bash
# Single flow
0 * * * * /path/to/pyperun/scripts/hourly_sync.sh my-flow >> /var/log/pyperun.log 2>&1

# Multiple flows (scripts/scheduled_flows.txt lists one flow name per line)
0 * * * * /path/to/pyperun/scripts/run_scheduled_flows.sh
```

### Query from Python (e.g. in a Flask route)

```python
from pyperun.core.api import (
    get_status, list_flows, list_presets,
    init_dataset, delete_dataset,
    list_runs, get_run_events,
)
from pyperun.core.flow import run_flow
from pyperun.core.logger import new_run_id

# Discovery
flows   = list_flows()
presets = list_presets()

# Dataset lifecycle
result = init_dataset("MY-EXP", preset="csv")
result = delete_dataset("MY-EXP")

# State
status = get_status()
runs   = list_runs(limit=20)
events = get_run_events("a3f9b2c1")

# Launch a flow and get its run_id back immediately
run_id = new_run_id()
Thread(target=run_flow, kwargs={"name": "my-flow", "last": True, "run_id": run_id}).start()
# then poll get_run_events(run_id) to track progress
```

---

## Key Rules and Constraints

1. **Steps read from `input/` and write to `output/`** — both resolved relative to `datasets/<dataset>/` when `dataset` is set.
2. **Parquet filenames encode metadata** — do not rename manually; the pipeline relies on the naming convention.
3. **`aggregate` must run before** `exportcsv`, `exportparquet`, `to_postgres` — they consume `__<window>.parquet` files.
4. **Column order in `exportcsv`** follows the key order of the `columns` dict.
5. **`to_postgres` is external** — no disk output; `pyperun status` and `get_status()` cannot check staleness for it.
6. **Duplicate treatment names require a `name` field** — used by `--step` and `--from-step`.
7. **`normalize` needs `normalize_params.json`** — run `fit=true` before any incremental use.
8. **`--last` uses mtime for CSV-only inputs** — reliable for intra-day updates; for parquet inputs it reads actual data timestamps.
9. **Logs go to `logs/pyperun.log`** — jsonlines, auto-created. Each event carries `run_id` when launched via `run_flow()`.
10. **`password` is always redacted** — in `log_event()` params and in `/api/flows/<flow>/steps` API responses.
11. **`api_server.py` must run with 1 gunicorn worker** — runs are threads inside the process; multiple workers would each have their own thread pool and could run the same flow concurrently.

---

## Adding a New Treatment

1. Scaffold: `pyperun new my_treatment` — creates `treatments/my_treatment/treatment.json` + `run.py`.

2. Edit `treatment.json`:

```json
{
    "name": "my_treatment",
    "description": "...",
    "input_format": "Parquet files: <source>__<domain>__<YYYY-MM-DD>.parquet",
    "output_format": "...",
    "params": {
        "my_param": {"type": "str", "default": "value", "description": "..."}
    }
}
```

3. Implement `run.py`:

```python
def run(input_dir: str, output_dir: str, params: dict) -> None:
    from pathlib import Path
    # read from input_dir, write to output_dir
    ...
```

4. Optionally register in `pyperun/core/pipeline.py` (PIPELINE_STEPS) for `pyperun status` support.

5. Add tests in `tests/test_<name>.py`.

---

## Testing Conventions

- Tests live in `tests/`, one file per treatment or module.
- Use `tmp_path` (pytest fixture) for temporary directories.
- Import `LOG_PATH` from `pyperun.core.logger` and patch it around tests that trigger runs (use `monkeypatch.setattr`).
- `monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", ...)` to point at test-local treatments.
- The `_ctypes` / pandas error seen in this environment is a pyenv build issue (missing libffi) — unrelated to pyperun. Tests that don't import pandas run fine.
