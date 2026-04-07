# Pyperun — LLM Reference Guide

> **For AI assistants working on or with this codebase.**
> This file is gitignored — local only. Keep it updated as the codebase evolves.

---

## What is Pyperun?

Pyperun is a CLI pipeline tool for processing IoT time-series sensor data (valvometric / bio-signal). It transforms raw sensor CSV files into aggregated parquet, CSV exports, and PostgreSQL tables through a chain of declarative, composable **treatments**.

The core unit of work is a **flow**: a JSON file that sequences treatments, maps directories, and merges parameters.

---

## Repository Layout

```
pyperun/                        ← Python package (the framework)
  cli.py                        ← pyperun CLI entry point (subcommands: flow, run, init, status, list)
  core/
    flow.py                     ← resolves paths, runs steps via runner
    runner.py                   ← loads a treatment, validates params, calls run()
    pipeline.py                 ← PIPELINE_STEPS registry (treatment → input/output dirs)
    validator.py                ← pydantic validation + param merging (defaults → flow → step → CLI)
    timefilter.py               ← time filtering, --last incremental logic
    filename.py                 ← parquet filename parse/build/list conventions
    logger.py                   ← jsonlines event log → logs/pyperun.log
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
logs/                           ← pyperun.log, per-flow logs (gitignored, auto-created)
scripts/
  hourly_sync.sh                ← cron wrapper for one flow (--last)
  run_scheduled_flows.sh        ← cron wrapper for all flows in scheduled_flows.txt
  run_flow_hourly.sh            ← loop runner (sleep N seconds between runs)
  update.sh                     ← git pull + pip install -e .
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
pyperun list flows | treatments | steps --flow <name>
```

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

`columns` behaviour:

| Value | Effect |
|-------|--------|
| `[]` | All numeric columns normalized in-place |
| `["m0","m1"]` | Those columns, in-place |
| `{"*__cbrt_inv":"*__cbrt_inv__norm"}` | Wildcard → new columns, originals kept |

| Param | Default | Description |
|-------|---------|-------------|
| `fit` | `false` | `true` = compute + save, `false` = load + apply |
| `method` | `"percentile"` | `"percentile"` or `"minmax"` (fit only) |
| `percentile_min/max` | `2.0` / `98.0` | Bounds (fit only) |
| `domain` | `"bio_signal"` | Domain to normalize |
| `columns` | `[]` | Column selection (see above) |
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
| `user` / `password` | `""` | Credentials |
| `table_prefix` | `""` | Prefix prepended to all table names |
| `table_template` | `"{experience}__{step}__{aggregation}"` | Variables: `{experience}`, `{step}` (domain), `{aggregation}` |
| `mode` | `"append"` | `append`, `replace`, `reset` — see below |
| `aggregations` | `[]` | Windows to export (`[]` = all) |
| `sources` | all domains | Domain/column filter list — see below |
| `from` / `to` | `null` | Date range filter for this step only |

#### mode details

| Mode | Effect |
|------|--------|
| `append` | Inserts only rows with `ts > MAX(ts)` in the table |
| `replace` | `TRUNCATE` — clears rows, keeps schema |
| `reset` | `DROP TABLE` — destroys and recreates (use when columns change) |

Force reset from CLI without editing the flow:
```bash
pyperun flow my-flow --step to_postgres --params '{"mode":"reset"}'
```

#### sources filter

Each entry in `sources` filters what gets exported for that domain. Multiple entries with the same domain are allowed (per-device rules — first matching entry wins).

| Field | Description |
|-------|-------------|
| `domain` | Required |
| `devices` | Device ids to include (omit = all) |
| `sensors` | Sensor names or fnmatch patterns (`"m*"` → m0…m11) |
| `transforms` | Transform names or patterns |
| `metrics` | Metric names or patterns |
| `columns` | Explicit full column names — takes priority over sensors/transforms/metrics |

`sensors`, `transforms`, `metrics` combine as a cartesian product. A `null`/absent axis = match all.

This step is marked **external** in `pipeline.py` — it writes to PostgreSQL, not disk. `pyperun status` excludes it from staleness checks.

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

## Core Module Behaviours

### `timefilter.py` — incremental logic (`--last`)

`resolve_last_range(input_dir, output_dir)` returns `(time_from, time_to)`:

- **Output empty** → `(None, None)` — process everything.
- **Already up-to-date** → raises `ValueError("already up-to-date")` — flow exits cleanly.
- **Delta exists** → `time_from = floor(last_output_ts, hour)`, `time_to = last_input_ts`. Minimum window: 1 hour.

**CSV-only input** (e.g. `parse` step, `00_raw`): uses the **mtime** of the most recent input file as `last_input`, not the filename date. This detects intra-day CSV updates (new data appended same day, mtime newer than last output data timestamp).

`compute_last_timestamp(dir)`:
- Parquet → reads actual timestamp column
- CSV / other → filename date at `23:59:59 UTC` (fallback)

### `logger.py` — event logging

All runs append a jsonlines event to `logs/pyperun.log`. The `logs/` directory is auto-created on first write. `password` fields are redacted to `***` in logged params.

### `pipeline.py` — PIPELINE_STEPS registry

Defines the canonical treatment → directory mapping and `external: True` marker for steps that don't produce files (like `to_postgres`). Steps not in the registry can still be used in flows — the registry is only used by `pyperun status`.

### `validator.py` — param merging

Merges params in priority order: treatment defaults → flow params → step params → CLI. `from`/`to` are extracted at flow level and used for time filtering; they are stripped before being passed to treatments.

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

### Incremental cron (one flow)

```bash
# scripts/hourly_sync.sh my-flow
0 * * * * /path/to/pyperun/scripts/hourly_sync.sh my-flow >> /var/log/pyperun.log 2>&1
```

### Incremental cron (multiple flows)

```
# scripts/scheduled_flows.txt
my-flow-streaming
my-flow-daily
```

```bash
0 * * * * /path/to/pyperun/scripts/run_scheduled_flows.sh
```

---

## Key Rules and Constraints

1. **Steps read from `input/` and write to `output/`** — both resolved relative to `datasets/<dataset>/` when `dataset` is set.
2. **Parquet filenames encode metadata** — do not rename manually; the pipeline relies on the naming convention.
3. **`aggregate` must run before** `exportcsv`, `exportparquet`, `to_postgres` — they consume `__<window>.parquet` files.
4. **Column order in `exportcsv`** follows the key order of the `columns` dict.
5. **`to_postgres` is external** — no disk output; `pyperun status` cannot check staleness for it.
6. **Duplicate treatment names require a `name` field** — used by `--step` and `--from-step`.
7. **`normalize` needs `normalize_params.json`** — run `fit=true` before any incremental use.
8. **`--last` uses mtime for CSV-only inputs** — reliable for intra-day updates; for parquet inputs it reads actual data timestamps.
9. **Logs go to `logs/`** — `logs/pyperun.log` (main), per-flow logs in `logs/<flow>.log` (scheduled runners). Directory is auto-created.

---

## Adding a New Treatment

1. Create `pyperun/treatments/<name>/treatment.json`:

```json
{
    "name": "my_treatment",
    "description": "...",
    "params": {
        "my_param": {"type": "str", "default": "value", "description": "..."}
    }
}
```

2. Create `pyperun/treatments/<name>/run.py`:

```python
def run(input_dir: str, output_dir: str, params: dict) -> None:
    from pathlib import Path
    # read from input_dir, write to output_dir
    ...
```

3. Optionally register it in `pyperun/core/pipeline.py` (PIPELINE_STEPS) for `pyperun status` support.

4. Add tests in `tests/test_<name>.py`.

---

## Testing Conventions

- Tests live in `tests/`, one file per treatment or module.
- Use `tmp_path` (pytest fixture) for temporary directories.
- Import `LOG_PATH` from `pyperun.core.logger` and clean it up around tests that trigger runs.
- `monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", ...)` to point at test-local treatments.
- Pre-existing failures in `test_parse.py::TestDtypeCoercion` and some `test_exportcsv.py` tests — unrelated to pipeline logic, tracked separately.
