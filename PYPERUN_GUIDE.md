# Pyperun — Guide for LLM Agents

## What is Pyperun?

Pyperun is a CLI pipeline tool for processing IoT time-series sensor data (valvometric/bio-signal data). It transforms raw CSV sensor files into ML-ready aggregated parquet files, CSVs, and PostgreSQL tables through a chain of declarative, composable **treatments**.

The core unit of work is a **flow**: a JSON file that sequences treatments, maps directories, and sets parameters.

---

## Data Model

### Raw Input Format

Files live in `datasets/<DATASET>/00_raw/`. Each file is a semicolon-delimited CSV with no header, where each row contains a UTC timestamp followed by `key:value` pairs:

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;m3:450;outdoor_temp:18.94
```

### Domains

Data is split into **domains** at parse time. Default domains:

| Domain | Columns | dtype |
|--------|---------|-------|
| `bio_signal` | `m0` to `m11` (columns with prefix `m`) | `int` |
| `environment` | `outdoor_temp` | `float` |

Domains are configurable in the `parse` treatment params.

### Parquet Naming Convention

```
<source>__<domain>__<YYYY-MM-DD>.parquet           # steps 1–4
<source>__<domain>__<YYYY-MM-DD>__<window>.parquet # step 5 (aggregate)
```

Where `<source>` = source device filename stem (e.g. `pil98`).

### Column Naming Convention (after aggregate)

Columns follow the pattern `<sensor>__<transform>__<metric>`, e.g.:
- `m0__raw__mean` — sensor m0, no transform, mean
- `m3__cbrt_inv__std` — sensor m3, cbrt_inv transform, std deviation
- `outdoor_temp__raw__mean` — environment signal, mean

---

## Directory Layout

```
datasets/<DATASET>/
  00_raw/           → raw CSV input files
  10_parsed/        → typed parquet per domain per day
  20_clean/         → cleaned parquet
  25_resampled/     → regular 1s grid parquet
  30_transform/     → columns with transforms applied
  35_normalized/    → (optional) normalized columns
  40_aggregated/    → multi-window aggregated parquet
  60_postgres/      → marker dir for postgres export (external)
  61_exportcsv/     → exported CSVs
  62_exportparquet/ → exported parquet files

flows/              → flow JSON files
datasets/           → one subdirectory per experiment
```

---

## CLI Reference

```bash
# Initialize a new dataset
pyperun init MY-EXPERIMENT

# Run a full flow
pyperun flow <flow-name>

# Run only one step
pyperun flow <flow-name> --step clean

# Run from a step to the end
pyperun flow <flow-name> --from-step resample

# Run from start to a step
pyperun flow <flow-name> --to-step aggregate

# Only process new data since last run
pyperun flow <flow-name> --last

# Filter by date range (overrides flow params)
pyperun flow <flow-name> --from 2026-01-01T00:00:00Z --to 2026-02-01T00:00:00Z

# Override any step param from CLI
pyperun flow <flow-name> --params '{"aggregation": "60s"}'

# Preview what would run without executing
pyperun flow <flow-name> --dry-run

# Show dataset status (which steps are up to date)
pyperun status

# List available flows, treatments, or steps in a flow
pyperun list flows
pyperun list treatments
pyperun list steps --flow <flow-name>
```

---

## Flow JSON Format

Flows live in `flows/<name>.json`. The `dataset` field makes all `input`/`output` paths relative to `datasets/<dataset>/`.

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
        {"treatment": "parse",     "input": "00_raw",        "output": "10_parsed"},
        {"treatment": "clean",     "input": "10_parsed",     "output": "20_clean"},
        {"treatment": "resample",  "input": "20_clean",      "output": "25_resampled"},
        {"treatment": "transform", "input": "25_resampled",  "output": "30_transform"},
        {"treatment": "aggregate", "input": "30_transform",  "output": "40_aggregated"},
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

### Params Priority (lowest → highest)

```
treatment.json defaults  →  flow.params  →  step.params  →  CLI --params
```

`from`/`to` in `flow.params` are used for time filtering — they are NOT passed to individual treatments.

### Named Steps (for duplicate treatments)

When the same treatment appears multiple times in a flow, add a `name` field to distinguish them:

```json
{"treatment": "exportcsv", "name": "exportcsv_10s", "input": "40_aggregated", "output": "61_exportcsv_10s", "params": {"aggregation": "10s", ...}},
{"treatment": "exportcsv", "name": "exportcsv_1s",  "input": "40_aggregated", "output": "62_exportcsv_1s",  "params": {"aggregation": "1s",  ...}}
```

Use `--step exportcsv_10s` to run only that step.

---

## Treatments Reference

### 1. `parse` — Raw CSV → Typed Parquet

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `format` | str | `"kv_csv"` | Input format (`kv_csv` = semicolon key:value) |
| `delimiter` | str | `";"` | Field delimiter |
| `tz` | str | `"UTC"` | Timestamp timezone in raw files (IANA) |
| `timestamp_column` | str | `"ts"` | Name of the timestamp column in output |
| `domains` | dict | `{bio_signal: {prefix:"m", dtype:"int"}, environment: {columns:["outdoor_temp"], dtype:"float"}}` | Domain definitions: use `prefix` to capture all columns with that prefix, or `columns` for explicit list |
| `file_name_substitute` | list | `[]` | List of `{"src": "...", "target": "..."}` to normalize raw filenames |

**Output columns**: `ts`, then one column per sensor key found in raw files.

---

### 2. `clean` — Remove Bad Data

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `drop_duplicates` | bool | `true` | Remove rows with duplicate timestamps |
| `domains` | dict | see below | Per-domain cleaning rules |

Per-domain keys: `min_value`, `max_value` (rows outside bounds are dropped), `spike_window` (rolling window size), `spike_threshold` (max deviation from rolling median to detect spikes).

Default domain config:
```json
{
    "bio_signal":   {"min_value": 0,    "max_value": 800, "spike_window": 10, "spike_threshold": 100},
    "environment":  {"min_value": -10.0,"max_value": 50.0,"spike_window": 7,  "spike_threshold": 5.0}
}
```

---

### 3. `resample` — Regular Time Grid

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `freq` | str | `"1s"` | Target frequency (pandas offset alias) |
| `max_gap_fill_s` | int | `20` | Max gap (seconds) to forward-fill; larger gaps become NaN |
| `agg_method` | dict | `{bio_signal:"nearest", environment:"mean"}` | Per-domain method when multiple raw samples land in same bin |

---

### 4. `transform` — Mathematical Transforms

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `transforms` | list | `[{function:"cbrt_inv", target:{domain:"bio_signal"}, mode:"add"}]` | List of transform specs |

Each transform spec:
- `function`: `cbrt_inv` (cube-root inverse), `sqrt_inv` (square-root inverse), `log`, `identity`
- `target`: `{"domain": "bio_signal"}` for all columns in a domain, or add `"columns": ["m0","m1"]` to restrict
- `mode`: `"add"` (new column `<col>__<function>`) or `"replace"` (overwrite)

---

### 5. `normalize` — Percentile Normalization to [0, 1]

**IMPORTANT — two-phase treatment.** `normalize` cannot run as a single step. It requires two distinct executions that must be two separate named steps in the flow:

1. **`fit=true`** — scans all input files, computes per-device percentile bounds, saves them to `normalize_params.json` in the output directory. Does NOT produce parquet output yet.
2. **`fit=false`** — loads `normalize_params.json` from the output directory and applies normalization to all input files, writing parquet output.

Both steps share the same `output` directory (e.g. `35_normalized`). The params file is saved there on fit, then read from there on apply.

**If `fit=false` runs without a prior `fit=true`, it crashes with `FileNotFoundError`.**

```json
{"treatment": "normalize", "name": "normalize_fit",   "input": "30_transform", "output": "35_normalized", "params": {"fit": true,  ...}},
{"treatment": "normalize", "name": "normalize_apply", "input": "30_transform", "output": "35_normalized", "params": {"fit": false, ...}}
```

For incremental runs (daily pipeline), only run `normalize_apply` (`--from-step normalize_apply`). Re-run `normalize_fit` only when the fitting data changes.

#### The normalization formula

```
normalized = (value - p2) / (p98 - p2)
```

With `clip: true`, output is clamped to `[0.0, 1.0]`. Outliers beyond the fit range are capped at 0 or 1.

The `normalize_params.json` stores one entry per device per column:
```json
{"pil98": {"m0__cbrt_inv": {"p2": 0.012, "p98": 0.087}}, ...}
```

#### The `columns` param controls in-place vs new columns

| `columns` value | Behaviour |
|-----------------|-----------|
| `[]` (empty list) | All numeric columns are normalized **in-place** (originals overwritten) |
| `["m0", "m1"]` | Only those columns, **in-place** |
| `{"*__cbrt_inv": "*__cbrt_inv__norm"}` | Wildcard pattern — creates **new columns**, originals kept (e.g. `m0__cbrt_inv` → `m0__cbrt_inv__norm`) |

Use the dict/wildcard form when you need both the raw transform values and the normalized values downstream.

#### Params that only matter during `fit=true`

`method`, `percentile_min`, `percentile_max`, `fit_window_days`, `min_range_warn` are read only during the fit phase. Passing them in the apply step has no effect.

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `fit` | bool | `false` | `true` = compute and save params. `false` = load and apply. |
| `method` | str | `"percentile"` | `"percentile"` (robust) or `"minmax"` (sensitive to outliers). Fit only. |
| `percentile_min` / `percentile_max` | float | `2.0` / `98.0` | Percentile bounds. Fit only. |
| `domain` | str | `"bio_signal"` | Domain to normalize |
| `columns` | dict or list | `[]` | Column selection — see table above |
| `clip` | bool | `true` | Clamp output to [0, 1] |
| `fit_window_days` | int | `0` | When fitting, use only last N days. `0` = all data. Fit only. |
| `min_range_warn` | float | `0.0` | Warn if fitted range < this value (0 = disabled). Fit only. |

---

### 6. `aggregate` — Multi-Window Aggregation

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `windows` | list | `["1s","10s","60s","5min","1h"]` | Time windows (pandas offset aliases) |
| `metrics` | list | `["mean","std"]` | Functions: `mean`, `std`, `min`, `max`, `median` |
| `decimals` | int | `-1` | Decimal places for rounding (-1 = no rounding) |

Produces one parquet file per domain per day **per window**, with columns like `m0__raw__mean`, `m0__cbrt_inv__std`, etc.

---

### 7. `exportcsv` — Export to CSV

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `aggregation` | str | `"10s"` | Window to export (must exist in aggregate output) |
| `domain` | str | `"bio_signal"` | Domain to export |
| `tz` | str | `"Europe/Paris"` | Output timezone for timestamp (IANA) |
| `from` | str | `null` | Start filter ISO 8601 (inclusive) |
| `to` | str | `null` | End filter ISO 8601 (exclusive) |
| `columns` | dict | see default | Map `source_column → {name, dtype}` — controls selection, order, renaming and casting |

Column spec formats:
```json
"m0__raw__mean": "c0"                          // rename only (shorthand)
"m0__raw__mean": {"name": "c0", "dtype": "int"} // rename + cast
```

Produces one CSV per source device. Columns appear in the order defined in `columns`.

---

### 8. `exportparquet` — Export to Parquet

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `aggregation` | str | `"10s"` | Window to export |
| `domain` | str | `"bio_signal"` | Domain to export |
| `columns` | dict | `{}` | Map `source_column → output_name`. `{}` = export all as-is |

Produces one parquet file per source device.

---

### 9. `to_postgres` — Export to PostgreSQL

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | str | `"localhost"` | Server hostname |
| `port` | int | `5432` | Server port |
| `dbname` | str | `""` | Database name |
| `user` | str | `""` | User |
| `password` | str | `""` | Password |
| `table_prefix` | str | `""` | Prefix prepended to all table names |
| `table_template` | str | `"{experience}__{step}__{aggregation}"` | Table name template. Variables: `{experience}`, `{step}` (domain), `{aggregation}` |
| `mode` | str | `"append"` | `"append"` (insert new rows only), `"replace"` (truncate data, keep schema), `"reset"` (drop table and recreate from scratch) |
| `aggregations` | list | `[]` | Windows to export. `[]` = all available |
| `sources` | list | `[{domain:"bio_signal"},{domain:"environment"}]` | Domains to export — see filters below |
| `from` / `to` | str | `null` | Date range filter for this step |

#### `sources` filters

Each entry in `sources` supports the following optional filters. Multiple entries with the same domain are allowed (for per-device column rules — first matching entry wins).

| Field | Description |
|-------|-------------|
| `domain` | Required. Domain name (`bio_signal`, `environment`, ...) |
| `devices` | List of device ids to include. Omit = all devices |
| `sensors` | List of sensor names or fnmatch patterns (e.g. `"m*"` matches m0…m11) |
| `transforms` | List of transform names or patterns (e.g. `"cbrt_inv"`, `"*"`) |
| `metrics` | List of metric names or patterns (e.g. `["mean", "std"]`) |
| `columns` | Explicit list of full column names (`<sensor>__<transform>__<metric>`). Takes priority over sensors/transforms/metrics |

`sensors`, `transforms`, `metrics` are combined as a cartesian product. A `null`/absent axis matches anything. Wildcards use [fnmatch](https://docs.python.org/3/library/fnmatch.html) syntax.

```json
"sources": [
    {
        "domain": "bio_signal",
        "devices": ["pil-78"],
        "sensors": ["m0", "m1"],
        "transforms": ["raw", "cbrt_inv"],
        "metrics": ["mean", "std"]
    },
    {
        "domain": "bio_signal",
        "devices": ["pil-79"],
        "sensors": ["m*"],
        "transforms": ["raw"],
        "metrics": ["mean"]
    }
]
```

#### `mode` details

| Mode | Effect |
|------|--------|
| `append` | Inserts only rows with `ts > MAX(ts)` already in the table |
| `replace` | `TRUNCATE` — clears all rows but keeps the table schema (columns unchanged) |
| `reset` | `DROP TABLE` — destroys and recreates the table; use when the column set changes |

Use `reset` from CLI without changing the flow: `pyperun flow my-flow --step to_postgres --params '{"mode": "reset"}'`

This step is marked **external** — it writes to PostgreSQL, not to disk. `pyperun status` cannot check whether it is up to date and excludes it from staleness checks.

---

## Common Patterns

### Minimal flow (parse → aggregate → csv)

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
                "domain": "bio_signal",
                "tz": "UTC",
                "columns": {
                    "m0__raw__mean": {"name": "c0", "dtype": "int"},
                    "m1__raw__mean": {"name": "c1", "dtype": "int"}
                }
            }
        }
    ]
}
```

### Multiple export steps (same treatment, different windows)

```json
{"treatment": "exportcsv", "name": "export_10s", "input": "40_aggregated", "output": "61_csv_10s", "params": {"aggregation": "10s", ...}},
{"treatment": "exportcsv", "name": "export_60s", "input": "40_aggregated", "output": "62_csv_60s", "params": {"aggregation": "60s", ...}}
```

### Custom domain (e.g. only 5 sensors)

In the `parse` step params:
```json
"domains": {
    "bio_signal": {"prefix": "m", "dtype": "int"}
}
```
(Omit `environment` if there are no environment columns in raw files.)

### Normalize with fit

```json
{"treatment": "normalize", "name": "normalize_fit",   "input": "30_transform", "output": "35_normalized", "params": {"fit": true,  "domain": "bio_signal", "columns": {"*__cbrt_inv": "*__cbrt_inv__norm"}}},
{"treatment": "normalize", "name": "normalize_apply", "input": "30_transform", "output": "35_normalized", "params": {"fit": false, "domain": "bio_signal", "columns": {"*__cbrt_inv": "*__cbrt_inv__norm"}}}
```

Run `--step normalize_fit` once, then use `normalize_apply` for incremental runs.

### Date-filtered run

```json
{
    "params": {
        "from": "2026-03-01T00:00:00Z",
        "to":   "2026-04-01T00:00:00Z"
    }
}
```

Or override at runtime: `pyperun flow my-flow --from 2026-03-01T00:00:00Z --to 2026-04-01T00:00:00Z`

---

## Key Rules and Constraints

1. **Each step reads from `input/` and writes to `output/`** — these dirs are resolved relative to `datasets/<dataset>/` when `dataset` is set.
2. **Parquet filenames encode metadata** — don't rename them manually; the pipeline relies on the naming convention to resolve files.
3. **`aggregate` must run before `exportcsv`/`exportparquet`/`to_postgres`** — they read `__<window>.parquet` files.
4. **`columns` in `exportcsv` controls output column order** — order of keys in the JSON object = column order in CSV.
5. **`to_postgres` is external** — it does not produce files; `pyperun status` won't flag it as stale.
6. **Duplicate treatment names require a `name` field** — `--step` and `--from-step` use this name as identifier.
7. **`normalize` needs a `normalize_params.json`** — run with `fit=true` first before incremental use.
