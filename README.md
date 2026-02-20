# PyMyx

Minimal IoT time-series data processing pipeline for valvometric data.

Raw sensor CSV files (key:value format) go through a 7-step pipeline to produce aggregated parquet files, PostgreSQL exports, and CSV exports.

```
CSV bruts  -->  parse --> clean --> resample --> transform --> aggregate --> to_postgres
                                                                        --> exportnour (CSV)
```

PyMyx is designed as a **framework**: you install it once and use it as a black box. Your experiment lives in a separate project directory containing only your flows, params, datasets, and optional custom treatments.

## Installation

```bash
git clone <url-du-repo> ~/pymyx
cd ~/pymyx

# Installer les dependances et la commande pymyx
pip install -e .

# Avec les outils de dev (pytest, ruff)
pip install -e ".[dev]"
```

> **setuptools trop ancien ?** Si `pip install -e .` echoue avec `build_editable hook missing` :
> `pip install --user --upgrade pip setuptools` puis relancer.

> **`pymyx` not found ?** Ajouter `~/.local/bin` au PATH :
> ```bash
> echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
> source ~/.bashrc
> ```

**Important** : toutes les commandes `pymyx` doivent etre lancees depuis le repertoire du projet (celui qui contient `flows/` et `datasets/`). Le mode editable (`-e`) est recommande pour le dev comme pour la prod.

Verify:

```bash
pymyx --help
pymyx list flows
pymyx list treatments
```

## Quick start

### 1. Initialize a dataset

```bash
pymyx init MON-EXPERIENCE
```

This creates:
- `datasets/MON-EXPERIENCE/00_raw/` — directory for raw CSV files
- `flows/mon-experience.json` — flow template with all 7 steps

### 2. Add raw data

```bash
cp /path/to/csvs/*.csv datasets/MON-EXPERIENCE/00_raw/
```

Expected CSV format (no header, semicolon-delimited, key:value pairs):

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94
2026-01-20T09:07:59.142308Z;m0:11;m1:13;outdoor_temp:18.95
```

### 3. Run the pipeline

```bash
pymyx flow mon-experience
```

### 4. Check status

```bash
pymyx status
```

```
mon-experience (MON-EXPERIENCE)
  parse          10_parsed            84 files   last: 2026-02-17
  clean          20_clean             84 files   last: 2026-02-17
  ...
  -> up-to-date
```

## CLI reference

### `pymyx flow <name>`

Run a full pipeline (all steps sequentially).

```bash
# Run the full pipeline
pymyx flow valvometry_daily

# Run a single step
pymyx flow valvometry_daily --step clean

# Run from a step to the end
pymyx flow valvometry_daily --from-step resample

# Run a range of steps
pymyx flow valvometry_daily --from-step clean --to-step aggregate

# Time filtering (ISO 8601)
pymyx flow valvometry_daily --from 2026-02-01 --to 2026-02-10

# Incremental mode (only process new data since last run)
pymyx flow valvometry_daily --last

# Replace output for the time range being processed
pymyx flow valvometry_daily --output-mode replace

# Wipe all output directories and reprocess from scratch
pymyx flow valvometry_daily --output-mode full-replace
```

### `pymyx run <treatment>`

Run a single treatment with explicit paths.

```bash
pymyx run parse --input datasets/PREMANIP-GRACE/00_raw --output datasets/PREMANIP-GRACE/10_parsed

# With custom params
pymyx run aggregate \
    --input datasets/PREMANIP-GRACE/30_transform \
    --output datasets/PREMANIP-GRACE/40_aggregated \
    --params '{"windows": ["30s", "5min"], "metrics": ["mean", "median"]}'
```

### `pymyx init <dataset>`

Scaffold a new dataset (creates directories + flow template).

```bash
pymyx init MY-EXPERIMENT
```

### `pymyx status`

Show the state of all datasets (file counts, last modification date).

```bash
pymyx status
```

### `pymyx list`

```bash
pymyx list flows        # List available flows
pymyx list treatments   # List available treatments
pymyx list steps --flow valvometry_daily  # List steps in a flow
```

## Custom treatments

PyMyx discovers treatments from two locations, **local takes priority over built-ins**:

1. `./treatments/<name>/` — your project (custom or overrides)
2. `pymyx/treatments/<name>/` — built-in treatments (fallback)

To add a custom treatment, create a directory in your project:

```
mon-projet/
  treatments/
    my_treatment/
      treatment.json   # param schema with defaults
      run.py           # def run(input_dir, output_dir, params)
  flows/
  datasets/
```

To override a built-in, create a treatment with the same name in `./treatments/`. To use only built-ins, simply omit the `treatments/` directory.

## Pipeline steps

| # | Treatment | Directory | Description |
|---|-----------|-----------|-------------|
| 1 | `parse` | 00_raw -> 10_parsed | Parse key:value CSV into typed parquet, split by domain and day |
| 2 | `clean` | 10_parsed -> 20_clean | Drop duplicates, enforce min/max bounds, remove spikes (rolling median) |
| 3 | `resample` | 20_clean -> 25_resampled | Regular 1s grid, floor to second, forward-fill small gaps (<=2s) |
| 4 | `transform` | 25_resampled -> 30_transform | Apply mathematical transformations (sqrt_inv, log) to selected columns |
| 5 | `aggregate` | 30_transform -> 40_aggregated | Multi-window aggregation (10s, 60s, 5min, 1h) with configurable metrics |
| 6 | `to_postgres` | 40_aggregated -> PostgreSQL | Export to PostgreSQL wide tables (for Grafana) |
| 7 | `exportnour` | 40_aggregated -> 61_exportnour | Export to CSV per device, with column renaming and timezone conversion |

## Flow format

Flows are JSON files in `flows/`. Each step declares its `input` and `output` explicitly — the data flow is readable like a recipe, without having to look at any code.

```json
{
    "name": "my-experiment",
    "description": "Full pipeline for MY-EXPERIMENT",
    "dataset": "MY-EXPERIMENT",
    "params": {
        "from": "2026-02-01T00:00:00Z"
    },
    "steps": [
        {"treatment": "parse",     "input": "00_raw",       "output": "10_parsed"},
        {"treatment": "clean",     "input": "10_parsed",    "output": "20_clean"},
        {"treatment": "resample",  "input": "20_clean",     "output": "25_resampled"},
        {"treatment": "transform", "input": "25_resampled", "output": "30_transform"},
        {"treatment": "aggregate", "input": "30_transform", "output": "40_aggregated"},
        {
            "treatment": "to_postgres",
            "input": "40_aggregated",
            "output": "60_postgres",
            "params": {
                "host": "my-server",
                "dbname": "mydb",
                "user": "myuser",
                "password": "mypass",
                "table_template": "MY_EXPERIMENT__AGGREGATED__{aggregation}"
            }
        },
        {
            "treatment": "exportnour",
            "input": "40_aggregated",
            "output": "61_exportnour",
            "params": {
                "columns": {
                    "m0__raw__mean": {"name": "c0", "dtype": "int"},
                    "m1__raw__mean": {"name": "c1", "dtype": "int"},
                    "outdoor_temp__raw__mean": "temperature"
                }
            }
        }
    ]
}
```

**Paths** (`input`/`output`) are relative to `datasets/<dataset>/` when `dataset` is set, or absolute otherwise.

**Params hierarchy** (lowest to highest priority):

| Level | Where | Wins over |
|-------|-------|-----------|
| `treatment.json` | default values per treatment | — |
| `flow.params` | inherited by all steps | treatment defaults |
| `step.params` | overrides for that step only | flow params |
| CLI `--params` / `--from` / `--to` | runtime override | everything |

`from`/`to` in `flow.params` set the default time range for all steps; CLI `--from`/`--to` override them.

## Configuration

Each treatment is configured via `pymyx/treatments/<name>/treatment.json` which declares typed params with defaults. Params can be overridden in the flow JSON or via `--params` on the CLI.

### parse

| Param | Default | Description |
|-------|---------|-------------|
| `delimiter` | `";"` | CSV delimiter |
| `tz` | `"UTC"` | Timezone of raw timestamps |
| `timestamp_column` | `"ts"` | Name of the timestamp column |
| `domains` | bio_signal + environment | Domain split: prefix-based or explicit columns, with dtype |
| `file_name_substitute` | `[]` | Filename substitutions for source extraction |

### clean

| Param | Default | Description |
|-------|---------|-------------|
| `drop_duplicates` | `true` | Remove duplicate timestamps |
| `domains` | per-domain config | `min_value`, `max_value`, `spike_window`, `spike_threshold` per domain |

### transform

| Param | Default | Description |
|-------|---------|-------------|
| `transforms` | `[]` | List of `{function, target, mode}` specs. Functions: `sqrt_inv`, `log`. Mode: `add` (new column) or `replace` |

### resample

| Param | Default | Description |
|-------|---------|-------------|
| `freq` | `"1s"` | Resample frequency |
| `max_gap_fill_s` | `2` | Max gap (seconds) to forward-fill |
| `agg_method` | per-domain | Aggregation method when flooring to `freq` |

### aggregate

| Param | Default | Description |
|-------|---------|-------------|
| `windows` | `["10s", "60s", "5min", "1h"]` | Time windows for aggregation |
| `metrics` | `["mean", "std", "min", "max"]` | Aggregation functions |

### to_postgres

| Param | Default | Description |
|-------|---------|-------------|
| `host` | `"localhost"` | PostgreSQL host |
| `port` | `5432` | PostgreSQL port |
| `dbname` | required | Database name |
| `user` | required | Database user |
| `password` | required | Database password |
| `table_template` | `"{source}__{domain}__{aggregation}"` | Table naming pattern |
| `table_prefix` | `""` | Prefix added to table names |
| `mode` | `"append"` | `append` or `replace` |

### exportnour

| Param | Default | Description |
|-------|---------|-------------|
| `aggregation` | `"10s"` | Which aggregation window to export |
| `domain` | `"bio_signal"` | Domain to export |
| `tz` | `"Europe/Paris"` | Output timezone |
| `from` / `to` | none | Date range filter (optional) |
| `columns` | m0-m11 as int | Dict mapping `source_column` -> `export_name` or `{"name": "...", "dtype": "int", "decimals": N}` (controls selection, renaming, order, integer casting, and optional float rounding) |

## Project structure

### PyMyx (framework repo)

```
pymyx/
  cli.py                    # CLI entry point (pymyx command)
  core/
    pipeline.py             # Pipeline registry (treatment -> directory mapping)
    flow.py                 # Flow executor (runs steps sequentially)
    runner.py               # Single treatment executor
    validator.py            # treatment.json validation + param merging
    logger.py               # jsonlines event logging
    timefilter.py           # Time filtering and incremental processing
    filename.py             # Parquet filename conventions
  treatments/               # Built-in treatments
    parse/                  # treatment.json + run.py
    clean/
    resample/
    transform/
    aggregate/
    to_postgres/
    exportnour/
tests/
scripts/
  hourly_sync.sh            # Cron script for incremental processing
```

### Your project repo

```
mon-projet/
  treatments/               # Optional: custom or overriding treatments
    my_treatment/
      treatment.json
      run.py
  flows/                    # Flow definitions (JSON)
  datasets/                 # Data (gitignored)
    <DATASET>/
      00_raw/               # Raw CSV input
      10_parsed/            # Parquet, split by domain + day
      20_clean/
      25_resampled/
      30_transform/
      40_aggregated/
      61_exportnour/        # CSV exports
```

## Production (cron)

For automatic incremental processing, add `scripts/hourly_sync.sh` to crontab:

```bash
crontab -e
0 * * * * /home/user/pymyx/scripts/hourly_sync.sh >> /var/log/pymyx_hourly.log 2>&1
```

The script uses `--last` to detect new data and only process the delta.

## Data conventions

- **Parquet naming**: `<source>__<domain>__<YYYY-MM-DD>.parquet`
- **Aggregated naming**: `<source>__<domain>__<YYYY-MM-DD>__<window>.parquet`
- **Domains**: `bio_signal` (m0-m11, Int64) and `environment` (outdoor_temp, Float64)
- **Logging**: all events go to `pymyx.log` (jsonlines, one event per line)

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run all tests
python -m pytest tests/ -v

# Run a single test
python -m pytest tests/test_runner.py::test_run_with_defaults -v

# Lint
ruff check .
```
