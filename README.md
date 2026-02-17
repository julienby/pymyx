# PyMyx

Minimal IoT time-series data processing pipeline for valvometric data.

Raw sensor CSV files (key:value format) go through a 7-step pipeline to produce aggregated parquet files, PostgreSQL exports, and CSV exports.

```
CSV bruts  -->  parse --> clean --> transform --> resample --> aggregate --> to_postgres
                                                                       --> exportnour (CSV)
```

## Installation

```bash
git clone <url-du-repo> ~/pymyx
cd ~/pymyx
pip install -e ".[dev]"
```

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

# Replace output instead of appending
pymyx flow valvometry_daily --output-mode replace
```

### `pymyx run <treatment>`

Run a single treatment with explicit paths.

```bash
pymyx run parse --input datasets/PREMANIP-GRACE/00_raw --output datasets/PREMANIP-GRACE/10_parsed

# With custom params
pymyx run aggregate \
    --input datasets/PREMANIP-GRACE/30_resampled \
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

## Pipeline steps

| # | Treatment | Directory | Description |
|---|-----------|-----------|-------------|
| 1 | `parse` | 00_raw -> 10_parsed | Parse key:value CSV into typed parquet, split by domain and day |
| 2 | `clean` | 10_parsed -> 20_clean | Drop duplicates, enforce min/max bounds, remove spikes (rolling median) |
| 3 | `transform` | 20_clean -> 25_transform | Apply mathematical transformations (sqrt_inv, log) to selected columns |
| 4 | `resample` | 25_transform -> 30_resampled | Regular 1s grid, floor to second, forward-fill small gaps (<=2s) |
| 5 | `aggregate` | 30_resampled -> 40_aggregated | Multi-window aggregation (10s, 60s, 5min, 1h) with configurable metrics |
| 6 | `to_postgres` | 40_aggregated -> PostgreSQL | Export to PostgreSQL wide tables (for Grafana) |
| 7 | `exportnour` | 40_aggregated -> 61_exportnour | Export to CSV per device, with column renaming and timezone conversion |

## Flow format

Flows are JSON files in `flows/`. The simplified format uses `dataset` to auto-resolve paths:

```json
{
    "name": "my-experiment",
    "dataset": "MY-EXPERIMENT",
    "steps": [
        {"treatment": "parse"},
        {"treatment": "clean"},
        {"treatment": "transform"},
        {"treatment": "resample"},
        {"treatment": "aggregate"},
        {
            "treatment": "to_postgres",
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
            "params": {
                "columns": {
                    "m0__raw__mean": "m0",
                    "m1__raw__mean": "c1"
                }
            }
        }
    ]
}
```

Steps without `params` use defaults from `treatment.json`. Steps can also override `input`/`output` explicitly if needed.

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
| `columns` | required | Dict mapping `source_column` -> `export_name` (controls selection, renaming, order) |

## Project structure

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
  treatments/
    parse/                  # treatment.json + run.py
    clean/
    transform/
    resample/
    aggregate/
    to_postgres/
    exportnour/
flows/                      # Flow definitions (JSON)
datasets/                   # Data (gitignored)
  <DATASET>/
    00_raw/                 # Raw CSV input
    10_parsed/              # Parquet, split by domain + day
    20_clean/
    25_transform/
    30_resampled/
    40_aggregated/
    61_exportnour/          # CSV exports
tests/
scripts/
  hourly_sync.sh            # Cron script for incremental processing
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
