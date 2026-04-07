# Pyperun

> Minimal IoT time-series pipeline — from raw sensor CSV to aggregated parquet, PostgreSQL, and CSV exports.

```
  raw CSV
     │
     ▼
  ┌─────────┐   ┌───────┐   ┌──────────┐   ┌───────────┐   ┌───────────┐
  │  parse  │──▶│ clean │──▶│ resample │──▶│ transform │──▶│ aggregate │──┐
  └─────────┘   └───────┘   └──────────┘   └───────────┘   └───────────┘  │
                                                                            │
                                                              ┌─────────────┼──────────────┐
                                                              ▼             ▼              ▼
                                                         to_postgres   exportcsv   exportparquet
                                                         (Grafana)      (CSV)       (parquet)
```

Pyperun is a **framework**: install it once, then describe your experiment as a **flow** — a plain JSON file that sequences treatments, maps directories, and sets parameters. No code to write for standard pipelines.

---

## Installation

```bash
git clone https://github.com/julienby/pyperun ~/pyperun
cd ~/pyperun
pip install -e .
```

```bash
# With dev tools (pytest, ruff)
pip install -e ".[dev]"
```

> **`pyperun` not found?** Add `~/.local/bin` to your PATH:
> ```bash
> echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
> ```

> **setuptools too old?** Run `pip install --upgrade pip setuptools` then retry.

All `pyperun` commands must be run from your **project directory** (the one containing `flows/` and `datasets/`).

---

## Quick start

**1. Initialize a dataset**

```bash
pyperun init MY-EXPERIMENT
```

Creates `datasets/MY-EXPERIMENT/00_raw/` and a flow template at `flows/my-experiment.json`.

**2. Drop your raw CSV files in**

```bash
cp /path/to/data/*.csv datasets/MY-EXPERIMENT/00_raw/
```

Expected format — no header, semicolon-delimited, key:value pairs:

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94
2026-01-20T09:07:59.142308Z;m0:11;m1:13;outdoor_temp:18.95
```

**3. Run the pipeline**

```bash
pyperun flow my-experiment
```

**4. Check what's been processed**

```bash
pyperun status
```

```
my-experiment (MY-EXPERIMENT)
  parse       10_parsed      84 files   last: 2026-02-17
  clean       20_clean       84 files   last: 2026-02-17
  resample    25_resampled   84 files   last: 2026-02-17
  ...
  -> up-to-date
```

---

## CLI reference

### `pyperun flow <name>` — run a pipeline

```bash
pyperun flow my-experiment                            # full run
pyperun flow my-experiment --step clean               # single step
pyperun flow my-experiment --from-step resample       # from a step to the end
pyperun flow my-experiment --from-step clean --to-step aggregate  # range

pyperun flow my-experiment --from 2026-02-01 --to 2026-02-10      # time filter
pyperun flow my-experiment --last                                   # incremental (delta only)

pyperun flow my-experiment --output-mode replace                    # overwrite output for the time range
pyperun flow my-experiment --output-mode full-replace               # wipe all outputs and reprocess
```

### `pyperun run <treatment>` — run a single treatment

```bash
pyperun run parse \
    --input  datasets/MY-EXPERIMENT/00_raw \
    --output datasets/MY-EXPERIMENT/10_parsed

pyperun run aggregate \
    --input  datasets/MY-EXPERIMENT/30_transform \
    --output datasets/MY-EXPERIMENT/40_aggregated \
    --params '{"windows": ["30s", "5min"], "metrics": ["mean", "median"]}'
```

### Other commands

```bash
pyperun init MY-EXPERIMENT          # scaffold a new dataset
pyperun status                      # show processing state for all datasets
pyperun list flows                  # list available flows
pyperun list treatments             # list available treatments
pyperun list steps --flow my-flow   # list steps in a flow
```

---

## Flow format

A flow is a JSON file in `flows/`. Each step declares its treatment, input directory, and output directory. The data flow is readable like a recipe.

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
            "treatment": "exportcsv",
            "input": "40_aggregated",
            "output": "61_exportcsv",
            "params": {
                "columns": {
                    "m0__raw__mean": "c0",
                    "m1__raw__mean": "c1",
                    "outdoor_temp__raw__mean": "temperature"
                }
            }
        }
    ]
}
```

When `dataset` is set, `input`/`output` paths are relative to `datasets/<DATASET>/`. Without it, use absolute paths.

### Params hierarchy

| Priority | Source | Scope |
|----------|--------|-------|
| lowest | `treatment.json` defaults | treatment built-in |
| | `flow.params` | all steps |
| | `step.params` | that step only |
| highest | CLI `--params` / `--from` / `--to` | runtime override |

`from`/`to` in `flow.params` set the default time range for all steps; CLI `--from`/`--to` always win.

---

## Pipeline steps

| Treatment | Input → Output | What it does |
|-----------|---------------|--------------|
| `parse` | `00_raw` → `10_parsed` | Parse key:value CSV → typed parquet, split by domain and day |
| `clean` | `10_parsed` → `20_clean` | Drop duplicates, clamp to min/max, remove spikes (rolling median) |
| `resample` | `20_clean` → `25_resampled` | Regular 1s grid, forward-fill short gaps |
| `transform` | `25_resampled` → `30_transform` | Apply column transforms: `sqrt_inv`, `cbrt_inv`, `log` (add or replace) |
| `normalize` | `30_transform` → `35_normalized` | Min-max normalization of selected columns *(optional)* |
| `aggregate` | `30_transform` → `40_aggregated` | Multi-window aggregation (10s, 60s, 5min, 1h) |
| `to_postgres` | `40_aggregated` → PostgreSQL | Export to wide PostgreSQL tables (e.g. for Grafana) |
| `exportcsv` | `40_aggregated` → `61_exportcsv` | Export per-device CSV with column renaming and timezone conversion |
| `exportparquet` | `40_aggregated` → `62_exportparquet` | Export selected aggregation windows to parquet |

---

## Treatment configuration

Each treatment exposes typed params with defaults, overridable in the flow or via `--params`.

<details>
<summary><strong>parse</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `delimiter` | `";"` | CSV delimiter |
| `tz` | `"UTC"` | Timezone of raw timestamps |
| `timestamp_column` | `"ts"` | Name of the timestamp field |
| `domains` | bio_signal + environment | Domain split: prefix-based or explicit columns, with dtype |
| `file_name_substitute` | `[]` | Filename substitutions for source name extraction |

</details>

<details>
<summary><strong>clean</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `drop_duplicates` | `true` | Remove duplicate timestamps |
| `domains` | per-domain | `min_value`, `max_value`, `spike_window`, `spike_threshold` per domain |

</details>

<details>
<summary><strong>resample</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `freq` | `"1s"` | Resample frequency |
| `max_gap_fill_s` | `2` | Max gap (seconds) to forward-fill |
| `agg_method` | per-domain | Aggregation method when flooring to `freq` |

</details>

<details>
<summary><strong>transform</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `transforms` | `[]` | List of `{function, target, mode}` — functions: `sqrt_inv`, `cbrt_inv`, `log`; mode: `add` or `replace` |

</details>

<details>
<summary><strong>aggregate</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `windows` | `["10s","60s","5min","1h"]` | Time windows |
| `metrics` | `["mean","std","min","max"]` | Aggregation functions |

</details>

<details>
<summary><strong>to_postgres</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `host` | `"localhost"` | PostgreSQL host |
| `port` | `5432` | PostgreSQL port |
| `dbname` | *(required)* | Database name |
| `user` | *(required)* | User |
| `password` | *(required)* | Password |
| `table_template` | `"{source}__{domain}__{aggregation}"` | Table naming pattern |
| `table_prefix` | `""` | Prefix prepended to table names |
| `mode` | `"append"` | `append` or `replace` |

</details>

<details>
<summary><strong>exportcsv</strong></summary>

| Param | Default | Description |
|-------|---------|-------------|
| `aggregation` | `"10s"` | Which aggregation window to export |
| `domain` | `"bio_signal"` | Domain to export |
| `tz` | `"Europe/Paris"` | Output timezone |
| `from` / `to` | none | Optional date range filter |
| `columns` | m0–m11 as int | `source_col → export_name` or `{"name":…,"dtype":"int","decimals":N}` |

</details>

---

## Custom treatments

Pyperun discovers treatments from two locations — **local takes priority**:

1. `./treatments/<name>/` — your project (custom or overrides)
2. `pyperun/treatments/<name>/` — built-in fallback

To add a custom treatment:

```
my-project/
  treatments/
    my_treatment/
      treatment.json    # param schema + defaults
      run.py            # def run(input_dir, output_dir, params): ...
```

To override a built-in, use the same treatment name in `./treatments/`.

---

## Project layout

```
pyperun/                          ← framework (this repo)
  cli.py                          ← pyperun command entry point
  core/
    flow.py                       ← runs a flow sequentially
    runner.py                     ← runs a single treatment
    pipeline.py                   ← treatment → directory registry
    validator.py                  ← param validation + merging
    timefilter.py                 ← time range filtering, --last logic
    filename.py                   ← parquet naming conventions
    logger.py                     ← jsonlines event log (logs/)
  treatments/                     ← built-in treatments
    parse/ clean/ resample/ transform/ normalize/
    aggregate/ to_postgres/ exportcsv/ exportparquet/
scripts/
  hourly_sync.sh                  ← cron: incremental run for one flow
  run_scheduled_flows.sh          ← cron: run all flows in scheduled_flows.txt
  run_flow_hourly.sh              ← loop: run a flow every N seconds
  update.sh                       ← git pull + pip install -e .
```

```
my-project/                       ← your experiment repo
  flows/                          ← flow definitions (JSON)
  datasets/                       ← data (gitignored)
    MY-EXPERIMENT/
      00_raw/                     ← raw CSV input
      10_parsed/
      20_clean/
      25_resampled/
      30_transform/
      40_aggregated/
      61_exportcsv/
  treatments/                     ← optional custom treatments
```

---

## Production — incremental cron

Run the pipeline automatically every hour, processing only new data:

```bash
crontab -e
# Add:
0 * * * * /path/to/pyperun/scripts/hourly_sync.sh my-flow >> /var/log/pyperun.log 2>&1
```

Or run multiple flows from a list:

```bash
# scripts/scheduled_flows.txt
my-flow-streaming
my-flow-daily

# crontab
0 * * * * /path/to/pyperun/scripts/run_scheduled_flows.sh
```

Both scripts use `--last` to detect the delta between input and output and skip runs when nothing is new.

---

## Development

```bash
pip install -e ".[dev]"

pytest tests/ -v                                      # full test suite
pytest tests/test_runner.py::test_run_with_defaults   # single test
ruff check .                                          # lint
```

---

## Data conventions

- **Parquet filenames**: `<source>__<domain>__<YYYY-MM-DD>.parquet`
- **Aggregated filenames**: `<source>__<domain>__<YYYY-MM-DD>__<window>.parquet`
- **Default domains**: `bio_signal` (m0–m11, Int64) · `environment` (outdoor_temp, Float64)
- **Logs**: `logs/pyperun.log` — jsonlines, one event per run
