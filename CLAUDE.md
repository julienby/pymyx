# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyMyx is a minimal IoT time-series data processing pipeline for valvometric data. It processes raw sensor CSV files (key:value format) through a 7-step pipeline to produce ML-ready aggregated parquet files, export to PostgreSQL, and export to CSV (parse -> clean -> resample -> transform -> aggregate -> to_postgres -> exportnour).

## Build & Run

```bash
# Install
pip install -e ".[dev]"

# CLI entry point
pymyx --help

# Initialize a new dataset
pymyx init MY-EXPERIMENT

# Run the full pipeline
pymyx flow valvometry_daily

# Run a single step from a flow
pymyx flow valvometry_daily --step clean

# Run from a step to the end
pymyx flow valvometry_daily --from-step resample

# Incremental mode (only new data)
pymyx flow valvometry_daily --last

# Run a single treatment with explicit paths
pymyx run parse --input datasets/PREMANIP-GRACE/00_raw --output datasets/PREMANIP-GRACE/10_parsed

# Run with custom params
pymyx run aggregate --input datasets/PREMANIP-GRACE/30_transform --output datasets/PREMANIP-GRACE/40_aggregated --params '{"windows": ["30s", "5min"], "metrics": ["mean", "median"]}'

# Show status of all datasets
pymyx status

# List flows / treatments / steps
pymyx list flows
pymyx list treatments
pymyx list steps --flow valvometry_daily

# Run tests
python -m pytest tests/ -v

# Run a single test
python -m pytest tests/test_runner.py::test_run_with_defaults -v

# Lint
ruff check .
```

## Architecture

### Core

- `pymyx/cli.py` — CLI entry point (`pymyx` command), subcommands: flow, run, init, status, list
- `pymyx/core/pipeline.py` — pipeline registry: maps treatment -> input/output directory convention
- `pymyx/core/flow.py` — reads a flow JSON, resolves paths from registry when `dataset` is present, runs steps sequentially via runner
- `pymyx/core/runner.py` — loads a treatment, validates params, executes run(), logs events
- `pymyx/core/validator.py` — pydantic validation of treatment.json + param merging (defaults + overrides)
- `pymyx/core/logger.py` — jsonlines event logging to `pymyx.log`
- `pymyx/core/timefilter.py` — time filtering, date extraction from filenames, incremental processing (`--last`)
- `pymyx/core/filename.py` — parquet filename conventions (parse, build, list)

### Pipeline (7 treatments)

| Step | Treatment | Input -> Output | What it does |
|------|-----------|-----------------|--------------|
| 1 | `parse` | 00_raw -> 10_parsed | Parse key:value CSV -> typed parquet, split by domain (bio_signal, environment) and day |
| 2 | `clean` | 10_parsed -> 20_clean | Drop duplicates, enforce min/max bounds, remove spikes via rolling median |
| 3 | `resample` | 20_clean -> 25_resampled | Regular 1s grid from first valid data point, floor to second, ffill small gaps (<=2s) |
| 4 | `transform` | 25_resampled -> 30_transform | Apply declarative mathematical transformations (sqrt_inv, log) to selected columns |
| 5 | `aggregate` | 30_transform -> 40_aggregated | Multi-window aggregation (10s, 60s, 5min, 1h) with configurable metrics (mean, std, min, max) |
| 6 | `to_postgres` | 40_aggregated -> PostgreSQL | Export parquet data to PostgreSQL wide tables for observability (Grafana). Marked `external` in registry. |
| 7 | `exportnour` | 40_aggregated -> 61_exportnour | Export aggregated data to CSV per device, with column selection/renaming and timezone conversion |

The pipeline registry lives in `pymyx/core/pipeline.py` (PIPELINE_STEPS). Steps marked `external: True` write to external services (not disk) and are excluded from the up-to-date check in `pymyx status`.

### Key files

- `pymyx/treatments/<name>/treatment.json` — declares params with types and defaults
- `pymyx/treatments/<name>/run.py` — implements `def run(input_dir, output_dir, params)`
- `pymyx/core/pipeline.py` — PIPELINE_STEPS registry (treatment -> directory convention)
- `flows/<name>.json` — flow definitions (simplified format with `dataset` field, or legacy format with explicit paths)

## Flow format

Simplified format (recommended): `dataset` field auto-resolves input/output paths via pipeline registry.

```json
{
    "name": "my-flow",
    "dataset": "MY-DATASET",
    "steps": [
        {"treatment": "parse"},
        {"treatment": "clean"},
        {"treatment": "to_postgres", "params": {"host": "..."}}
    ]
}
```

Legacy format (still supported): explicit `input`/`output` per step.

Steps can override `input`/`output` even in simplified format. Params override defaults from treatment.json.

## Configuration

Each treatment is configured via `treatment.json` which declares typed params with defaults. Params can be overridden via `--params '{}'` CLI argument or in flow step definitions.

Key configurable params:
- **parse**: `delimiter`, `tz`, `domains` (define domain split and column selection)
- **clean**: `drop_duplicates`, `domains` (per-domain min/max bounds, spike window/threshold)
- **transform**: `transforms` (list of transform specs: `function`, `target` domain/columns, `mode` add/replace)
- **resample**: `freq`, `max_gap_fill_s`, `agg_method` (per-domain aggregation for flooring)
- **aggregate**: `windows` (list of time windows), `metrics` (list of aggregation functions)
- **to_postgres**: `host`, `port`, `dbname`, `user`, `password` (connection), `table_template` (naming pattern), `mode` (append/replace), `sources` (list of domains with optional column filter)
- **exportnour**: `aggregation` (window to select, default "10s"), `domain` (default "bio_signal"), `tz` (timezone, default "Europe/Paris"), `from`/`to` (date range, optional), `columns` (dict mapping source column -> export name, controls selection, renaming and order)

## Data

- **Raw input**: `datasets/<DATASET>/00_raw/` — CSV files, key:value format
- **Raw format**: `2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94` (no header, semicolon-delimited, key:value pairs)
- **Parquet naming**: `<source>__<domain>__<YYYY-MM-DD>.parquet` (aggregated adds `__<window>`)
- **Domains**: `bio_signal` (m0-m11, Int64) and `environment` (outdoor_temp, Float64)

## Conventions

- Treatments live under `pymyx/treatments/`, flows under `flows/`
- Each treatment has `treatment.json` (schema) + `run.py` (logic)
- All logging goes to `pymyx.log` (jsonlines format, one event per line)
- Pipeline stages are numbered: 00_raw, 10_parsed, 20_clean, 25_resampled, 30_transform, 40_aggregated, 60_postgres, 61_exportnour
- Datasets live under `datasets/<DATASET>/` (gitignored)
- `pymyx init <DATASET>` scaffolds a new dataset with directories + flow template
