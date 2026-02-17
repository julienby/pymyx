# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyMyx is a minimal IoT time-series data processing pipeline for valvometric data. It processes raw sensor CSV files (key:value format) through a 6-step pipeline to produce ML-ready aggregated parquet files and export to PostgreSQL (parse → clean → transform → resample → aggregate → to_postgres).

## Build & Run

```bash
# Install
pip install -e ".[dev]"

# Run the full pipeline
python -m pymyx.core.flow --flow valvometry_daily

# Run a single treatment
python -m pymyx.core.runner --treatment parse --input datasets/PREMANIP-GRACE/00_raw --output datasets/PREMANIP-GRACE/10_parsed

# Run with custom params
python -m pymyx.core.runner --treatment aggregate --input datasets/PREMANIP-GRACE/40_resampled --output datasets/PREMANIP-GRACE/50_aggregated --params '{"windows": ["30s", "5min"], "metrics": ["mean", "median"]}'

# Run tests
python -m pytest tests/ -v

# Run a single test
python -m pytest tests/test_runner.py::test_run_with_defaults -v

# Lint
ruff check .
```

## Architecture

### Core

- `pymyx/core/runner.py` — loads a treatment, validates params, executes run(), logs events. CLI entry point.
- `pymyx/core/flow.py` — reads a flow JSON, runs steps sequentially via runner. Stops on first error.
- `pymyx/core/validator.py` — pydantic validation of treatment.json + param merging (defaults + overrides)
- `pymyx/core/logger.py` — jsonlines event logging to `pymyx.log`

### Pipeline (6 treatments)

| Step | Treatment | Input → Output | What it does |
|------|-----------|----------------|--------------|
| 1 | `parse` | 00_raw → 10_parsed | Parse key:value CSV → typed parquet, split by domain (bio_signal, environment) and day |
| 2 | `clean` | 10_parsed → 20_clean | Drop duplicates, enforce min/max bounds, remove spikes via rolling median |
| 3 | `transform` | 20_clean → 25_transform | Apply declarative mathematical transformations (sqrt_inv, log) to selected columns |
| 4 | `resample` | 25_transform → 30_resampled | Regular 1s grid (86400 rows/day), floor to second, ffill small gaps (≤2s) |
| 5 | `aggregate` | 30_resampled → 40_aggregated | Multi-window aggregation (10s, 60s, 5min, 1h) with configurable metrics (mean, std, min, max) |
| 6 | `to_postgres` | any step → PostgreSQL | Export parquet data to PostgreSQL wide tables for observability (Grafana) |

### Key files

- `pymyx/treatments/<name>/treatment.json` — declares params with types and defaults
- `pymyx/treatments/<name>/run.py` — implements `def run(input_dir, output_dir, params)`
- `flows/valvometry_daily.json` — defines the 5-step pipeline

## Configuration

Each treatment is configured via `treatment.json` which declares typed params with defaults. Params can be overridden via `--params '{}'` CLI argument or in flow step definitions.

Key configurable params:
- **parse**: `delimiter`, `tz`, `domains` (define domain split and column selection)
- **clean**: `drop_duplicates`, `domains` (per-domain min/max bounds, spike window/threshold)
- **transform**: `transforms` (list of transform specs: `function`, `target` domain/columns, `mode` add/replace)
- **resample**: `freq`, `max_gap_fill_s`, `agg_method` (per-domain aggregation for flooring)
- **aggregate**: `windows` (list of time windows), `metrics` (list of aggregation functions)
- **to_postgres**: `host`, `port`, `dbname`, `user`, `password` (connection), `table_template` (naming pattern), `mode` (append/replace), `sources` (list of domains with optional column filter)

## Data

- **Raw input**: `datasets/PREMANIP-GRACE/00_raw/` — 42 CSV files (~350 Mo), 2 sensors (pil-90, pil-98), 21 days
- **Raw format**: `2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94` (no header, semicolon-delimited, key:value pairs)
- **Parquet naming**: `<source>__<domain>__<YYYY-MM-DD>.parquet` (aggregated adds `__<window>`)
- **Domains**: `bio_signal` (m0-m11, Int64) and `environment` (outdoor_temp, Float64)

## Conventions

- Treatments live under `pymyx/treatments/`, flows under `flows/`
- Each treatment has `treatment.json` (schema) + `run.py` (logic)
- All logging goes to `pymyx.log` (jsonlines format, one event per line)
- Pipeline stages are numbered: 00_raw, 10_parsed, 20_clean, 25_transform, 30_resampled, 40_aggregated, 60_postgres
