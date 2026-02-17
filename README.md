# PyMyx

Minimal IoT time-series data processing pipeline for valvometric data.

## Install

```bash
pip install -e ".[dev]"
```

## Usage

### Run a single treatment

```bash
python -m pymyx.core.runner --treatment parse \
    --input datasets/PREMANIP-GRACE/00_raw \
    --output datasets/PREMANIP-GRACE/10_parsed
```

### Run a flow (chained treatments)

```bash
python -m pymyx.core.flow --flow valvometry_daily
```

### Run tests

```bash
python -m pytest tests/ -v
```

## Architecture

```
pymyx/
  core/
    logger.py      # jsonlines event logging
    validator.py   # treatment.json validation (pydantic)
    runner.py      # single treatment executor + CLI
    flow.py        # sequential flow executor + CLI
  treatments/
    parse/         # raw CSV -> typed parquet
    clean/         # outlier removal
    transform/     # mathematical transformations (sqrt_inv, log)
    resample/      # fixed-frequency resampling
    aggregate/     # multi-window aggregation
flows/
  valvometry_daily.json   # parse -> clean -> transform -> resample -> aggregate
```

Each treatment has a `treatment.json` (params schema) and a `run.py` (logic).
