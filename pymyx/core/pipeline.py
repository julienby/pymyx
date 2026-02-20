"""Pipeline registry — convention mapping treatment → directory."""

DATASETS_PREFIX = "datasets"

PIPELINE_STEPS = [
    {"treatment": "parse",       "input": "00_raw",        "output": "10_parsed"},
    {"treatment": "clean",       "input": "10_parsed",     "output": "20_clean"},
    {"treatment": "resample",    "input": "20_clean",      "output": "25_resampled"},
    {"treatment": "transform",   "input": "25_resampled",  "output": "30_transform"},
    {"treatment": "normalize",   "input": "30_transform",  "output": "35_normalized"},
    {"treatment": "aggregate",   "input": "35_normalized", "output": "40_aggregated"},
    {"treatment": "to_postgres", "input": "40_aggregated", "output": "60_postgres", "external": True},
    {"treatment": "exportcsv",   "input": "40_aggregated", "output": "61_exportcsv"},
]

# Quick lookup: treatment name → step dict
_STEP_BY_NAME = {s["treatment"]: s for s in PIPELINE_STEPS}


def resolve_paths(dataset: str, treatment: str) -> tuple[str, str]:
    """Return (input_dir, output_dir) for a treatment within a dataset."""
    step = _STEP_BY_NAME.get(treatment)
    if step is None:
        raise ValueError(f"Unknown treatment '{treatment}' in pipeline registry")
    return (
        f"{DATASETS_PREFIX}/{dataset}/{step['input']}",
        f"{DATASETS_PREFIX}/{dataset}/{step['output']}",
    )
