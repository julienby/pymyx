"""Pipeline registry — convention mapping treatment → directory."""

DATASETS_PREFIX = "datasets"

PIPELINE_STEPS = [
    {"treatment": "parse",       "input": "00_raw",        "output": "10_parsed"},
    {"treatment": "clean",       "input": "10_parsed",     "output": "20_clean"},
    {"treatment": "transform",   "input": "20_clean",      "output": "25_transform"},
    {"treatment": "resample",    "input": "25_transform",  "output": "30_resampled"},
    {"treatment": "aggregate",   "input": "30_resampled",  "output": "40_aggregated"},
    {"treatment": "to_postgres", "input": "40_aggregated", "output": "60_postgres"},
    {"treatment": "exportnour",  "input": "40_aggregated", "output": "61_exportnour"},
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
