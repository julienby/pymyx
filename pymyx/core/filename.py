from __future__ import annotations

import re
from dataclasses import dataclass, replace
from pathlib import Path


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


@dataclass
class FileParts:
    experience: str
    device_id: str
    step: str
    day: str
    domain: str
    aggregation: str | None = None

    def with_step(self, step: str) -> FileParts:
        return replace(self, step=step, aggregation=None)

    def with_aggregation(self, step: str, aggregation: str) -> FileParts:
        return replace(self, step=step, aggregation=aggregation)


def parse_raw_stem(stem: str, substitutions: list[dict] | None = None) -> tuple[str, str, str]:
    """Parse a raw CSV stem after applying substitutions.

    1. Apply substitutions (e.g. PREMANIP_GRACE -> PREMANIP-GRACE)
    2. Extract date from the right (_YYYY-MM-DD)
    3. rsplit('_', 1) on the remainder -> experience, device_id

    Returns (experience, device_id, day).
    """
    name = stem
    for sub in (substitutions or []):
        name = name.replace(sub["src"], sub["target"])

    # Extract date from the right
    m = _DATE_RE.search(name)
    if not m:
        raise ValueError(f"No date found in raw filename: {stem}")
    day = m.group(1)

    # Remove the date and its preceding separator
    prefix = name[: m.start()].rstrip("_")

    # Split into experience and device_id (last segment)
    if "_" in prefix:
        experience, device_id = prefix.rsplit("_", 1)
    else:
        experience = ""
        device_id = prefix

    return experience, device_id, day


def parse_parquet_path(path: Path) -> FileParts:
    """Parse a parquet path with domain= directory.

    Expected: domain=bio_signal/PREMANIP-GRACE__pil-90__clean__2026-01-25.parquet
    Or:       domain=bio_signal/PREMANIP-GRACE__pil-90__aggregated__60s__2026-01-25.parquet
    """
    # Extract domain from parent directory
    parent_name = path.parent.name
    if parent_name.startswith("domain="):
        domain = parent_name[len("domain="):]
    else:
        domain = ""

    parts = path.stem.split("__")

    if len(parts) == 4:
        # experience__device_id__step__day
        return FileParts(
            experience=parts[0],
            device_id=parts[1],
            step=parts[2],
            day=parts[3],
            domain=domain,
        )
    elif len(parts) == 5:
        # experience__device_id__step__aggregation__day
        return FileParts(
            experience=parts[0],
            device_id=parts[1],
            step=parts[2],
            day=parts[4],
            domain=domain,
            aggregation=parts[3],
        )
    else:
        raise ValueError(f"Cannot parse parquet filename: {path.name} (expected 4 or 5 '__'-separated parts)")


def build_parquet_path(parts: FileParts, output_dir: Path) -> Path:
    """Build the full output path, creating domain= directory if needed.

    Returns: output_dir/domain=bio_signal/PREMANIP-GRACE__pil-90__clean__2026-01-25.parquet
    """
    domain_dir = output_dir / f"domain={parts.domain}"
    domain_dir.mkdir(parents=True, exist_ok=True)

    if parts.aggregation:
        name = f"{parts.experience}__{parts.device_id}__{parts.step}__{parts.aggregation}__{parts.day}.parquet"
    else:
        name = f"{parts.experience}__{parts.device_id}__{parts.step}__{parts.day}.parquet"

    return domain_dir / name


def list_parquet_files(directory: Path) -> list[Path]:
    """List all parquet files in domain=*/ subdirectories."""
    return sorted(directory.glob("domain=*/*.parquet"))
