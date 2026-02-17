from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, field_validator


class ParamSchema(BaseModel):
    type: str
    default: Any = None

    @field_validator("type")
    @classmethod
    def check_type(cls, v: str) -> str:
        allowed = {"str", "int", "float", "bool", "list", "dict"}
        if v not in allowed:
            raise ValueError(f"type must be one of {allowed}, got '{v}'")
        return v


class TreatmentSchema(BaseModel):
    name: str
    description: str = ""
    params: dict[str, ParamSchema] = {}


TYPE_MAP = {"str": str, "int": int, "float": float, "bool": bool, "list": list, "dict": dict}


def load_treatment(treatment_dir: Path) -> TreatmentSchema:
    path = treatment_dir / "treatment.json"
    if not path.exists():
        raise FileNotFoundError(f"treatment.json not found in {treatment_dir}")
    with open(path) as f:
        data = json.load(f)
    return TreatmentSchema(**data)


def merge_params(schema: TreatmentSchema, provided: dict[str, Any]) -> dict[str, Any]:
    merged = {}
    for name, param in schema.params.items():
        if name in provided:
            val = provided[name]
        elif param.default is not None:
            val = param.default
        else:
            raise ValueError(f"Missing required param '{name}' (no default)")
        expected = TYPE_MAP[param.type]
        if not isinstance(val, expected):
            raise TypeError(f"Param '{name}' expected {param.type}, got {type(val).__name__}")
        merged[name] = val
    # Allow __-prefixed keys (framework-injected) to pass through
    framework_keys = {k: v for k, v in provided.items() if k.startswith("__")}
    unknown = set(provided) - set(schema.params) - set(framework_keys)
    if unknown:
        raise ValueError(f"Unknown params: {unknown}")
    merged.update(framework_keys)
    return merged


def validate_input_dir(input_dir: str) -> Path:
    p = Path(input_dir)
    if not p.exists():
        raise FileNotFoundError(f"input_dir does not exist: {input_dir}")
    if not p.is_dir():
        raise NotADirectoryError(f"input_dir is not a directory: {input_dir}")
    return p
