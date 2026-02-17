import json
import os
import tempfile
from pathlib import Path

import jsonlines
import pytest

from pymyx.core.runner import run_treatment
from pymyx.core.logger import LOG_PATH


@pytest.fixture
def tmp_treatment(tmp_path):
    """Create a minimal temporary treatment for testing."""
    treatment_dir = tmp_path / "treatments" / "echo"
    treatment_dir.mkdir(parents=True)

    treatment_json = {
        "name": "echo",
        "description": "Test treatment that writes params to a file",
        "params": {
            "greeting": {"type": "str", "default": "hello"},
            "count": {"type": "int", "default": 3},
        },
    }
    (treatment_dir / "treatment.json").write_text(json.dumps(treatment_json))

    run_py = '''
import json
from pathlib import Path

def run(input_dir, output_dir, params):
    out = Path(output_dir) / "result.json"
    out.write_text(json.dumps(params, default=str))
'''
    (treatment_dir / "run.py").write_text(run_py)

    return treatment_dir


@pytest.fixture(autouse=True)
def clean_log():
    """Remove pymyx.log before each test to isolate log assertions."""
    if LOG_PATH.exists():
        LOG_PATH.unlink()
    yield
    if LOG_PATH.exists():
        LOG_PATH.unlink()


def test_run_with_defaults(tmp_treatment, tmp_path, monkeypatch):
    """run_treatment merges defaults correctly and the run function receives them."""
    import pymyx.core.runner as runner_mod

    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_treatment.parent)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    run_treatment("echo", str(input_dir), str(output_dir))

    result = json.loads((output_dir / "result.json").read_text())
    assert result["greeting"] == "hello"
    assert result["count"] == 3


def test_run_with_override(tmp_treatment, tmp_path, monkeypatch):
    """Provided params override defaults."""
    import pymyx.core.runner as runner_mod

    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_treatment.parent)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    run_treatment("echo", str(input_dir), str(output_dir), {"greeting": "bonjour", "count": 7})

    result = json.loads((output_dir / "result.json").read_text())
    assert result["greeting"] == "bonjour"
    assert result["count"] == 7


def test_log_contains_success(tmp_treatment, tmp_path, monkeypatch):
    """pymyx.log should contain a success event after a run."""
    import pymyx.core.runner as runner_mod

    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_treatment.parent)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"

    run_treatment("echo", str(input_dir), str(output_dir))

    assert LOG_PATH.exists()
    with jsonlines.open(LOG_PATH) as reader:
        events = list(reader)

    statuses = [e["status"] for e in events]
    assert "start" in statuses
    assert "success" in statuses


def test_missing_input_dir(tmp_treatment, tmp_path, monkeypatch):
    """Should raise FileNotFoundError for non-existent input dir."""
    import pymyx.core.runner as runner_mod

    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_treatment.parent)

    with pytest.raises(FileNotFoundError):
        run_treatment("echo", str(tmp_path / "nonexistent"), str(tmp_path / "output"))


def test_unknown_param(tmp_treatment, tmp_path, monkeypatch):
    """Should raise ValueError for unknown params."""
    import pymyx.core.runner as runner_mod

    monkeypatch.setattr(runner_mod, "TREATMENTS_ROOT", tmp_treatment.parent)

    input_dir = tmp_path / "input"
    input_dir.mkdir()

    with pytest.raises(ValueError, match="Unknown params"):
        run_treatment("echo", str(input_dir), str(tmp_path / "output"), {"unknown_key": "val"})
