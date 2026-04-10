"""Packaging configuration regression tests."""

import tomllib
from pathlib import Path


def test_hatch_vcs_does_not_write_source_version_file() -> None:
    """Editable installs must not require writing src/gza/_version.py."""
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text())

    hooks = config.get("tool", {}).get("hatch", {}).get("build", {}).get("hooks", {})
    assert "vcs" not in hooks

