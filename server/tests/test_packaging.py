from __future__ import annotations

import re
import shutil
import subprocess
import tomllib
from pathlib import Path


def test_minimum_dependency_install_rejects_malformed_bulk_preview_json() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    metadata = tomllib.loads((project_dir / "pyproject.toml").read_text())
    declared = [
        *metadata["project"]["dependencies"],
        *metadata["dependency-groups"]["dev"],
    ]
    minimum_requirements = []
    for requirement in declared:
        match = re.fullmatch(r"([A-Za-z0-9_.-]+)>=(\d+(?:\.\d+)*)", requirement)
        if match:
            minimum_requirements.append(f"{match.group(1)}=={match.group(2)}")

    uv = shutil.which("uv")
    assert uv is not None
    smoke_test = """
from fastapi.testclient import TestClient
from gza_server.app import create_app


def unexpected_store_resolution():
    raise AssertionError("malformed JSON resolved the task store")


response = TestClient(create_app(store_factory=unexpected_store_resolution)).post(
    "/api/tasks/tags/bulk",
    json={
        "status": ["pending"],
        "mutation": "add",
        "mutation_tag": "new",
        "q": {"unexpected": True},
    },
)
assert response.status_code == 422, response.text
"""
    command = [uv, "run", "--isolated", "--no-project", "--with", str(project_dir)]
    for requirement in minimum_requirements:
        command.extend(("--with", requirement))
    command.extend(("python", "-c", smoke_test))

    result = subprocess.run(
        command,
        cwd=project_dir,
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
