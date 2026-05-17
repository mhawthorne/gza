"""End-to-end coverage for the latency report script."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest


def _write_sample_suite(test_root: Path) -> None:
    test_root.mkdir()
    (test_root / "test_latency_sample.py").write_text(
        "import time\n\n"
        "def test_fast():\n"
        "    time.sleep(0.001)\n\n"
        "def test_slow():\n"
        "    time.sleep(0.02)\n",
        encoding="utf-8",
    )


@pytest.mark.timeout(30, method="signal")
def test_test_latency_script_emits_required_sections_and_json(tmp_path: Path) -> None:
    suite_dir = tmp_path / "latency_suite"
    _write_sample_suite(suite_dir)
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "bin/test-latency"

    result = subprocess.run(
        [str(script_path), "--", str(suite_dir), "-q"],
        capture_output=True,
        text=True,
        cwd=repo_root,
        env=os.environ.copy(),
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    assert "# Unit Test Latency Report" in result.stdout
    assert "## Summary" in result.stdout
    assert "## Buckets" in result.stdout
    assert "## Slow tests (≥p95)" in result.stdout
    assert "## Slow tests (≥p99)" in result.stdout
    slow_p95_section = result.stdout.split("## Slow tests (≥p95)", maxsplit=1)[1]
    assert "| Duration | Test |" in slow_p95_section
    assert "`" in slow_p95_section

    json_output = tmp_path / "latency.json"
    json_result = subprocess.run(
        [str(script_path), "--json", "--output", str(json_output), "--", str(suite_dir), "-q"],
        capture_output=True,
        text=True,
        cwd=repo_root,
        timeout=4,
    )

    assert json_result.returncode == 0, json_result.stderr
    payload = json.loads(json_output.read_text(encoding="utf-8"))
    assert payload["tests_run"] == 2
    assert payload["slow_tests_p95"]
