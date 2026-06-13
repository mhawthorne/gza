"""End-to-end coverage for the latency report script."""

from __future__ import annotations

import json
import os
import subprocess
import sys
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
    cmd = [sys.executable, "-m", "gza.test_latency"]
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{repo_root / 'src'}:{repo_root}:{env.get('PYTHONPATH', '')}".rstrip(":")

    markdown_output = tmp_path / "latency.md"
    result = subprocess.run(
        [*cmd, "--output", str(markdown_output), "--", str(suite_dir), "-q"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
        timeout=4,
    )

    assert result.returncode == 0, result.stderr
    markdown = markdown_output.read_text(encoding="utf-8")
    assert "# Unit Test Latency Report" in markdown
    assert "## Summary" in markdown
    assert "## Buckets" in markdown
    assert "## Slow tests (≥p95)" in markdown
    assert "## Slow tests (≥p99)" in markdown
    slow_p95_section = markdown.split("## Slow tests (≥p95)", maxsplit=1)[1]
    assert "| Duration | Test" in slow_p95_section
    assert "`" in slow_p95_section

    json_output = tmp_path / "latency.json"
    json_result = subprocess.run(
        [*cmd, "--json", "--output", str(json_output), "--", str(suite_dir), "-q"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
        timeout=4,
    )

    assert json_result.returncode == 0, json_result.stderr
    payload = json.loads(json_output.read_text(encoding="utf-8"))
    assert payload["tests_run"] == 2
    assert payload["slow_tests_p95"]


@pytest.mark.timeout(30, method="signal")
def test_test_latency_summary_flushes_on_sigterm(tmp_path: Path) -> None:
    suite_dir = tmp_path / "sigterm_suite"
    suite_dir.mkdir()
    (suite_dir / "test_sigterm_summary.py").write_text(
        "import time\n\n"
        "def test_fast_before_sigterm():\n"
        "    time.sleep(0.01)\n\n"
        "def test_slow_then_terminate():\n"
        "    print('READY_FOR_SIGTERM', flush=True)\n"
        "    time.sleep(30)\n",
        encoding="utf-8",
    )
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "gza.test_latency", "--summary", "--", str(suite_dir), "-q", "-s"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )

    assert proc.stderr is not None
    stderr_chunks: list[str] = []
    for _ in range(200):
        line = proc.stderr.readline()
        if not line:
            break
        stderr_chunks.append(line)
        if "READY_FOR_SIGTERM" in line:
            break
    else:
        proc.kill()
        raise AssertionError("pytest never reached the SIGTERM probe point")

    proc.terminate()
    stdout, stderr_tail = proc.communicate(timeout=10)
    stderr = "".join(stderr_chunks) + stderr_tail

    assert proc.returncode not in (None, 0)
    assert "READY_FOR_SIGTERM" in stderr
    assert "latency: p50=" in stderr
    assert "n=1" in stderr
