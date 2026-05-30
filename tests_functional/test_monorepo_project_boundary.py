from __future__ import annotations

import os
from pathlib import Path

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, task_id_numeric_key
from gza.git import Git
from tests_functional.helpers.cli import run_gza_subprocess


def _init_repo(path: Path) -> Git:
    git = Git(path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (path / "README.md").write_text("root\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def _write_fake_codex(bin_dir: Path) -> None:
    script = bin_dir / "codex"
    script.write_text(
        """#!/usr/bin/env python3
import json
import sys
from pathlib import Path

if "--version" in sys.argv:
    print("codex 0.0.test")
    raise SystemExit(0)

prompt = sys.stdin.read()
cwd = Path.cwd()

if "OUT_OF_SCOPE_WRITE" in prompt or "ALLOW_OUT_OF_SCOPE_WRITE" in prompt:
    if (cwd / "services").is_dir():
        target = cwd / "services" / "other" / "outside.txt"
    else:
        target = cwd.parent / "other" / "outside.txt"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("outside\\n")
elif "IN_SCOPE_WRITE" in prompt:
    (cwd / "inside.txt").write_text("inside\\n")
else:
    (cwd / "noop.txt").write_text("noop\\n")

for event in [
    {"type": "thread.started", "thread_id": "sess-test"},
    {"type": "turn.started"},
    {"type": "item.completed", "item": {"type": "agent_message", "text": "done"}},
    {"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}},
]:
    print(json.dumps(event), flush=True)
"""
    )
    script.chmod(0o755)


def _store_for(project_dir: Path) -> SqliteTaskStore:
    config = Config.load(project_dir)
    return SqliteTaskStore(config.db_path, prefix=config.project_prefix)


def _latest_task(project_dir: Path):
    store = _store_for(project_dir)
    return max(store.get_all(), key=lambda task: task_id_numeric_key(task.id))


def _task_by_id(project_dir: Path, task_id: str):
    return _store_for(project_dir).get(task_id)


def _latest_child_task(project_dir: Path, parent_id: str):
    children = [task for task in _store_for(project_dir).get_all() if task.based_on == parent_id]
    return max(children, key=lambda task: task_id_numeric_key(task.id)) if children else None


def _add_task(project_dir: Path, prompt: str) -> str:
    result = run_gza_subprocess("add", prompt, "--project", str(project_dir), cwd=project_dir, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    task = _latest_task(project_dir)
    assert task.id is not None
    return task.id


@pytest.mark.functional
@pytest.mark.timeout(30, method="signal")
def test_monorepo_project_boundary_flow(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "api"
    sibling_dir = repo_root / "services" / "other"
    bin_dir = tmp_path / "bin"

    project_dir.mkdir(parents=True)
    sibling_dir.mkdir(parents=True)
    bin_dir.mkdir()
    git = _init_repo(repo_root)
    (project_dir / "pyproject.toml").write_text("[project]\nname = 'api'\nversion = '0.1.0'\n")
    (project_dir / "uv.lock").write_text("version = 1\n")
    (sibling_dir / "tracked.txt").write_text("tracked\n")
    git._run("add", "services/api/pyproject.toml", "services/api/uv.lock", "services/other/tracked.txt")
    git._run("commit", "-m", "Add monorepo fixture layout")
    _write_fake_codex(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ.get('PATH', '')}")
    monkeypatch.setenv("CODEX_API_KEY", "test-token")

    (project_dir / "gza.yaml").write_text(
        f"""
project_name: api
provider: codex
use_docker: false
worktree_dir: {tmp_path / "worktrees"}
db_path: .gza/gza.db
verify_command: ""
""".strip()
        + "\n"
    )

    out_of_scope_task_id = _add_task(project_dir, "OUT_OF_SCOPE_WRITE")
    out_of_scope_run = run_gza_subprocess(
        "work",
        out_of_scope_task_id,
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert out_of_scope_run.returncode == 1
    assert "outside the allowed project scope" in out_of_scope_run.stdout
    assert "services/other/outside.txt" in out_of_scope_run.stdout

    failed_task = _task_by_id(project_dir, out_of_scope_task_id)
    assert failed_task is not None
    assert failed_task.status == "failed"
    assert failed_task.failure_reason == "PROJECT_SCOPE_VIOLATION"
    if failed_task.branch:
        assert git.count_commits_ahead(failed_task.branch, "main") == 0

    in_scope_task_id = _add_task(project_dir, "IN_SCOPE_WRITE")
    in_scope_run = run_gza_subprocess(
        "work",
        in_scope_task_id,
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert in_scope_run.returncode == 0
    in_scope_task = _task_by_id(project_dir, in_scope_task_id)
    assert in_scope_task is not None
    assert in_scope_task.status == "completed"
    assert in_scope_task.branch is not None
    assert git.count_commits_ahead(in_scope_task.branch, "main") == 1

    add_tag_result = run_gza_subprocess(
        "edit",
        out_of_scope_task_id,
        "--add-tag",
        "cross-project",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert add_tag_result.returncode == 0

    retry_result = run_gza_subprocess(
        "retry",
        out_of_scope_task_id,
        "--queue",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert retry_result.returncode == 0
    retried_task = _latest_child_task(project_dir, out_of_scope_task_id)
    assert retried_task is not None
    assert retried_task.id != out_of_scope_task_id
    assert "cross-project" in retried_task.tags

    retried_run = run_gza_subprocess(
        "work",
        str(retried_task.id),
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert retried_run.returncode == 0
    completed_retry = _task_by_id(project_dir, str(retried_task.id))
    assert completed_retry is not None
    assert completed_retry.status == "completed"
    assert completed_retry.branch is not None
    assert git.count_commits_ahead(completed_retry.branch, "main") == 1

    config_text = (project_dir / "gza.yaml").read_text()
    (project_dir / "gza.yaml").write_text(config_text + "enforce_project_scope: false\n")

    ungated_task_id = _add_task(project_dir, "ALLOW_OUT_OF_SCOPE_WRITE")
    ungated_run = run_gza_subprocess(
        "work",
        ungated_task_id,
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        timeout=20,
    )
    assert ungated_run.returncode == 0
    ungated_task = _task_by_id(project_dir, ungated_task_id)
    assert ungated_task is not None
    assert ungated_task.status == "completed"
    assert ungated_task.branch is not None
    assert git.count_commits_ahead(ungated_task.branch, "main") == 1
