"""Functional nested-CLI coverage for provider DB snapshot routing."""

import os
import sqlite3
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from gza.cli import invoke_provider_resolve
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.providers.base import RunResult
from gza.runner import (
    ProjectBoundary,
    _provider_runtime_env_with_db_snapshot,
    _stage_worktree_agent_resources,
    _staged_provider_db_snapshot,
)
from gza.runtime_context import RuntimeExecutionContext


def _write_gza_config(project_dir: Path) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "gza.yaml").write_text(
        "project_name: nested\n"
        "project_id: nested\n"
        "project_prefix: nested\n"
        "provider: codex\n"
        "model: gpt-5.5\n"
        "db_path: .gza/gza.db\n",
        encoding="utf-8",
    )


def _new_config(project_dir: Path, *, provider: str = "codex") -> Config:
    return Config(
        project_dir=project_dir,
        project_name="nested",
        project_id="nested",
        project_prefix="nested",
        db_path_value=str(project_dir / ".gza" / "gza.db"),
        provider=provider,
        model="gpt-5.5",
        use_docker=False,
    )


def _run_nested_uv_gza(
    cwd: Path,
    provider_env: dict[str, str],
    *args: str,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(provider_env)
    return subprocess.run(
        ["uv", "--project", str(Path(__file__).resolve().parents[1]), "run", "gza", *args],
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )


def _task_prompts(db_path: Path) -> set[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return {row[0] for row in conn.execute("SELECT prompt FROM tasks")}
    finally:
        conn.close()


def test_provider_env_routes_nested_uv_gza_to_readonly_snapshot_without_mutation(tmp_path: Path) -> None:
    project_dir = tmp_path / "repo"
    worktree_dir = tmp_path / "worktree"
    _write_gza_config(project_dir)
    _write_gza_config(worktree_dir)

    config = _new_config(project_dir)
    store = SqliteTaskStore.from_config(config)
    snapshot_task = store.add("Snapshot visible task", task_type="explore")
    assert snapshot_task.id is not None
    boundary = ProjectBoundary(repo_root=project_dir, scope_root=Path("."), local_dependencies=())
    runtime_env = {**os.environ, "GZA_DB_PATH": str(config.db_path), "TOKEN": "runtime"}

    with patch("gza.skills_utils.ensure_all_skills", return_value=0):
        _stage_worktree_agent_resources(config, worktree_dir, boundary=boundary)

    live_only_task = store.add("Live only task", task_type="explore")
    assert live_only_task.id is not None
    snapshot = _staged_provider_db_snapshot(config, worktree_dir, boundary)
    provider_env = _provider_runtime_env_with_db_snapshot(runtime_env, snapshot)

    snapshot_read = _run_nested_uv_gza(worktree_dir, provider_env, "show", snapshot_task.id)
    assert snapshot_read.returncode == 0, snapshot_read.stderr
    assert "Snapshot visible task" in snapshot_read.stdout

    live_read = _run_nested_uv_gza(worktree_dir, provider_env, "show", live_only_task.id)
    assert live_read.returncode != 0
    assert f"Task {live_only_task.id} not found" in live_read.stdout

    snapshot_write = _run_nested_uv_gza(worktree_dir, provider_env, "add", "Nested provider write")
    assert snapshot_write.returncode != 0
    assert "readonly" in (snapshot_write.stdout + snapshot_write.stderr).lower()
    assert "Nested provider write" not in _task_prompts(snapshot.host_path)
    assert "Nested provider write" not in _task_prompts(config.db_path)


def test_provider_resolve_routes_nested_uv_gza_rebase_write_to_writable_snapshot_only(
    tmp_path: Path,
) -> None:
    project_dir = tmp_path / "repo"
    worktree_dir = tmp_path / "worktree"
    _write_gza_config(project_dir)
    _write_gza_config(worktree_dir)

    config = _new_config(project_dir, provider="claude")
    store = SqliteTaskStore.from_config(config)
    store.add("Parent task", task_type="implement")
    task = SimpleNamespace(
        id="nested-999",
        task_type="rebase",
        provider=None,
        provider_is_explicit=False,
        model=None,
    )
    runtime_context = RuntimeExecutionContext(
        cwd=config.project_dir,
        env={**os.environ, "GZA_DB_PATH": str(config.db_path), "TOKEN": "runtime"},
        project_id=config.project_id,
        db_path=config.db_path,
    )
    log_file = project_dir / ".gza" / "logs" / "rebase.log"

    def provider_run(_config, _prompt, _log_file, _work_dir, env=None, **_kwargs):
        assert env is not None
        assert stat.S_IMODE(Path(env["GZA_DB_PATH"]).stat().st_mode) == 0o644
        nested_write = _run_nested_uv_gza(_work_dir, env, "add", "Nested rebase provider write")
        assert nested_write.returncode == 0, nested_write.stderr
        return RunResult(exit_code=0)

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False),
        patch("gza.skills_utils.copy_skill", return_value=(True, "installed")),
    ):
        mock_provider = Mock()
        mock_provider.run.side_effect = provider_run
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(
            task,
            "feature",
            "main",
            config,
            log_file=log_file,
            worktree_path=worktree_dir,
            runtime_context=runtime_context,
        )

    assert result is True
    assert "Nested rebase provider write" in _task_prompts(worktree_dir / ".gza" / "gza.db")
    assert "Nested rebase provider write" not in _task_prompts(config.db_path)
    assert runtime_context.env["GZA_DB_PATH"] == str(config.db_path)
