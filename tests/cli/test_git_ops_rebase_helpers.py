"""Tests for rebase helper functions."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.providers.base import RunResult


def _new_config(tmp_path: Path, provider: str = "codex", use_docker: bool = True) -> Config:
    return Config(project_dir=tmp_path, project_name="test", provider=provider, use_docker=use_docker)


def _new_task() -> SimpleNamespace:
    return SimpleNamespace(
        id="gza-1",
        task_type="rebase",
        provider=None,
        provider_is_explicit=False,
        model=None,
    )


def _new_log_file(tmp_path: Path) -> Path:
    path = tmp_path / ".gza" / "logs" / "rebase.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def test_invoke_provider_resolve_uses_effective_codex_provider(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="claude")
    task = _new_task()
    task.provider = "codex"
    task.provider_is_explicit = True
    log_file = _new_log_file(tmp_path)

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False),
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=0)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=log_file)

    assert result is True
    resolve_config = mock_get_provider.call_args.args[0]
    assert resolve_config.provider == "codex"
    assert resolve_config.use_docker is config.use_docker
    assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto --continue"


def test_invoke_provider_resolve_uses_worktree_mode_without_continue(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="claude")
    task = _new_task()
    worktree = tmp_path / "wt"
    worktree.mkdir()
    log_file = _new_log_file(tmp_path)

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False),
        patch("gza.skills_utils.copy_skill", return_value=(True, "installed")),
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=0)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(
            task,
            "feature",
            "main",
            config,
            log_file=log_file,
            worktree_path=worktree,
        )

    assert result is True
    assert mock_provider.run.call_args.args[1] == "/gza-rebase --auto"
    assert mock_provider.run.call_args.args[3] == worktree


def test_invoke_provider_resolve_fails_fast_when_skill_missing(tmp_path: Path, capsys) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex")
    task = _new_task()

    with (
        patch("gza.cli.ensure_skill", return_value=False),
        patch("gza.providers.get_provider") as mock_get_provider,
    ):
        result = invoke_provider_resolve(task, "feature", "main", config, log_file=_new_log_file(tmp_path))

    assert result is False
    assert mock_get_provider.call_count == 0
    captured = capsys.readouterr()
    assert "Missing required 'gza-rebase' skill" in (captured.out + captured.err)


def test_invoke_provider_resolve_honors_use_docker_override(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex", use_docker=False)
    task = _new_task()
    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False),
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=0)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=_new_log_file(tmp_path))

    assert result is True
    assert mock_get_provider.call_args.args[0].use_docker is False


def test_invoke_provider_resolve_returns_false_on_provider_exception(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex")
    task = _new_task()
    log_file = _new_log_file(tmp_path)

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
    ):
        mock_provider = Mock()
        mock_provider.run.side_effect = RuntimeError("provider failure")
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=log_file)

    assert result is False
    log_text = log_file.read_text()
    assert "Provider resolve failed with exception: provider failure" in log_text


def test_invoke_provider_resolve_returns_false_if_rebase_still_in_progress(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex")
    task = _new_task()

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=True),
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=0)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=_new_log_file(tmp_path))

    assert result is False


def test_invoke_provider_resolve_returns_false_on_nonzero_exit(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex")
    task = _new_task()

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=1)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=_new_log_file(tmp_path))

    assert result is False


def test_invoke_provider_resolve_does_not_create_internal_tasks_and_logs_to_parent_file(tmp_path: Path) -> None:
    from gza.cli import invoke_provider_resolve

    config = _new_config(tmp_path, provider="codex")
    store = SqliteTaskStore(config.db_path)
    task = _new_task()
    log_file = _new_log_file(tmp_path)

    with (
        patch("gza.cli.ensure_skill", return_value=True),
        patch("gza.providers.get_provider") as mock_get_provider,
        patch("gza.cli.git_ops._is_rebase_in_progress", return_value=False),
    ):
        mock_provider = Mock()
        mock_provider.run.return_value = RunResult(exit_code=0)
        mock_get_provider.return_value = mock_provider

        result = invoke_provider_resolve(task, "feature", "main", config, log_file=log_file)

    assert result is True
    assert store.get_history(limit=None, task_type="internal") == []
    log_text = log_file.read_text()
    assert "Provider fallback" in log_text
    assert "Running provider command: /gza-rebase --auto --continue" in log_text


def test_ensure_skill_returns_true_when_skill_already_present(tmp_path: Path) -> None:
    from gza.cli import ensure_skill

    skills_dir = tmp_path / ".claude" / "skills"
    skill_dir = skills_dir / "gza-rebase"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text("---\nname: gza-rebase\n---\n")

    result = ensure_skill("gza-rebase", "claude", tmp_path)
    assert result is True


def test_ensure_skill_installs_when_missing(tmp_path: Path) -> None:
    from gza.cli import ensure_skill

    with (
        patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve,
        patch("gza.skills_utils.copy_skill") as mock_copy,
    ):
        runtime_dir = tmp_path / ".claude" / "skills"
        mock_resolve.return_value = ("claude", runtime_dir)

        def fake_copy(name, target, force=False):
            skill_path = target / name / "SKILL.md"
            skill_path.parent.mkdir(parents=True, exist_ok=True)
            skill_path.write_text("---\nname: gza-rebase\n---\n")
            return True, "installed"

        mock_copy.side_effect = fake_copy

        result = ensure_skill("gza-rebase", "claude", tmp_path)

    assert result is True
    mock_copy.assert_called_once_with("gza-rebase", runtime_dir)


def test_ensure_skill_returns_false_when_install_fails(tmp_path: Path) -> None:
    from gza.cli import ensure_skill

    with (
        patch("gza.cli._resolve_runtime_skill_dir") as mock_resolve,
        patch("gza.skills_utils.copy_skill", return_value=(False, "copy failed: error")),
    ):
        runtime_dir = tmp_path / ".claude" / "skills"
        mock_resolve.return_value = ("claude", runtime_dir)

        result = ensure_skill("gza-rebase", "claude", tmp_path)

    assert result is False


def test_ensure_skill_returns_false_for_unknown_provider(tmp_path: Path) -> None:
    from gza.cli import ensure_skill

    result = ensure_skill("gza-rebase", "unknown-provider", tmp_path)
    assert result is False
