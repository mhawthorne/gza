"""Tests for advance auto-squash and related config validation."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza.cli.git_ops import cmd_advance
from gza.config import Config, ConfigError

from tests.cli.conftest import make_store, setup_config


@pytest.fixture(autouse=True)
def _patch_ambient_real_git():
    with (
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.git.Git.branch_exists", return_value=True),
        patch("gza.git.Git.ref_exists", return_value=False),
    ):
        yield


def _advance_args(tmp_path: Path, **overrides) -> argparse.Namespace:
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_id=None,
        dry_run=False,
        auto=True,
        max=None,
        batch=None,
        no_docker=True,
        force=False,
        plans=False,
        unimplemented=False,
        create=False,
        no_resume_failed=False,
        max_resume_attempts=None,
        advance_type=None,
        new=False,
        max_review_cycles=None,
        squash_threshold=None,
    )
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def _mock_git(*, current_branch: str = "main", can_merge: bool = True, commit_count: int = 0) -> Mock:
    git = Mock()
    git.current_branch.return_value = current_branch
    git.can_merge.return_value = can_merge
    git.count_commits_ahead.return_value = commit_count
    return git


def _create_completed_non_implement_task(store, prompt="Document the codebase"):
    task = store.add(prompt, task_type="task")
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = f"feature/{task.id}"
    task.merge_status = "unmerged"
    task.has_commits = True
    store.update(task)
    assert task.id is not None
    unit = store.create_merge_unit(
        source_branch=task.branch,
        target_branch="main",
        owner_task_id=task.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(task.id, unit.id, "owner")
    store.dual_write_legacy_merge_status(unit.id)
    return task


def test_advance_no_squash_when_threshold_zero(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_non_implement_task(store)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(commit_count=3)),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._merge_single_task", return_value=0) as merge_single,
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    merge_args = merge_single.call_args.args[4]
    assert merge_args.squash is False


def test_advance_squash_when_commits_meet_threshold(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_non_implement_task(store)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(commit_count=3)),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._merge_single_task", return_value=0) as merge_single,
    ):
        rc = cmd_advance(_advance_args(tmp_path, squash_threshold=2))

    assert rc == 0
    merge_args = merge_single.call_args.args[4]
    assert merge_args.squash is True


def test_advance_no_squash_when_commits_below_threshold(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_non_implement_task(store)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(commit_count=2)),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops._merge_single_task", return_value=0) as merge_single,
    ):
        rc = cmd_advance(_advance_args(tmp_path, squash_threshold=3))

    assert rc == 0
    merge_args = merge_single.call_args.args[4]
    assert merge_args.squash is False


def test_advance_squash_threshold_cli_override(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    _create_completed_non_implement_task(store)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(commit_count=2)),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
    ):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True, squash_threshold=2))

    assert rc == 0
    assert "auto-squash" in capsys.readouterr().out


def test_advance_dry_run_shows_squash_annotation(tmp_path: Path, capsys) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "merge_squash_threshold: 2\n"
    )
    store = make_store(tmp_path)
    _create_completed_non_implement_task(store)

    with (
        patch("gza.cli.git_ops.Git", return_value=_mock_git(commit_count=3)),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
    ):
        rc = cmd_advance(_advance_args(tmp_path, dry_run=True))

    assert rc == 0
    assert "auto-squash" in capsys.readouterr().out


def test_default_merge_squash_threshold_is_zero(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config = Config.load(tmp_path)
    assert config.merge_squash_threshold == 0
    assert config.max_resume_attempts == 1


def test_yaml_merge_squash_threshold_parsed(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "merge_squash_threshold: 3\n"
    )
    config = Config.load(tmp_path)
    assert config.merge_squash_threshold == 3


def test_invalid_type_raises_config_error(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "merge_squash_threshold: two\n"
    )
    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_negative_value_raises_config_error(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "merge_squash_threshold: -1\n"
    )
    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_validate_rejects_negative_max_resume_attempts(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_resume_attempts: -1\n"
    )
    is_valid, errors, _warnings = Config.validate(tmp_path)
    assert is_valid is False
    assert "'max_resume_attempts' must be non-negative" in errors


def test_validate_rejects_non_integer_max_resume_attempts(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_resume_attempts: nope\n"
    )
    is_valid, errors, _warnings = Config.validate(tmp_path)
    assert is_valid is False
    assert "'max_resume_attempts' must be an integer" in errors


def test_validate_rejects_non_positive_max_review_cycles(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_review_cycles: 0\n"
    )
    is_valid, errors, _warnings = Config.validate(tmp_path)
    assert is_valid is False
    assert "'max_review_cycles' must be positive" in errors


def test_load_rejects_non_integer_max_resume_attempts(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_resume_attempts: nope\n"
    )
    with pytest.raises(ConfigError, match="'max_resume_attempts' must be an integer"):
        Config.load(tmp_path)


def test_load_rejects_negative_max_resume_attempts(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_resume_attempts: -1\n"
    )
    with pytest.raises(ConfigError, match="'max_resume_attempts' must be non-negative"):
        Config.load(tmp_path)


def test_load_rejects_non_integer_max_review_cycles(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_review_cycles: nope\n"
    )
    with pytest.raises(ConfigError, match="'max_review_cycles' must be an integer"):
        Config.load(tmp_path)


def test_load_rejects_non_positive_max_review_cycles(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
        "max_review_cycles: 0\n"
    )
    with pytest.raises(ConfigError, match="'max_review_cycles' must be positive"):
        Config.load(tmp_path)


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("max_resume_attempts", "true", "'max_resume_attempts' must be an integer"),
        ("max_resume_attempts", '"2"', "'max_resume_attempts' must be an integer"),
        ("max_review_cycles", "true", "'max_review_cycles' must be an integer"),
        ("max_review_cycles", '"3"', "'max_review_cycles' must be an integer"),
    ],
)
def test_load_and_validate_reject_bool_and_quoted_numeric_values(
    tmp_path: Path,
    field: str,
    value: str,
    expected_error: str,
) -> None:
    (tmp_path / "gza.yaml").write_text(
        f"project_name: test-project\n"
        f"db_path: .gza/gza.db\n"
        f"{field}: {value}\n"
    )

    is_valid, errors, _warnings = Config.validate(tmp_path)
    assert is_valid is False
    assert expected_error in errors

    with pytest.raises(ConfigError, match=expected_error):
        Config.load(tmp_path)


def test_cmd_advance_passes_git_instance_to_list_failed_tasks(tmp_path: Path) -> None:
    """Guards advance-path wiring: cmd_advance must thread its live Git instance and
    resolved target_branch through to list_failed_tasks_for_recovery so the helper
    never falls back to ambient Config.load(discover=True) + Git() discovery."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    # A failed task ensures the recovery-warning path is exercised (no_resume_failed=False).
    failed = store.add("Failed impl", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.branch = "feature/advance-wiring-test"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    mock_git = _mock_git(current_branch="main")
    captured: dict = {}

    def _spy(store_arg, *, warnings, git, target_branch, **kwargs):
        captured["git"] = git
        captured["target_branch"] = target_branch
        return []

    with (
        patch("gza.cli.git_ops.Git", return_value=mock_git),
        patch("gza.cli.git_ops._resolve_advance_target_branch", return_value="main"),
        patch("gza.cli.git_ops.query_lineage_owner_rows", return_value=iter(())),
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.git_ops.list_failed_tasks_for_recovery", side_effect=_spy),
    ):
        rc = cmd_advance(_advance_args(tmp_path))

    assert rc == 0
    assert "git" in captured, "list_failed_tasks_for_recovery was not called by cmd_advance"
    # The Git instance constructed inside cmd_advance must be the one forwarded.
    assert captured["git"] is mock_git
    # target_branch is resolved via git.current_branch() when no task_id is supplied.
    assert captured["target_branch"] == "main"
