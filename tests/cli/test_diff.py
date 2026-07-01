"""Tests for `gza diff` validation and argument routing."""

import argparse
from pathlib import Path
from unittest.mock import Mock, patch

from gza.cli.git_ops import cmd_diff
from tests.cli.conftest import make_store, setup_config


def _diff_args(tmp_path: Path, *diff_args: str) -> argparse.Namespace:
    return argparse.Namespace(
        project_dir=tmp_path,
        diff_args=list(diff_args),
    )


def test_diff_with_task_id_not_found_falls_back_to_git_ref(tmp_path: Path) -> None:
    setup_config(tmp_path)
    make_store(tmp_path)

    with (
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.subprocess.run", return_value=Mock(returncode=7)) as run_call,
    ):
        rc = cmd_diff(_diff_args(tmp_path, "testproject-999999"))

    assert rc == 7
    assert run_call.call_args.args[0] == ["git", "diff", "--color=always", "testproject-999999"]


def test_diff_with_task_id_no_branch(tmp_path: Path, capsys) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Test task", task_type="implement")

    with (
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.subprocess.run") as run_call,
    ):
        rc = cmd_diff(_diff_args(tmp_path, str(task.id)))

    assert rc == 1
    assert f"Error: Task {task.id} has no branch" in capsys.readouterr().out
    run_call.assert_not_called()


def test_diff_with_non_numeric_argument_passes_through_to_git(tmp_path: Path) -> None:
    setup_config(tmp_path)

    with (
        patch("gza.cli.git_ops.Git", return_value=Mock()),
        patch("gza.cli.git_ops.subprocess.run", return_value=Mock(returncode=0)) as run_call,
    ):
        rc = cmd_diff(_diff_args(tmp_path, "--cached"))

    assert rc == 0
    assert run_call.call_args.args[0] == ["git", "diff", "--color=always", "--cached"]
