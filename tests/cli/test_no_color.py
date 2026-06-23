"""CLI color-disable integration tests."""

import io
import re
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.cli import query as query_cli, watch as watch_cli
from gza.console import build_console, set_config_no_color
from gza.db import Task

from .conftest import make_store, mark_orphaned, invoke_gza, setup_config
from .test_query import _FastUnmergedGit, _UnavailableGitHub

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _seed_task(tmp_path: Path, *, status: str = "completed", merge_status: str | None = None) -> Task:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Color output regression task", task_type="implement")
    assert task.id is not None
    task.status = status
    task.branch = "feature/color-output"
    task.has_commits = True
    task.merge_status = merge_status
    store.update(task)
    return task


def _capture_command_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *args: str,
    env: dict[str, str] | None = None,
) -> tuple[int, str]:
    output = io.StringIO()
    tty_console = build_console(
        file=output,
        force_terminal=True,
        color_system="truecolor",
        highlight=False,
    )

    monkeypatch.setattr("gza.console.console", tty_console)
    monkeypatch.setattr(query_cli, "console", tty_console)
    monkeypatch.setattr(watch_cli, "console", tty_console)
    monkeypatch.setattr(query_cli, "_stderr_console", tty_console)
    fake_git = _FastUnmergedGit()
    fake_git._branches.add("main")

    with (
        patch("gza.git.Git.default_branch", return_value="main"),
        patch("gza.git.Git.local_branch_names", return_value=()),
        patch("gza.cli.query.GitHub", _UnavailableGitHub),
        patch.object(query_cli, "Git", lambda _project_dir: fake_git),
        patch.object(watch_cli, "Git", lambda _project_dir: fake_git),
    ):
        result = invoke_gza(*args, "--project", str(tmp_path), env=env)
    return result.returncode, result.stdout + output.getvalue()


@pytest.mark.parametrize("command_name", ["ps", "show", "unmerged", "queue", "history"])
def test_no_color_config_disables_ansi_on_forced_tty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
) -> None:
    task = _seed_task(
        tmp_path,
        status="in_progress" if command_name == "ps" else "completed",
        merge_status="unmerged" if command_name == "unmerged" else None,
    )
    if command_name == "ps":
        store = make_store(tmp_path)
        mark_orphaned(store, task)

    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text(encoding="utf-8") + "no_color: true\n", encoding="utf-8")

    if command_name == "show":
        cli_args = ("show", str(task.id))
    elif command_name == "unmerged":
        fake_git = _FastUnmergedGit()
        fake_git._branches.update({"main", "feature/color-output"})
        with patch("gza.cli.main.cmd_unmerged", lambda args: query_cli.cmd_unmerged(args, git=fake_git)):
            returncode, output = _capture_command_output(monkeypatch, tmp_path, "unmerged")
    else:
        cli_args = (command_name,)

    if command_name != "unmerged":
        with patch("gza.cli.query.Git") as mock_git_cls:
            mock_git = mock_git_cls.return_value
            mock_git.default_branch.return_value = "main"
            returncode, output = _capture_command_output(monkeypatch, tmp_path, *cli_args)

    assert returncode == 0
    assert ANSI_RE.search(output) is None, output


@pytest.mark.parametrize(
    ("command_name", "seed_status"),
    [
        ("history", "completed"),
        ("show", "completed"),
        ("queue", "pending"),
    ],
)
def test_default_color_still_emits_ansi_on_forced_tty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command_name: str,
    seed_status: str,
) -> None:
    task = _seed_task(tmp_path, status=seed_status)
    cli_args = (command_name, str(task.id)) if command_name == "show" else (command_name,)

    returncode, output = _capture_command_output(monkeypatch, tmp_path, *cli_args)

    assert returncode == 0
    assert ANSI_RE.search(output) is not None, output


def test_no_color_env_disables_ansi_even_when_config_allows_color(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _seed_task(tmp_path, status="completed")

    returncode, output = _capture_command_output(
        monkeypatch,
        tmp_path,
        "show",
        str(task.id),
        env={"NO_COLOR": "1"},
    )

    assert returncode == 0
    assert ANSI_RE.search(output) is None, output


@pytest.fixture(autouse=True)
def _reset_no_color_state():
    set_config_no_color(False)
    try:
        yield
    finally:
        set_config_no_color(False)
