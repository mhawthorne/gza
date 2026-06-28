from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest

from tests.cli.conftest import invoke_gza, setup_config


def test_unstick_requires_explicit_selector(tmp_path):
    setup_config(tmp_path)

    result = invoke_gza("unstick", "--project", str(tmp_path))

    assert result.returncode == 2
    assert "requires at least one selector" in result.stdout


def test_unstick_dispatches_through_live_parser(tmp_path, monkeypatch):
    setup_config(tmp_path)
    cli_main_module = importlib.import_module("gza.cli.main")
    captured = {}

    def fake_cmd(args):
        captured["command"] = args.command
        captured["task_ids"] = tuple(args.task_ids)
        captured["tags"] = list(args.tags or [])
        captured["all_tags"] = args.all_tags
        captured["reasons"] = list(args.reasons or [])
        captured["all"] = args.all
        captured["project_dir"] = args.project_dir
        return 0

    monkeypatch.setattr(cli_main_module, "cmd_unstick", fake_cmd)

    with patch.object(
        sys,
        "argv",
        [
            "gza",
            "unstick",
            "testproject-1",
            "testproject-2",
            "--tag",
            "ops",
            "--tag",
            "critical",
            "--all-tags",
            "--reason",
            "backstop",
            "--reason",
            "reconcile",
            "--all",
            "--project",
            str(tmp_path),
        ],
    ):
        result = cli_main_module.main()

    assert result == 0
    assert captured == {
        "command": "unstick",
        "task_ids": ("testproject-1", "testproject-2"),
        "tags": ["ops", "critical"],
        "all_tags": True,
        "reasons": ["backstop", "reconcile"],
        "all": True,
        "project_dir": tmp_path.resolve(),
    }


def test_unstick_help_mentions_reason_and_all_tags(tmp_path):
    setup_config(tmp_path)

    result = invoke_gza("unstick", "--help", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "--reason {backstop,reconcile}" in result.stdout
    assert "--all-tags" in result.stdout
    assert "--all" in result.stdout
