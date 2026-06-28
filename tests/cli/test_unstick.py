from __future__ import annotations

import importlib
import sys
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from gza.git import Git
from gza.recovery_engine import _MergeContext, decide_failed_task_recovery
from tests.cli.conftest import invoke_gza, make_store, setup_config


class _UnstickGitDouble(Git):
    def __init__(self, _project_dir=None) -> None:
        pass

    def default_branch(self) -> str:
        return "main"

    def branch_exists(self, branch: str) -> bool:
        return not branch.startswith("missing/")

    def branches_exist(self, branches: tuple[str, ...]) -> dict[str, bool]:
        return {branch: self.branch_exists(branch) for branch in branches}

    def ref_exists(self, ref: str) -> bool:
        return False

    def resolve_refs(self, refs, peel: str = "commit") -> dict[str, str | None]:
        del peel
        return {str(ref): self.rev_parse_if_exists(str(ref)) for ref in refs}

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        del into, use_cherry
        return branch.startswith("merged/")

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        del branch, into
        return True

    def get_diff_numstat(self, revision_range: str) -> str:
        del revision_range
        return "1\t0\tfeature.txt\n"

    def count_commits_ahead_checked(self, branch: str, target: str) -> int | None:
        del target
        if branch.startswith("empty/"):
            return 0
        return 1

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return f"sha-{ref}"

    def local_branch_names(self) -> tuple[str, ...]:
        return ()


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
            "retry-limit",
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
        "reasons": ["backstop", "retry-limit", "reconcile"],
        "all": True,
        "project_dir": tmp_path.resolve(),
    }


def test_unstick_help_mentions_reason_and_all_tags(tmp_path):
    setup_config(tmp_path)

    result = invoke_gza("unstick", "--help", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "--reason {backstop,retry-limit,reconcile}" in result.stdout
    assert "--all-tags" in result.stdout
    assert "--all" in result.stdout


def test_unstick_cli_rearms_real_retry_limit_failed_owner_by_retry_id(tmp_path, monkeypatch):
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "max_resume_attempts: 1\n")
    store = make_store(tmp_path)

    impl = store.add("CLI retry limit owner", task_type="implement")
    assert impl.id is not None
    impl.status = "failed"
    impl.failure_reason = "MAX_TURNS"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/cli-retry-limit"
    impl.session_id = "sess-cli-retry-limit"
    impl.has_commits = False
    store.update(impl)

    first_retry = store.add(impl.prompt, task_type="implement", based_on=impl.id, depends_on=impl.depends_on)
    assert first_retry.id is not None
    first_retry.status = "failed"
    first_retry.failure_reason = "MAX_TURNS"
    first_retry.completed_at = datetime.now(UTC)
    first_retry.branch = impl.branch
    first_retry.session_id = impl.session_id
    first_retry.has_commits = False
    store.update(first_retry)

    exhausted_retry = store.add(impl.prompt, task_type="implement", based_on=impl.id, depends_on=impl.depends_on)
    assert exhausted_retry.id is not None
    exhausted_retry.status = "failed"
    exhausted_retry.failure_reason = "MAX_TURNS"
    exhausted_retry.completed_at = datetime.now(UTC)
    exhausted_retry.branch = impl.branch
    exhausted_retry.session_id = impl.session_id
    exhausted_retry.has_commits = False
    store.update(exhausted_retry)

    with patch("gza.recovery_engine._load_merge_context", return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main")):
        decision = decide_failed_task_recovery(store, impl, max_recovery_attempts=1)
    assert decision.action == "skip"
    assert decision.reason_code == "retry_limit_reached"

    monkeypatch.setattr("gza.cli.unstick.Git", _UnstickGitDouble)

    with patch("gza.recovery_engine._load_merge_context", return_value=_MergeContext(git=_UnstickGitDouble(), default_branch="main")):
        result = invoke_gza(
            "unstick",
            exhausted_retry.id,
            "--reason",
            "retry-limit",
            "--project",
            str(tmp_path),
        )

    assert result.returncode == 0
    assert "No parked owners matched" not in result.stdout
    assert "Selected 1 parked owner(s)" in result.stdout
    assert f"{impl.id} [retry-limit] CLI retry limit owner" in result.stdout

    rearm = store.get_parked_task_rearm(
        subject_kind="task",
        subject_id=impl.id,
        attention_reason="retry-limit-reached",
    )
    assert rearm is not None
    assert rearm.manual_rearm_epoch == 1
