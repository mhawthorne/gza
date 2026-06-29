"""Regression coverage for shared task-type admission and filtering."""

import argparse
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza.artifacts import store_command_output_artifact
from gza.cli._common import _add_query_filter_args
from gza.cli.execution import cmd_add
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.query import HistoryFilter, query_history
from gza.review_verify_state import VerifyEpoch, persist_verify_gate_artifact


def _write_config(project_dir: Path) -> None:
    (project_dir / "gza.yaml").write_text(
        "project_name: demo\n"
        "verify_command: './bin/tests'\n"
        "autonomous_verify_timeout_seconds: 300\n"
        "review_verify_timeout_grace_seconds: 5.0\n",
        encoding="utf-8",
    )


def _load_store(project_dir: Path) -> SqliteTaskStore:
    config = Config.load(project_dir)
    return SqliteTaskStore(
        config.db_path,
        prefix=config.project_prefix,
        project_id=config.project_id,
        project_root=config.project_dir,
        config_path=Config.config_path(config.project_dir),
        project_name=config.project_name,
    )


def _seed_failed_verify_evidence(
    *,
    project_dir: Path,
    store: SqliteTaskStore,
    impl_id: str,
    source_task_id: str,
    epoch: VerifyEpoch,
) -> None:
    config = Config.load(project_dir)
    impl = store.get(impl_id)
    source_task = store.get(source_task_id)
    assert impl is not None
    assert source_task is not None

    output_artifact = store_command_output_artifact(
        store,
        source_task,
        config,
        kind="verify_command_output",
        producer="test",
        label="verify_command_output",
        output="setup ok\npytest failed\nAssertionError: expected green\n",
        command=epoch.verify_command,
        status="failed",
        exit_status="1",
        head_sha=epoch.reviewed_head_sha,
        created_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
    )
    result = type(
        "Result",
        (),
        {
            "command": epoch.verify_command,
            "status": "failed",
            "exit_status": "1",
            "captured_at": datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            "reviewed_branch": epoch.reviewed_branch,
            "reviewed_head_sha": epoch.reviewed_head_sha,
            "reviewed_base_sha": "base-sha",
            "working_directory": str(project_dir / "worktrees" / "verify"),
            "failure": "pytest failed",
        },
    )()
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=source_task,
        result=result,
        verify_timeout_seconds=epoch.verify_timeout_seconds,
        verify_timeout_grace_seconds=epoch.verify_timeout_grace_seconds,
        output_artifact_id=output_artifact.id,
        output_artifact_task_id=source_task.id,
        output_artifact_path=output_artifact.path,
        producer="test",
    )


def test_cmd_add_accepts_well_formed_plan_review_and_plan_improve_task_types(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    source_plan = store.add("Draft the plan", task_type="plan")
    source_review = store.add("Review the plan", task_type="plan_review", depends_on=source_plan.id)
    store.add("Implement the plan", task_type="implement")

    args_by_type = {
        "plan_review": argparse.Namespace(
            project_dir=tmp_path,
            prompt="plan_review prompt",
            prompt_file=None,
            edit=False,
            type="plan_review",
            explore=False,
            depends_on=source_plan.id,
            based_on=None,
            review=False,
            hold_for_review=False,
            create_pr=False,
            same_branch=False,
            spec=None,
            review_scope=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
            tags=None,
        ),
        "plan_improve": argparse.Namespace(
            project_dir=tmp_path,
            prompt="plan_improve prompt",
            prompt_file=None,
            edit=False,
            type="plan_improve",
            explore=False,
            depends_on=source_review.id,
            based_on=source_plan.id,
            review=False,
            hold_for_review=False,
            create_pr=False,
            same_branch=False,
            spec=None,
            review_scope=None,
            branch_type=None,
            model=None,
            provider=None,
            skip_learnings=False,
            next=True,
            tags=None,
        ),
    }

    for task_type in ("plan_review", "plan_improve"):
        with patch("gza.cli.execution.set_task_urgency", return_value=True):
            rc = cmd_add(args_by_type[task_type])

        assert rc == 0

    pending = store.get_pending()
    assert [task.task_type for task in pending] == [
        "plan",
        "plan_review",
        "implement",
        "plan_review",
        "plan_improve",
    ]


def test_cmd_add_rejects_verify_fix_without_lineage_anchor(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="verify_fix prompt",
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=None,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "verify_fix tasks require --based-on" in capsys.readouterr().out


def test_cmd_add_accepts_same_branch_verify_fix_anchored_to_code_lineage(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/test"
    store.update(impl)
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)
    epoch = VerifyEpoch(
        reviewed_branch="feature/test",
        reviewed_head_sha="deadbeef",
        verify_command="./bin/tests",
        verify_timeout_seconds=300,
        verify_timeout_grace_seconds=5.0,
    )
    assert improve.id is not None
    _seed_failed_verify_evidence(
        project_dir=tmp_path,
        store=store,
        impl_id=impl.id,
        source_task_id=improve.id,
        epoch=epoch,
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt=None,
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=improve.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    class _Git:
        def __init__(self, _project_dir: Path) -> None:
            pass

        def rev_parse_if_exists(self, ref: str) -> str:
            assert ref == "feature/test"
            return "deadbeef"

    with patch("gza.cli.execution.Git", _Git), patch("gza.cli.execution.set_task_urgency", return_value=True):
        rc = cmd_add(args)

    assert rc == 0
    verify_fix = next(task for task in store.get_pending() if task.task_type == "verify_fix")
    assert verify_fix.based_on == improve.id
    assert verify_fix.same_branch is True
    assert verify_fix.prompt == (
        "Fix verify failures for task "
        f"{impl.id} [branch=feature/test head=deadbeef command=./bin/tests timeout=300 grace=5.0]"
    )


def test_cmd_add_rejects_manual_verify_fix_custom_prompt_text(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)
    epoch = VerifyEpoch(
        reviewed_branch="feature/test",
        reviewed_head_sha="deadbeef",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    assert impl.id is not None
    assert improve.id is not None
    _seed_failed_verify_evidence(
        project_dir=tmp_path,
        store=store,
        impl_id=impl.id,
        source_task_id=improve.id,
        epoch=epoch,
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="arbitrary prompt",
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=improve.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "derives its prompt from the latest failed verify evidence" in capsys.readouterr().out
    assert [task.task_type for task in store.get_pending()] == ["implement", "improve"]


def test_cmd_add_rejects_verify_fix_when_latest_failed_evidence_is_stale(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/test"
    store.update(impl)
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)
    assert improve.id is not None
    stale_epoch = VerifyEpoch(
        reviewed_branch="feature/test",
        reviewed_head_sha="old-head",
        verify_command="./bin/tests",
        verify_timeout_seconds=300,
        verify_timeout_grace_seconds=5.0,
    )
    _seed_failed_verify_evidence(
        project_dir=tmp_path,
        store=store,
        impl_id=impl.id,
        source_task_id=improve.id,
        epoch=stale_epoch,
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt=None,
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=improve.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    class _Git:
        def __init__(self, _project_dir: Path) -> None:
            pass

        def rev_parse_if_exists(self, ref: str) -> str:
            assert ref == "feature/test"
            return "new-head"

    with patch("gza.cli.execution.Git", _Git):
        rc = cmd_add(args)

    out = capsys.readouterr().out
    assert rc == 1
    assert "latest persisted failure is stale" in out
    assert "rerun the failing verify gate on the current implementation head" in out
    assert [task.task_type for task in store.get_pending()] == ["implement", "improve"]


def test_cmd_add_rejects_verify_fix_when_based_on_is_not_current_failed_head_representative(
    tmp_path: Path, capsys
) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/test"
    store.update(impl)
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)
    improve.status = "completed"
    improve.branch = "feature/test"
    improve.has_commits = True
    improve.completed_at = datetime(2026, 6, 29, 12, 1, tzinfo=UTC)
    store.update(improve)
    rebase = store.add("Rebase the plan", task_type="rebase", based_on=improve.id, same_branch=True)
    rebase.status = "completed"
    rebase.branch = "feature/test"
    rebase.has_commits = True
    rebase.completed_at = datetime(2026, 6, 29, 12, 2, tzinfo=UTC)
    store.update(rebase)
    epoch = VerifyEpoch(
        reviewed_branch="feature/test",
        reviewed_head_sha="deadbeef",
        verify_command="./bin/tests",
        verify_timeout_seconds=300,
        verify_timeout_grace_seconds=5.0,
    )
    assert improve.id is not None
    assert rebase.id is not None
    _seed_failed_verify_evidence(
        project_dir=tmp_path,
        store=store,
        impl_id=impl.id,
        source_task_id=rebase.id,
        epoch=epoch,
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt=None,
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=improve.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    class _Git:
        def __init__(self, _project_dir: Path) -> None:
            pass

        def rev_parse_if_exists(self, ref: str) -> str:
            assert ref == "feature/test"
            return "deadbeef"

    with patch("gza.cli.execution.Git", _Git):
        rc = cmd_add(args)

    out = capsys.readouterr().out
    assert rc == 1
    assert f"current failed verify epoch is represented by task {rebase.id}" in out
    assert f"Rerun with --based-on {rebase.id} --same-branch." in out
    assert [task.task_type for task in store.get_pending()] == ["implement"]


def test_cmd_add_accepts_verify_fix_when_based_on_matches_current_failed_head_representative(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    impl = store.add("Implement the plan", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/test"
    store.update(impl)
    improve = store.add("Improve the plan", task_type="improve", based_on=impl.id, same_branch=True)
    improve.status = "completed"
    improve.branch = "feature/test"
    improve.has_commits = True
    improve.completed_at = datetime(2026, 6, 29, 12, 1, tzinfo=UTC)
    store.update(improve)
    rebase = store.add("Rebase the plan", task_type="rebase", based_on=improve.id, same_branch=True)
    rebase.status = "completed"
    rebase.branch = "feature/test"
    rebase.has_commits = True
    rebase.completed_at = datetime(2026, 6, 29, 12, 2, tzinfo=UTC)
    store.update(rebase)
    epoch = VerifyEpoch(
        reviewed_branch="feature/test",
        reviewed_head_sha="deadbeef",
        verify_command="./bin/tests",
        verify_timeout_seconds=300,
        verify_timeout_grace_seconds=5.0,
    )
    assert rebase.id is not None
    _seed_failed_verify_evidence(
        project_dir=tmp_path,
        store=store,
        impl_id=impl.id,
        source_task_id=rebase.id,
        epoch=epoch,
    )

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt=None,
        prompt_file=None,
        edit=False,
        type="verify_fix",
        explore=False,
        depends_on=None,
        based_on=rebase.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=True,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    class _Git:
        def __init__(self, _project_dir: Path) -> None:
            pass

        def rev_parse_if_exists(self, ref: str) -> str:
            assert ref == "feature/test"
            return "deadbeef"

    with patch("gza.cli.execution.Git", _Git), patch("gza.cli.execution.set_task_urgency", return_value=True):
        rc = cmd_add(args)

    assert rc == 0
    verify_fix = next(task for task in store.get_pending() if task.task_type == "verify_fix")
    assert verify_fix.based_on == rebase.id
    assert verify_fix.same_branch is True


def test_cmd_add_rejects_plan_review_without_plan_source_dependency(tmp_path: Path, capsys) -> None:
    _write_config(tmp_path)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="Review the plan",
        prompt_file=None,
        edit=False,
        type="plan_review",
        explore=False,
        depends_on=None,
        based_on=None,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "plan_review tasks require --depends-on" in capsys.readouterr().out


def test_cmd_add_rejects_plan_improve_without_matching_plan_review_dependency(
    tmp_path: Path,
    capsys,
) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)
    plan = store.add("Draft the plan", task_type="plan")
    other_plan = store.add("Different plan", task_type="plan")
    review = store.add("Review the plan", task_type="plan_review", depends_on=other_plan.id)

    args = argparse.Namespace(
        project_dir=tmp_path,
        prompt="Revise the plan",
        prompt_file=None,
        edit=False,
        type="plan_improve",
        explore=False,
        depends_on=review.id,
        based_on=plan.id,
        review=False,
        hold_for_review=False,
        create_pr=False,
        same_branch=False,
        spec=None,
        review_scope=None,
        branch_type=None,
        model=None,
        provider=None,
        skip_learnings=False,
        next=False,
        tags=None,
    )

    rc = cmd_add(args)

    assert rc == 1
    assert "must be based on the same plan source" in capsys.readouterr().out


def test_query_history_filters_accept_plan_review_plan_improve_and_verify_fix(tmp_path: Path) -> None:
    _write_config(tmp_path)
    store = _load_store(tmp_path)

    review_task = store.add("Review the plan", task_type="plan_review")
    review_task.status = "completed"
    store.update(review_task)
    improve_task = store.add("Revise the plan", task_type="plan_improve")
    improve_task.status = "completed"
    store.update(improve_task)
    verify_fix_task = store.add("Fix verify", task_type="verify_fix")
    verify_fix_task.status = "completed"
    store.update(verify_fix_task)

    review_rows = query_history(store, HistoryFilter(limit=None, task_type="plan_review"))
    improve_rows = query_history(store, HistoryFilter(limit=None, task_type="plan_improve"))
    verify_fix_rows = query_history(store, HistoryFilter(limit=None, task_type="verify_fix"))

    assert [task.task_type for task in review_rows] == ["plan_review"]
    assert [task.task_type for task in improve_rows] == ["plan_improve"]
    assert [task.task_type for task in verify_fix_rows] == ["verify_fix"]


def test_query_filter_flags_include_new_plan_review_task_types() -> None:
    parser = argparse.ArgumentParser()
    _add_query_filter_args(parser)

    choices_by_dest = {
        action.dest: set(action.choices or [])
        for action in parser._actions
        if action.dest in {"type", "type_not"}
    }

    for dest in ("type", "type_not"):
        assert "plan_review" in choices_by_dest[dest]
        assert "plan_improve" in choices_by_dest[dest]
        assert "verify_fix" in choices_by_dest[dest]
