"""Tests for the manual verify-gate CLI command."""

import argparse
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gza.cli.advance_engine import determine_next_action
import gza.cli.advance_executor as advance_executor
from gza.cli.advance_executor import AdvanceActionExecutionResult
from gza.cli.git_ops import _resolve_merge_subject_query_only
from gza.cli.verify import _effective_verify_gate_decision, cmd_verify
from gza.config import Config
from gza.review_verify_state import (
    VERIFY_GATE_ARTIFACT_KIND,
    persist_verify_gate_artifact,
    resolve_verify_gate_decision,
)
from gza.review_verdict import ParsedReviewReport

from .conftest import make_store, setup_config


def _setup_verify_config(tmp_path):
    setup_config(tmp_path)
    with (tmp_path / "gza.yaml").open("a", encoding="utf-8") as handle:
        handle.write("verify_command: ./bin/tests\n")
        handle.write("autonomous_verify_timeout_seconds: 120\n")
        handle.write("review_verify_timeout_grace_seconds: 5.0\n")
    return Config.load(tmp_path)


def _completed_unmerged_task(store, prompt="Implement verified change"):
    task = store.add(prompt, task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/verified-change"
    task.has_commits = True
    store.update(task)
    store.set_merge_status(task.id, "unmerged")
    return task


def _completed_branch_task_without_merge_unit(
    store,
    prompt="Implement verified change",
    branch="feature/verified-change",
    task_type="implement",
    based_on=None,
):
    task = store.add(prompt, task_type=task_type, based_on=based_on)
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged" if task_type == "implement" else None
    store.update(task)
    return task


def _failed_branch_implement(store, prompt="Historical failed owner", branch="feature/verified-change"):
    task = store.add(prompt, task_type="implement")
    assert task.id is not None
    task.status = "failed"
    task.completed_at = datetime.now(UTC)
    task.branch = branch
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    return task


def _fake_git(tmp_path, head_sha="head-current"):
    return SimpleNamespace(
        repo_dir=tmp_path,
        default_branch=MagicMock(return_value="main"),
        branch_exists=MagicMock(return_value=True),
        rev_parse_if_exists=MagicMock(return_value=head_sha),
        can_merge=MagicMock(return_value=True),
        is_merged=MagicMock(return_value=False),
        get_diff_name_status=MagicMock(return_value=""),
        merge_base=MagicMock(return_value="base-sha"),
    )


def _persist_verify(
    store,
    config,
    task,
    *,
    status,
    exit_status,
    head_sha="head-current",
    path=None,
    captured_at=None,
    reviewed_branch=None,
):
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=task,
        source_task=task,
        result=SimpleNamespace(
            command="./bin/tests",
            status=status,
            exit_status=exit_status,
            captured_at=captured_at or datetime.now(UTC),
            reviewed_branch=task.branch if reviewed_branch is None else reviewed_branch,
            reviewed_head_sha=head_sha,
            reviewed_base_sha="base-sha",
            working_directory=str(config.project_dir),
            failure=None if status == "passed" else "verify failed",
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        output_artifact_path=path or f".gza/artifacts/{task.id}/verify-command-output.md",
        producer="test",
    )


def _args(tmp_path, task_id, *, dry_run=False, force=False):
    return argparse.Namespace(project_dir=tmp_path, task_id=task_id, dry_run=dry_run, force=force)


def _attach_merge_unit(store, owner, contributor):
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(contributor.id, unit.id, "member")
    return unit


def _completed_approved_review(store, owner, *, head_sha="head-current"):
    review = store.add(f"Review {owner.id}", task_type="review", based_on=owner.id, depends_on=owner.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.review_verify_head_sha = head_sha
    review.output_content = "## Verdict\n\nVerdict: APPROVED\n"
    store.update(review)
    return review


def _snapshot_verify_dry_run_state(store):
    with store._connect() as conn:
        tables = {}
        for table in ("tasks", "merge_units", "merge_unit_tasks", "task_artifacts"):
            rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            tables[table] = [dict(row) for row in rows]
    return tables


def _snapshot_db_bytes(store):
    return store.db_path.read_bytes()


def _completed_branchless_review(store, prompt="Branchless review"):
    review = store.add(prompt, task_type="review")
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.branch = None
    store.update(review)
    return review


def _verify_decision_summary(owner, decision):
    result = decision.lookup.result
    evidence_source = None
    if result is not None:
        evidence_source = result.output_artifact_task_id or result.source_task_id
    return {
        "owner_id": owner.id,
        "verdict": result.status if result is not None else None,
        "exit_status": result.exit_status if result is not None else None,
        "evidence_source": evidence_source,
        "artifact_path": result.output_artifact_path if result is not None else None,
    }


def _verify_resolution_summary(resolved, decision):
    result = decision.lookup.result
    evidence_source = None
    if result is not None:
        evidence_source = result.output_artifact_task_id or result.source_task_id
    epoch = decision.current_epoch
    return {
        "owner_id": resolved.merge_subject.id,
        "representative_id": resolved.execution_task.id,
        "epoch": {
            "branch": epoch.reviewed_branch if epoch is not None else None,
            "head": epoch.reviewed_head_sha if epoch is not None else None,
            "command": epoch.verify_command if epoch is not None else None,
        },
        "verdict": result.status if result is not None else None,
        "exit_status": result.exit_status if result is not None else None,
        "evidence_source": evidence_source,
        "artifact_path": result.output_artifact_path if result is not None else None,
    }


def _dry_run_effective_verify_summary(store, config, git, task):
    resolved = _resolve_merge_subject_query_only(store, git, task.id, target_branch="main")
    assert resolved is not None
    decision = _effective_verify_gate_decision(
        store=store,
        owner_task=resolved.merge_subject,
        config=config,
        git=git,
        member_tasks=resolved.merge_member_tasks,
    )
    return _verify_decision_summary(resolved.merge_subject, decision)


def _dry_run_resolution_verify_summary(store, config, git, task):
    resolved = _resolve_merge_subject_query_only(store, git, task.id, target_branch="main")
    assert resolved is not None
    decision = _effective_verify_gate_decision(
        store=store,
        owner_task=resolved.merge_subject,
        config=config,
        git=git,
        member_tasks=resolved.merge_member_tasks,
    )
    return _verify_resolution_summary(resolved, decision)


def _writable_effective_verify_summary(store, config, git, task):
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    owner = store.resolve_merge_unit_owner_task(unit)
    assert owner is not None
    decision = _effective_verify_gate_decision(store=store, owner_task=owner, config=config, git=git)
    return _verify_decision_summary(owner, decision)


def _writable_resolution_verify_summary(store, config, git, task):
    unit = store.get_or_create_merge_unit_for_task(task)
    assert unit is not None
    owner = store.resolve_merge_unit_owner_task(unit)
    assert owner is not None
    representative = store.resolve_merge_unit_representative_task(
        unit,
        preferred_task_id=task.id,
        require_actionable=True,
    )
    if representative is None:
        representative = task if task.branch == unit.source_branch else owner
    decision = _effective_verify_gate_decision(store=store, owner_task=owner, config=config, git=git)
    return _verify_resolution_summary(
        SimpleNamespace(merge_subject=owner, execution_task=representative),
        decision,
    )


def test_verify_red_epoch_that_becomes_green_records_pass_and_exits_zero(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="red-output.md")
    git = _fake_git(tmp_path)

    def execute_verify(*, task, action, context):
        owner = action["verify_owner_task"]
        _persist_verify(context.store, context.config, owner, status="passed", exit_status="0", path="green-output.md")
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="success",
            success_message="Verify gate passed for the current tip before merge.",
            work_done=True,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify) as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, task.id))

    assert rc == 0
    execute_action.assert_called_once()
    decision = resolve_verify_gate_decision(store, task, config=config, git=git)
    assert decision.state == "passed"
    output = capsys.readouterr().out
    assert "Verify gate passed" in output
    assert "artifact: green-output.md" in output


def test_verify_red_epoch_that_stays_red_records_failure_and_exits_one(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="red-output.md")
    git = _fake_git(tmp_path)

    def execute_verify(*, task, action, context):
        owner = action["verify_owner_task"]
        _persist_verify(context.store, context.config, owner, status="failed", exit_status="1", path="still-red-output.md")
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="skip",
            message="SKIP: verify gate remained failed; merge is blocked.",
            work_done=True,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, task.id))

    assert rc == 1
    decision = resolve_verify_gate_decision(store, task, config=config, git=git)
    assert decision.state == "failed"
    output = capsys.readouterr().out
    assert "Verify gate: failed" in output
    assert "artifact: still-red-output.md" in output


def test_verify_current_green_is_noop_without_force(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="passed", exit_status="0", path="green-output.md")
    git = _fake_git(tmp_path)
    before = store.list_artifacts(task.id, kind=VERIFY_GATE_ARTIFACT_KIND)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, task.id))

    assert rc == 0
    execute_action.assert_not_called()
    assert store.list_artifacts(task.id, kind=VERIFY_GATE_ARTIFACT_KIND) == before
    output = capsys.readouterr().out
    assert "already passed for the current epoch" in output


def test_verify_merge_unit_newer_contributor_red_rerun_can_clear_block(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Owner green")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Contributor red",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    _attach_merge_unit(store, owner, contributor)
    captured_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        owner,
        status="passed",
        exit_status="0",
        path="owner-green.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        contributor,
        status="failed",
        exit_status="1",
        path="contributor-red.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        dry_rc = cmd_verify(_args(tmp_path, owner.id, dry_run=True))

    dry_output = capsys.readouterr().out
    assert dry_rc == 1
    execute_action.assert_not_called()
    assert "[dry-run] Verify gate: failed" in dry_output
    assert f"evidence: {contributor.id}" in dry_output
    assert "artifact: contributor-red.md" in dry_output

    action_types = []

    def execute_action(*, task, action, context):
        action_types.append(action["type"])
        if action["type"] == "verify_gate":
            owner_task = action["verify_owner_task"]
            _persist_verify(
                context.store,
                context.config,
                owner_task,
                status="passed",
                exit_status="0",
                path="owner-rerun-green.md",
            )
            return AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="success",
                success_message="Verify gate passed for the current tip before merge.",
                work_done=True,
            )
        return advance_executor.execute_advance_action(task=task, action=action, context=context)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_action),
    ):
        normal_rc = cmd_verify(_args(tmp_path, owner.id))

    normal_output = capsys.readouterr().out
    assert normal_rc == 0
    assert action_types == ["reconcile_verify_gate_evidence", "verify_gate"]
    assert "Recredited current merge-unit verify gate evidence (failed)" in normal_output
    assert "Verify gate passed for the current tip before merge." in normal_output
    assert "Verify gate: passed" in normal_output
    assert "artifact: owner-rerun-green.md" in normal_output
    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    next_action = determine_next_action(config, store, git, refreshed_owner, "main", selected_for_merge=True)
    assert next_action["type"] not in {
        "reconcile_verify_gate_evidence",
        "verify_gate",
        "create_verify_fix",
        "run_verify_fix",
    }


def test_verify_merge_unit_newer_contributor_red_rerun_keeps_new_failure(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Owner green")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Contributor red",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    _attach_merge_unit(store, owner, contributor)
    captured_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        owner,
        status="passed",
        exit_status="0",
        path="owner-green.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        contributor,
        status="failed",
        exit_status="1",
        path="contributor-red.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    git = _fake_git(tmp_path)
    action_types = []

    def execute_action(*, task, action, context):
        action_types.append(action["type"])
        if action["type"] == "verify_gate":
            owner_task = action["verify_owner_task"]
            _persist_verify(
                context.store,
                context.config,
                owner_task,
                status="failed",
                exit_status="1",
                path="owner-rerun-failed.md",
            )
            return AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="skip",
                message="SKIP: verify gate remained failed; merge is blocked.",
                work_done=True,
            )
        return advance_executor.execute_advance_action(task=task, action=action, context=context)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_action),
    ):
        normal_rc = cmd_verify(_args(tmp_path, owner.id))

    normal_output = capsys.readouterr().out
    assert normal_rc == 1
    assert action_types == ["reconcile_verify_gate_evidence", "verify_gate"]
    assert "Recredited current merge-unit verify gate evidence (failed)" in normal_output
    assert "SKIP: verify gate remained failed; merge is blocked." in normal_output
    assert "Verify gate: failed" in normal_output
    assert "artifact: owner-rerun-failed.md" in normal_output


def test_verify_merge_unit_newer_contributor_green_dry_run_and_no_force_reconcile_without_verify(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Owner red")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Contributor green",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    _attach_merge_unit(store, owner, contributor)
    captured_at = datetime(2026, 8, 21, 11, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        owner,
        status="failed",
        exit_status="1",
        path="owner-red.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        contributor,
        status="passed",
        exit_status="0",
        path="contributor-green.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    git = _fake_git(tmp_path)
    before_dry_run = _snapshot_verify_dry_run_state(store)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        dry_rc = cmd_verify(_args(tmp_path, owner.id, dry_run=True))

    dry_output = capsys.readouterr().out
    assert dry_rc == 0
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before_dry_run
    assert "[dry-run] Verify gate: passed" in dry_output
    assert f"evidence: {contributor.id}" in dry_output

    action_types = []

    def execute_action(*, task, action, context):
        action_types.append(action["type"])
        if action["type"] == "verify_gate":
            raise AssertionError("verify should not rerun when reconciliation makes the epoch green")
        return advance_executor.execute_advance_action(task=task, action=action, context=context)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_action),
    ):
        normal_rc = cmd_verify(_args(tmp_path, owner.id))

    normal_output = capsys.readouterr().out
    refreshed_owner = store.get(owner.id)
    assert refreshed_owner is not None
    assert normal_rc == 0
    assert action_types == ["reconcile_verify_gate_evidence"]
    assert "Recredited current merge-unit verify gate evidence (passed)" in normal_output
    assert "Verify gate: passed" in normal_output
    next_action = determine_next_action(config, store, git, refreshed_owner, "main", selected_for_merge=True)
    assert next_action["type"] not in {
        "reconcile_verify_gate_evidence",
        "verify_gate",
        "create_verify_fix",
        "run_verify_fix",
    }


def test_verify_blocked_stale_owner_reconciles_green_to_canonical_representative_without_rerun(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    stored_owner = _failed_branch_implement(store, prompt="Failed stored owner")
    representative = _completed_branch_task_without_merge_unit(
        store,
        prompt="Actionable representative",
        branch=stored_owner.branch,
    )
    unit = store.create_merge_unit(
        source_branch=stored_owner.branch,
        target_branch="main",
        owner_task_id=stored_owner.id,
        state="blocked",
    )
    store.attach_task_to_merge_unit(stored_owner.id, unit.id, "owner")
    store.attach_task_to_merge_unit(representative.id, unit.id, "member")
    captured_at = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        representative,
        status="failed",
        exit_status="1",
        path="representative-red.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        stored_owner,
        status="passed",
        exit_status="0",
        path="stored-owner-green.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    git = _fake_git(tmp_path)
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        dry_rc = cmd_verify(_args(tmp_path, representative.id, dry_run=True))

    dry_output = capsys.readouterr().out
    assert dry_rc == 0
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    assert f"[dry-run] Verify gate: passed for {representative.id}" in dry_output
    assert f"evidence: {stored_owner.id}" in dry_output
    dry_summary = _dry_run_resolution_verify_summary(store, config, git, representative)
    writable_summary = _writable_resolution_verify_summary(store, config, git, representative)
    assert dry_summary == writable_summary == {
        "owner_id": representative.id,
        "representative_id": representative.id,
        "epoch": {"branch": representative.branch, "head": "head-current", "command": "./bin/tests"},
        "verdict": "passed",
        "exit_status": "0",
        "evidence_source": stored_owner.id,
        "artifact_path": "stored-owner-green.md",
    }

    action_types = []

    def execute_action(*, task, action, context):
        action_types.append(action["type"])
        if action["type"] == "verify_gate":
            raise AssertionError("verify should not rerun when owner reconciliation makes the epoch green")
        return advance_executor.execute_advance_action(task=task, action=action, context=context)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_action),
    ):
        normal_rc = cmd_verify(_args(tmp_path, representative.id))

    normal_output = capsys.readouterr().out
    assert normal_rc == 0
    assert action_types == ["reconcile_verify_gate_evidence"]
    assert "Recredited current merge-unit verify gate evidence (passed)" in normal_output
    assert f"Verify gate: passed for {representative.id}" in normal_output
    assert "artifact: stored-owner-green.md" in normal_output
    next_action = determine_next_action(config, store, git, representative, "main", selected_for_merge=True)
    assert next_action["type"] not in {
        "reconcile_verify_gate_evidence",
        "verify_gate",
        "create_verify_fix",
        "run_verify_fix",
    }


def test_verify_red_pre_review_refresh_uses_shared_planner_metadata(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="pre-review-red.md")
    git = _fake_git(tmp_path)
    actions = []

    def execute_verify(*, task, action, context):
        actions.append(action)
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="skip",
            message="SKIP: verify gate remained failed; review is blocked.",
            handled_task_id=action["verify_owner_task"].id,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, task.id))

    assert rc == 1
    assert len(actions) == 1
    action = actions[0]
    assert action["type"] == "verify_gate"
    assert action["verify_owner_task"].id == task.id
    assert action["verify_gate_phase"] == "pre_review"
    assert action["verify_gate_state"] == "failed"
    assert action["verify_gate_explicit_refresh"] is True
    assert "review is blocked" in capsys.readouterr().out


def test_verify_red_pre_merge_refresh_uses_shared_planner_metadata(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _completed_approved_review(store, task)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="pre-merge-red.md")
    git = _fake_git(tmp_path)
    actions = []

    def execute_verify(*, task, action, context):
        actions.append(action)
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="skip",
            message="SKIP: verify gate remained failed; merge is blocked.",
            handled_task_id=action["verify_owner_task"].id,
        )

    with (
        patch("gza.advance_engine.get_review_report", return_value=ParsedReviewReport("APPROVED", (), "legacy")),
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, task.id))

    assert rc == 1
    assert len(actions) == 1
    action = actions[0]
    assert action["type"] == "verify_gate"
    assert action["verify_owner_task"].id == task.id
    assert action["verify_gate_phase"] == "pre_merge"
    assert action["verify_gate_state"] == "failed"
    assert action["verify_gate_explicit_refresh"] is True
    assert "merge is blocked" in capsys.readouterr().out


def test_verify_force_failed_executor_reports_pre_existing_green_and_exits_one(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="passed", exit_status="0", path="old-green-output.md")
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch(
            "gza.cli.verify.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="skip",
                message="SKIP: could not prepare the verify-gate worktree; merge is blocked.",
            ),
        ),
    ):
        rc = cmd_verify(_args(tmp_path, task.id, force=True))

    output = capsys.readouterr().out
    assert rc == 1
    assert "SKIP: could not prepare the verify-gate worktree" in output
    assert "Pre-existing verify gate evidence: passed" in output
    assert "Forced verify rerun did not produce new current green evidence." in output
    assert "Verify gate: passed" not in output


def test_verify_force_persistence_failure_reports_pre_existing_green_and_exits_one(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="passed", exit_status="0", path="old-green-output.md")
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch(
            "gza.cli.verify.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="error",
                error_message="could not persist verify gate result",
            ),
        ),
    ):
        rc = cmd_verify(_args(tmp_path, task.id, force=True))

    output = capsys.readouterr().out
    assert rc == 1
    assert "could not persist verify gate result" in output
    assert "Pre-existing verify gate evidence: passed" in output
    assert "Forced verify rerun did not produce new current green evidence." in output
    assert "Verify gate: passed" not in output


def test_verify_force_new_failed_artifact_cannot_reuse_later_pre_existing_green(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    captured_at = datetime(2026, 8, 21, 12, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        task,
        status="passed",
        exit_status="0",
        path="old-green-output.md",
        captured_at=captured_at + timedelta(minutes=5),
    )
    git = _fake_git(tmp_path)

    def execute_verify(*, task, action, context):
        owner = action["verify_owner_task"]
        _persist_verify(
            context.store,
            context.config,
            owner,
            status="failed",
            exit_status="1",
            path="new-failed-output.md",
            captured_at=captured_at,
        )
        decision = resolve_verify_gate_decision(context.store, owner, config=context.config, git=git)
        assert decision.state == "passed"
        assert decision.lookup.result is not None
        assert decision.lookup.result.output_artifact_path == "old-green-output.md"
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="success",
            success_message="Verify gate passed for the current tip before merge.",
            work_done=True,
            handled_task_id=owner.id,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, task.id, force=True))

    output = capsys.readouterr().out
    assert rc == 1
    assert "Verify gate passed for the current tip before merge." in output
    assert "Pre-existing verify gate evidence: passed" in output
    assert "artifact: old-green-output.md" in output
    assert "Forced verify rerun did not produce new current green evidence." in output
    assert "artifact: new-failed-output.md" not in output


def test_verify_force_new_green_evidence_exits_zero(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="passed", exit_status="0", path="old-green-output.md")
    git = _fake_git(tmp_path)

    def execute_verify(*, task, action, context):
        owner = action["verify_owner_task"]
        _persist_verify(
            context.store,
            context.config,
            owner,
            status="passed",
            exit_status="0",
            path="new-green-output.md",
        )
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="success",
            success_message="Verify gate passed for the current tip before merge.",
            work_done=True,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, task.id, force=True))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Verify gate: passed" in output
    assert "artifact: new-green-output.md" in output
    assert "Pre-existing verify gate evidence" not in output


def test_verify_force_uses_representative_owner_for_fresh_green_evidence(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _failed_branch_implement(store)
    representative = _completed_branch_task_without_merge_unit(
        store,
        prompt="Completed representative",
        branch=owner.branch,
    )
    unit = _attach_merge_unit(store, owner, representative)
    assert unit.owner_task_id == owner.id
    _persist_verify(store, config, owner, status="passed", exit_status="0", path="old-owner-green.md")
    git = _fake_git(tmp_path)

    def execute_verify(*, task, action, context):
        assert task.id == representative.id
        verify_owner = action["verify_owner_task"]
        assert verify_owner.id == representative.id
        _persist_verify(
            context.store,
            context.config,
            verify_owner,
            status="passed",
            exit_status="0",
            path="fresh-representative-green.md",
        )
        return AdvanceActionExecutionResult(
            action_type="verify_gate",
            status="success",
            success_message="Verify gate passed for the current tip before merge.",
            work_done=True,
            handled_task_id=verify_owner.id,
        )

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action", side_effect=execute_verify),
    ):
        rc = cmd_verify(_args(tmp_path, representative.id, force=True))

    output = capsys.readouterr().out
    assert rc == 0
    assert "Verify gate: passed" in output
    assert f"for {representative.id}" in output
    assert "artifact: fresh-representative-green.md" in output
    assert "Forced verify rerun did not produce new current green evidence." not in output


def test_verify_force_representative_setup_failure_cannot_reuse_old_owner_green(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _failed_branch_implement(store)
    representative = _completed_branch_task_without_merge_unit(
        store,
        prompt="Completed representative",
        branch=owner.branch,
    )
    _attach_merge_unit(store, owner, representative)
    _persist_verify(
        store,
        config,
        representative,
        status="passed",
        exit_status="0",
        path="old-representative-green.md",
    )
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch(
            "gza.cli.verify.execute_advance_action",
            return_value=AdvanceActionExecutionResult(
                action_type="verify_gate",
                status="skip",
                message="SKIP: could not prepare the verify-gate worktree; merge is blocked.",
                handled_task_id=representative.id,
            ),
        ),
    ):
        rc = cmd_verify(_args(tmp_path, representative.id, force=True))

    output = capsys.readouterr().out
    assert rc == 1
    assert "SKIP: could not prepare the verify-gate worktree" in output
    assert "Pre-existing verify gate evidence: passed" in output
    assert "artifact: old-representative-green.md" in output
    assert "Forced verify rerun did not produce new current green evidence." in output


def test_verify_dry_run_legacy_branch_task_without_merge_unit_writes_nothing(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Legacy completed task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    task.branch = "feature/legacy-without-unit"
    task.has_commits = True
    task.merge_status = "unmerged"
    store.update(task)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="legacy-red-output.md")
    assert store.resolve_merge_unit_for_task(task.id) is None
    before = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, task.id, dry_run=True))

    assert rc == 1
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before
    output = capsys.readouterr().out
    assert "[dry-run] Verify gate: failed" in output
    assert store.resolve_merge_unit_for_task(task.id) is None


def test_verify_dry_run_unmaterialized_legacy_unit_matches_writable_newer_red(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Legacy owner green")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Legacy contributor red",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    captured_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    _persist_verify(store, config, owner, status="passed", exit_status="0", path="legacy-owner-green.md", captured_at=captured_at)
    _persist_verify(
        store,
        config,
        contributor,
        status="failed",
        exit_status="1",
        path="legacy-contributor-red.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    assert store.resolve_merge_unit_for_task(contributor.id) is None
    before = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, contributor.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 1
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before
    assert f"[dry-run] Verify gate: failed for {owner.id}" in output
    assert f"evidence: {contributor.id}" in output
    assert "artifact: legacy-contributor-red.md" in output

    unit = store.get_or_create_merge_unit_for_task(contributor)
    assert unit is not None
    assert unit.owner_task_id == owner.id
    writable_owner = store.resolve_merge_unit_owner_task(unit)
    assert writable_owner is not None
    writable_decision = _effective_verify_gate_decision(store=store, owner_task=writable_owner, config=config, git=git)
    assert writable_decision.state == "failed"
    assert writable_decision.lookup.result is not None
    assert writable_decision.lookup.result.output_artifact_path == "legacy-contributor-red.md"


def test_verify_dry_run_unmaterialized_legacy_unit_matches_writable_newer_green(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Legacy owner red")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Legacy contributor green",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    captured_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    _persist_verify(store, config, owner, status="failed", exit_status="1", path="legacy-owner-red.md", captured_at=captured_at)
    _persist_verify(
        store,
        config,
        contributor,
        status="passed",
        exit_status="0",
        path="legacy-contributor-green.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    assert store.resolve_merge_unit_for_task(contributor.id) is None
    before = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, contributor.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before
    assert f"[dry-run] Verify gate: passed for {owner.id}" in output
    assert f"evidence: {contributor.id}" in output
    assert "artifact: legacy-contributor-green.md" in output

    unit = store.get_or_create_merge_unit_for_task(contributor)
    assert unit is not None
    assert unit.owner_task_id == owner.id
    writable_owner = store.resolve_merge_unit_owner_task(unit)
    assert writable_owner is not None
    writable_decision = _effective_verify_gate_decision(store=store, owner_task=writable_owner, config=config, git=git)
    assert writable_decision.state == "passed"
    assert writable_decision.lookup.result is not None
    assert writable_decision.lookup.result.output_artifact_path == "legacy-contributor-green.md"


def test_verify_dry_run_unattached_task_joining_active_unit_sees_existing_member_evidence(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Active unit owner green")
    attached_member = _completed_branch_task_without_merge_unit(
        store,
        prompt="Attached member red",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    trigger = _completed_branch_task_without_merge_unit(
        store,
        prompt="Unattached same-lineage trigger",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    _attach_merge_unit(store, owner, attached_member)
    captured_at = datetime(2026, 8, 21, 13, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        owner,
        status="passed",
        exit_status="0",
        path="active-owner-green.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        attached_member,
        status="failed",
        exit_status="1",
        path="attached-member-red.md",
        captured_at=captured_at + timedelta(minutes=1),
    )
    assert store.resolve_merge_unit_for_task(trigger.id) is None
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, trigger.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 1
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    dry_summary = _dry_run_effective_verify_summary(store, config, git, trigger)
    assert dry_summary == {
        "owner_id": owner.id,
        "verdict": "failed",
        "exit_status": "1",
        "evidence_source": attached_member.id,
        "artifact_path": "attached-member-red.md",
    }
    assert f"[dry-run] Verify gate: failed for {owner.id}" in output
    assert f"evidence: {attached_member.id}" in output
    assert "artifact: attached-member-red.md" in output

    writable_summary = _writable_effective_verify_summary(store, config, git, trigger)
    assert writable_summary == dry_summary


def test_verify_dry_run_unmaterialized_unit_sees_linked_branchless_review_evidence(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Legacy owner red")
    trigger = _completed_branch_task_without_merge_unit(
        store,
        prompt="Legacy contributor",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    review = store.add("Review with green verify evidence", task_type="review", based_on=owner.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.branch = None
    store.update(review)
    captured_at = datetime(2026, 8, 21, 14, 0, tzinfo=UTC)
    _persist_verify(
        store,
        config,
        owner,
        status="failed",
        exit_status="1",
        path="branch-owner-red.md",
        captured_at=captured_at,
    )
    _persist_verify(
        store,
        config,
        review,
        status="passed",
        exit_status="0",
        path="branchless-review-green.md",
        captured_at=captured_at + timedelta(minutes=1),
        reviewed_branch=owner.branch,
    )
    assert review.id not in {task.id for task in store.get_tasks_for_branch(owner.branch)}
    assert store.resolve_merge_unit_for_task(trigger.id) is None
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, trigger.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    dry_summary = _dry_run_effective_verify_summary(store, config, git, trigger)
    assert dry_summary == {
        "owner_id": owner.id,
        "verdict": "passed",
        "exit_status": "0",
        "evidence_source": review.id,
        "artifact_path": "branchless-review-green.md",
    }
    assert f"[dry-run] Verify gate: passed for {owner.id}" in output
    assert f"evidence: {review.id}" in output
    assert "artifact: branchless-review-green.md" in output

    writable_summary = _writable_effective_verify_summary(store, config, git, trigger)
    assert writable_summary == dry_summary


def test_verify_dry_run_branchless_review_trigger_matches_writable_resolution_without_mutation(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Reviewed owner")
    unit = store.create_merge_unit(
        source_branch=owner.branch,
        target_branch="main",
        owner_task_id=owner.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(owner.id, unit.id, "owner")
    review = store.add("Branchless review trigger", task_type="review", based_on=owner.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.branch = None
    store.update(review)
    _persist_verify(
        store,
        config,
        owner,
        status="passed",
        exit_status="0",
        path="owner-green-for-review.md",
        captured_at=datetime(2026, 8, 21, 15, 0, tzinfo=UTC),
    )
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, review.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    dry_summary = _dry_run_resolution_verify_summary(store, config, git, review)
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    assert dry_summary == {
        "owner_id": owner.id,
        "representative_id": owner.id,
        "epoch": {"branch": owner.branch, "head": "head-current", "command": "./bin/tests"},
        "verdict": "passed",
        "exit_status": "0",
        "evidence_source": owner.id,
        "artifact_path": "owner-green-for-review.md",
    }
    assert f"[dry-run] Verify gate: passed for {owner.id}" in output
    assert "artifact: owner-green-for-review.md" in output

    writable_summary = _writable_resolution_verify_summary(store, config, git, review)
    assert writable_summary == dry_summary


def test_verify_branchless_self_linked_review_cycle_refuses_without_mutation(
    tmp_path,
    capsys,
):
    _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    review = _completed_branchless_review(store, "Self-linked review")
    review.based_on = review.id
    review.depends_on = review.id
    store.update(review)
    before_tables = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    for dry_run in (True, False):
        with (
            patch("gza.cli.verify.Git", return_value=git),
            patch("gza.cli.verify.execute_advance_action") as execute_action,
            patch("gza.cli.verify._resolve_merge_subject") as writable_resolve,
        ):
            rc = cmd_verify(_args(tmp_path, review.id, dry_run=dry_run))

        output = capsys.readouterr().out
        assert rc == 1
        execute_action.assert_not_called()
        writable_resolve.assert_not_called()
        assert "Error: lineage cycle while resolving merge-unit plan" in output
        assert f"{review.id} -> {review.id}" in output
        assert "Traceback" not in output
        assert _snapshot_verify_dry_run_state(store) == before_tables


def test_verify_branchless_two_review_cycle_refuses_without_mutation(
    tmp_path,
    capsys,
):
    _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    first = _completed_branchless_review(store, "First branchless review")
    second = _completed_branchless_review(store, "Second branchless review")
    first.based_on = second.id
    second.based_on = first.id
    store.update(first)
    store.update(second)
    before_tables = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    for dry_run in (True, False):
        with (
            patch("gza.cli.verify.Git", return_value=git),
            patch("gza.cli.verify.execute_advance_action") as execute_action,
            patch("gza.cli.verify._resolve_merge_subject") as writable_resolve,
        ):
            rc = cmd_verify(_args(tmp_path, first.id, dry_run=dry_run))

        output = capsys.readouterr().out
        assert rc == 1
        execute_action.assert_not_called()
        writable_resolve.assert_not_called()
        assert "Error: lineage cycle while resolving merge-unit plan" in output
        assert f"{first.id} -> {second.id} -> {first.id}" in output
        assert "Traceback" not in output
        assert _snapshot_verify_dry_run_state(store) == before_tables


def test_verify_dry_run_attached_successful_implementation_previews_owner_tip_sync_without_mutation(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    stale_owner = _completed_branch_task_without_merge_unit(store, prompt="Stale owner")
    latest_tip = _completed_branch_task_without_merge_unit(
        store,
        prompt="Latest implementation tip",
        branch=stale_owner.branch,
    )
    stale_owner.completed_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    latest_tip.completed_at = datetime(2026, 8, 21, 11, 0, tzinfo=UTC)
    store.update(stale_owner)
    store.update(latest_tip)
    unit = _attach_merge_unit(store, stale_owner, latest_tip)
    assert unit.owner_task_id == stale_owner.id
    _persist_verify(
        store,
        config,
        latest_tip,
        status="passed",
        exit_status="0",
        path="latest-tip-green.md",
        captured_at=datetime(2026, 8, 21, 16, 0, tzinfo=UTC),
    )
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, latest_tip.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    dry_summary = _dry_run_resolution_verify_summary(store, config, git, latest_tip)
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    assert dry_summary == {
        "owner_id": latest_tip.id,
        "representative_id": latest_tip.id,
        "epoch": {"branch": latest_tip.branch, "head": "head-current", "command": "./bin/tests"},
        "verdict": "passed",
        "exit_status": "0",
        "evidence_source": latest_tip.id,
        "artifact_path": "latest-tip-green.md",
    }
    assert f"[dry-run] Verify gate: passed for {latest_tip.id}" in output
    assert "artifact: latest-tip-green.md" in output

    writable_summary = _writable_resolution_verify_summary(store, config, git, latest_tip)
    assert writable_summary == dry_summary
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.owner_task_id == latest_tip.id


def test_verify_dry_run_unattached_successful_implementation_joining_active_unit_previews_owner_tip_sync(
    tmp_path, capsys
):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    stale_owner = _completed_branch_task_without_merge_unit(store, prompt="Active stale owner")
    attached_member = _completed_branch_task_without_merge_unit(
        store,
        prompt="Attached fix",
        branch=stale_owner.branch,
        task_type="fix",
        based_on=stale_owner.id,
    )
    latest_tip = _completed_branch_task_without_merge_unit(
        store,
        prompt="Unattached latest implementation tip",
        branch=stale_owner.branch,
        based_on=stale_owner.id,
    )
    stale_owner.completed_at = datetime(2026, 8, 21, 10, 0, tzinfo=UTC)
    latest_tip.completed_at = datetime(2026, 8, 21, 11, 0, tzinfo=UTC)
    store.update(stale_owner)
    store.update(latest_tip)
    unit = _attach_merge_unit(store, stale_owner, attached_member)
    assert unit.owner_task_id == stale_owner.id
    assert store.resolve_merge_unit_for_task(latest_tip.id) is None
    _persist_verify(
        store,
        config,
        latest_tip,
        status="passed",
        exit_status="0",
        path="unattached-latest-tip-green.md",
        captured_at=datetime(2026, 8, 21, 17, 0, tzinfo=UTC),
    )
    before_tables = _snapshot_verify_dry_run_state(store)
    before_bytes = _snapshot_db_bytes(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, latest_tip.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    dry_summary = _dry_run_resolution_verify_summary(store, config, git, latest_tip)
    assert _snapshot_verify_dry_run_state(store) == before_tables
    assert _snapshot_db_bytes(store) == before_bytes
    assert dry_summary == {
        "owner_id": latest_tip.id,
        "representative_id": latest_tip.id,
        "epoch": {"branch": latest_tip.branch, "head": "head-current", "command": "./bin/tests"},
        "verdict": "passed",
        "exit_status": "0",
        "evidence_source": latest_tip.id,
        "artifact_path": "unattached-latest-tip-green.md",
    }
    assert f"[dry-run] Verify gate: passed for {latest_tip.id}" in output
    assert "artifact: unattached-latest-tip-green.md" in output

    writable_summary = _writable_resolution_verify_summary(store, config, git, latest_tip)
    assert writable_summary == dry_summary
    refreshed_unit = store.get_merge_unit(unit.id)
    assert refreshed_unit is not None
    assert refreshed_unit.owner_task_id == latest_tip.id


def test_verify_dry_run_existing_merge_unit_uses_owner_and_effective_evidence(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    owner = _completed_branch_task_without_merge_unit(store, prompt="Owner with unit")
    contributor = _completed_branch_task_without_merge_unit(
        store,
        prompt="Contributor with unit",
        branch=owner.branch,
        task_type="fix",
        based_on=owner.id,
    )
    _attach_merge_unit(store, owner, contributor)
    _persist_verify(
        store,
        config,
        contributor,
        status="passed",
        exit_status="0",
        path="existing-unit-green.md",
        captured_at=datetime(2026, 8, 21, 12, 0, tzinfo=UTC),
    )
    before = _snapshot_verify_dry_run_state(store)
    git = _fake_git(tmp_path)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, contributor.id, dry_run=True))

    output = capsys.readouterr().out
    assert rc == 0
    execute_action.assert_not_called()
    assert _snapshot_verify_dry_run_state(store) == before
    assert f"[dry-run] Verify gate: passed for {owner.id}" in output
    assert f"evidence: {contributor.id}" in output
    assert "artifact: existing-unit-green.md" in output


def test_verify_dry_run_reports_current_epoch_without_mutation(tmp_path, capsys):
    config = _setup_verify_config(tmp_path)
    store = make_store(tmp_path)
    task = _completed_unmerged_task(store)
    _persist_verify(store, config, task, status="failed", exit_status="1", path="red-output.md")
    git = _fake_git(tmp_path)
    before = store.list_artifacts(task.id, kind=VERIFY_GATE_ARTIFACT_KIND)

    with (
        patch("gza.cli.verify.Git", return_value=git),
        patch("gza.cli.verify.execute_advance_action") as execute_action,
    ):
        rc = cmd_verify(_args(tmp_path, task.id, dry_run=True))

    assert rc == 1
    execute_action.assert_not_called()
    assert store.list_artifacts(task.id, kind=VERIFY_GATE_ARTIFACT_KIND) == before
    output = capsys.readouterr().out
    assert "[dry-run] Verify gate: failed" in output
    assert "head=head-current" in output
