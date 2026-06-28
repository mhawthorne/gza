from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch
from unittest.mock import Mock

from gza.config import Config
from gza.db import WatchProgressObservation
from gza.lineage_query import LineageOwnerRow
from gza.unstick import (
    RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON,
    discover_parked_tasks,
    select_and_clear_parked_tasks,
)
from gza.watch_progress import (
    WATCH_NO_PROGRESS_BACKSTOP_REASON,
    build_watch_progress_candidate,
)
from tests.cli.conftest import make_store, setup_config


class _GitDouble:
    def default_branch(self) -> str:
        return "main"

    def branch_exists(self, branch: str) -> bool:
        return not branch.startswith("missing/")

    def ref_exists(self, ref: str) -> bool:
        return False

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        del into, use_cherry
        return branch.startswith("merged/")

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


def _config_and_store(tmp_path: Path):
    setup_config(tmp_path)
    return Config.load(tmp_path), make_store(tmp_path)


def _make_backstop_owner(store, *, prompt: str, branch: str):
    impl = store.add(prompt, task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = branch
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add(f"Review {prompt}", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
    store.update(review)

    candidate = build_watch_progress_candidate(
        store,
        subject_task=impl,
        action={"type": "improve", "review_task": review},
        action_task=impl,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )
    owner_row = LineageOwnerRow(
        owner_task=impl,
        members=(impl,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "skip",
            "description": "watch no progress",
            "needs_attention_reason": WATCH_NO_PROGRESS_BACKSTOP_REASON,
            "subject_task_id": impl.id,
        },
        next_action_reason="needs_attention",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
    )
    return impl, owner_row


def test_discover_parked_tasks_includes_owner_row_reconcile_and_watch_backstop(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    git = _GitDouble()

    reconcile_task = store.add("Needs manual reconcile", task_type="implement")
    assert reconcile_task.id is not None
    reconcile_task.status = "completed"
    reconcile_task.completed_at = datetime.now(UTC)
    reconcile_task.branch = "feature/reconcile"
    reconcile_task.has_commits = True
    reconcile_task.tags = ("ops",)
    store.update(reconcile_task)

    backstop_task = store.add("No progress owner", task_type="implement")
    assert backstop_task.id is not None
    backstop_task.status = "completed"
    backstop_task.completed_at = datetime.now(UTC)
    backstop_task.branch = "feature/backstop"
    backstop_task.has_commits = True
    backstop_task.tags = ("ops",)
    store.update(backstop_task)
    store.set_merge_status(backstop_task.id, "unmerged")
    review = store.add("Review no progress owner", task_type="review", depends_on=backstop_task.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime.now(UTC)
    review.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix it."
    store.update(review)
    candidate = build_watch_progress_candidate(
        store,
        subject_task=backstop_task,
        action={"type": "improve", "review_task": review},
        action_task=backstop_task,
        failed_task=None,
    )
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=3,
            parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
            observed_at=datetime.now(UTC),
        )
    )

    owner_rows = (
        LineageOwnerRow(
            owner_task=reconcile_task,
            members=(reconcile_task,),
            tree=None,
            lineage_status="needs_attention",
            next_action={
                "type": "skip",
                "description": "manual reconcile required",
                "needs_attention_reason": RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON,
            },
            next_action_reason="needs_attention",
            unresolved_tasks=(reconcile_task,),
            unresolved_leaf_summary=(),
        ),
        LineageOwnerRow(
            owner_task=backstop_task,
            members=(backstop_task,),
            tree=None,
            lineage_status="needs_attention",
            next_action={
                "type": "skip",
                "description": "watch no progress",
                "needs_attention_reason": WATCH_NO_PROGRESS_BACKSTOP_REASON,
                "subject_task_id": backstop_task.id,
            },
            next_action_reason="needs_attention",
            unresolved_tasks=(backstop_task,),
            unresolved_leaf_summary=(),
        ),
    )

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=(owner_rows, object())):
        candidates, stale_cleared = discover_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
        )

    assert stale_cleared == 0
    assert {(candidate.owner_task.id, candidate.reason_class) for candidate in candidates} == {
        (reconcile_task.id, "reconcile"),
        (backstop_task.id, "backstop"),
    }


def test_select_and_clear_parked_tasks_clears_backstop_and_is_idempotent(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    git = _GitDouble()

    impl, owner_row = _make_backstop_owner(store, prompt="Backstop candidate", branch="feature/backstop")
    impl.tags = ("ops", "critical")
    store.update(impl)

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((owner_row,), object())):
        first = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
            reason_classes=("backstop",),
        )
    assert [outcome.status for outcome in first.outcomes] == ["rearmed"]
    assert store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=str(store.get_or_create_merge_unit_for_task(impl).id)) == []

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((), object())):
        second = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
        )
    assert [(outcome.status, outcome.detail) for outcome in second.outcomes] == [("skipped", "not currently parked")]


def test_select_and_clear_parked_tasks_handles_reconcile_clear_only(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    git = _GitDouble()

    impl = store.add("Needs reconcile clear-only", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.completed_at = datetime.now(UTC)
    impl.branch = "feature/reconcile-clear-only"
    impl.has_commits = True
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    owner_row = LineageOwnerRow(
        owner_task=impl,
        members=(impl,),
        tree=None,
        lineage_status="needs_attention",
        next_action={
            "type": "skip",
            "description": "manual reconcile required",
            "needs_attention_reason": RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON,
            "subject_task_id": impl.id,
        },
        next_action_reason="needs_attention",
        unresolved_tasks=(impl,),
        unresolved_leaf_summary=(),
    )

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((owner_row,), object())):
        result = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
            reason_classes=("reconcile",),
        )

    assert [(outcome.status, outcome.reason_class, outcome.detail) for outcome in result.outcomes] == [
        ("rearmed", "reconcile", f"cleared {RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON}"),
    ]
    assert store.list_all_watch_progress_observations() == []


def test_select_and_clear_parked_tasks_applies_landed_and_missing_branch_guards(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    git = _GitDouble()

    merged = store.add("Already landed", task_type="implement")
    assert merged.id is not None
    merged.status = "completed"
    merged.completed_at = datetime.now(UTC)
    merged.branch = "merged/already-landed"
    merged.has_commits = True
    store.update(merged)

    missing = store.add("Missing branch", task_type="implement")
    assert missing.id is not None
    missing.status = "completed"
    missing.completed_at = datetime.now(UTC)
    missing.branch = "missing/cannot-prove"
    missing.has_commits = True
    store.update(missing)

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((), object())):
        result = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(merged.id, missing.id),
        )

    assert [(outcome.owner_task.id, outcome.detail) for outcome in result.outcomes] == [
        (merged.id, "already merged"),
        (missing.id, "missing branch cannot prove unresolved"),
    ]


def test_select_and_clear_parked_tasks_skips_remote_only_branch_without_remote_target_proof(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    impl, owner_row = _make_backstop_owner(
        store,
        prompt="Remote-only branch without target proof",
        branch="feature/remote-only-no-target",
    )
    merge_unit = store.get_or_create_merge_unit_for_task(impl)
    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.side_effect = lambda ref: ref == f"origin/{impl.branch}"

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((owner_row,), object())):
        result = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
        )

    assert [(outcome.status, outcome.detail) for outcome in result.outcomes] == [
        ("skipped", "missing branch cannot prove unresolved"),
    ]
    observations = store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=str(merge_unit.id))
    assert len(observations) == 1
    git.is_merged.assert_not_called()


def test_select_and_clear_parked_tasks_uses_remote_proof_path_for_remote_only_unresolved_branch(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    impl, owner_row = _make_backstop_owner(
        store,
        prompt="Remote-only branch with unresolved proof",
        branch="feature/remote-only-unmerged",
    )
    merge_unit = store.get_or_create_merge_unit_for_task(impl)
    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.side_effect = lambda ref: ref in {f"origin/{impl.branch}", "origin/main"}
    git.is_merged.return_value = False
    git.rev_parse_if_exists.side_effect = lambda ref: {
        f"origin/{impl.branch}": "head-remote-only-unmerged",
        "origin/main": "base-origin-main",
    }.get(ref)

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((owner_row,), object())):
        result = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
        )

    assert [(outcome.status, outcome.detail) for outcome in result.outcomes] == [
        ("rearmed", f"cleared {WATCH_NO_PROGRESS_BACKSTOP_REASON}"),
    ]
    assert store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=str(merge_unit.id)) == []
    git.is_merged.assert_called_once_with(f"origin/{impl.branch}", into="origin/main")


def test_select_and_clear_parked_tasks_skips_remote_only_branch_when_remote_proves_merged(tmp_path: Path) -> None:
    config, store = _config_and_store(tmp_path)
    impl, owner_row = _make_backstop_owner(
        store,
        prompt="Remote-only branch already merged",
        branch="feature/remote-only-merged",
    )
    merge_unit = store.get_or_create_merge_unit_for_task(impl)
    git = Mock()
    git.branch_exists.return_value = False
    git.ref_exists.side_effect = lambda ref: ref in {f"origin/{impl.branch}", "origin/main"}
    git.is_merged.return_value = True
    git.count_commits_ahead_checked.return_value = 1
    git.get_diff_numstat.return_value = "1\t0\tfeature.txt\n"
    git.rev_parse_if_exists.side_effect = lambda ref: {
        f"origin/{impl.branch}": "head-remote-only-merged",
        "origin/main": "base-origin-main",
    }.get(ref)

    with patch("gza.unstick.query_lineage_owner_rows_in_read_session", return_value=((owner_row,), object())):
        result = select_and_clear_parked_tasks(
            store,
            config=config,
            git=git,
            target_branch="main",
            task_ids=(impl.id,),
        )

    assert [(outcome.status, outcome.detail) for outcome in result.outcomes] == [
        ("skipped", "already merged"),
    ]
    observations = store.list_watch_progress_observations(subject_kind="merge_unit", subject_id=str(merge_unit.id))
    assert len(observations) == 1
    git.is_merged.assert_called_once_with(f"origin/{impl.branch}", into="origin/main")
