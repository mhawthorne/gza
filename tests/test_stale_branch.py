from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from gza.db import SqliteTaskStore
from gza.stale_branch import latest_fix_ledger_evidence, resolve_branch_staleness
from tests.cli.conftest import make_store, setup_config


class _FakeGit:
    def __init__(self, behind_count: int | None = None, *, raise_error: bool = False):
        self.behind_count = behind_count
        self.raise_error = raise_error
        self.calls: list[tuple[str, str]] = []

    def count_commits_behind(self, source_ref: str, target_ref: str) -> int | None:
        self.calls.append((source_ref, target_ref))
        if self.raise_error:
            raise RuntimeError("boom")
        return self.behind_count


def _completed_impl(store: SqliteTaskStore, *, branch: str = "feature/stale") -> Any:
    impl = store.add("Implement stale-branch signal", task_type="implement")
    impl.status = "completed"
    impl.completed_at = datetime(2026, 5, 15, 10, 0, tzinfo=UTC)
    impl.branch = branch
    impl.has_commits = True
    store.update(impl)
    return impl


def _completed_fix(
    store: SqliteTaskStore,
    impl_id: str,
    *,
    branch: str,
    output_content: str | None,
    report_file: str | None = None,
    completed_at: datetime | None = None,
):
    fix = store.add("Manual rescue via /gza-task-fix", task_type="fix", based_on=impl_id, same_branch=True)
    fix.status = "completed"
    fix.completed_at = completed_at or datetime(2026, 5, 15, 11, 0, tzinfo=UTC)
    fix.branch = branch
    fix.output_content = output_content
    fix.report_file = report_file
    store.update(fix)
    return fix


def test_latest_fix_ledger_duration_equal_timeout_recommends_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 120\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
    )

    config = SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1)
    stale = resolve_branch_staleness(
        config=config,
        store=store,
        git=_FakeGit(behind_count=0),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )

    assert stale is not None
    assert stale.recommend_rebase is True
    assert stale.reason == "verify_duration"
    assert stale.verify_duration_seconds == 120.0


def test_latest_fix_ledger_duration_below_timeout_does_not_recommend(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: diagnosed_no_change\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 119.5\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
    )

    config = SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1)
    stale = resolve_branch_staleness(
        config=config,
        store=store,
        git=_FakeGit(behind_count=0),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )

    assert stale is None


def test_failed_verify_does_not_recommend_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: false\n"
            "  duration_seconds: 240\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is not None
    assert evidence.verify_duration_seconds is None
    assert evidence.recommend_rebase is False


def test_explicit_recommend_rebase_ledger_behind_target_survives_git_unavailable(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    fix = _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: diagnosed_no_change\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 90\n"
            "  review_verify_timeout_seconds: 120\n"
            "recommend_rebase:\n"
            "  recommended: true\n"
            "  reasons:\n"
            "    - branch_behind_target\n"
            "  target_branch: main\n"
            "  source_ref: origin/feature/stale\n"
            "  behind_count: 1\n"
            "  behind_threshold: 1\n"
            "  verify_duration_seconds: 90\n"
            "  review_verify_timeout_seconds: 120\n"
            "  operator_action: run uv run gza rebase --background\n"
        ),
    )

    config = SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1)
    stale = resolve_branch_staleness(
        config=config,
        store=store,
        git=_FakeGit(raise_error=True),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )

    assert stale is not None
    assert stale.reason == "behind_target"
    assert stale.behind_count == 1
    assert stale.evidence_task_id == fix.id
    assert stale.warning is not None
    assert "behind count unavailable" in stale.warning


def test_legacy_or_malformed_fix_ledgers_are_ignored(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(store, impl.id, branch=impl.branch, output_content="fix_result: diagnosed_no_change\n")
    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content="fix_result: repaired_pending_review\nverify: [unterminated\n",
        completed_at=datetime(2026, 5, 15, 11, 1, tzinfo=UTC),
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is None


def test_newest_legacy_fix_ledger_supersedes_older_slow_verify_evidence(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 180\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
        completed_at=datetime(2026, 5, 15, 11, 0, tzinfo=UTC),
    )
    latest = _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content="fix_result: diagnosed_no_change\n",
        completed_at=datetime(2026, 5, 15, 11, 1, tzinfo=UTC),
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is not None
    assert evidence.task_id == latest.id
    assert evidence.verify_duration_seconds is None

    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1),
        store=store,
        git=_FakeGit(behind_count=0),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )
    assert stale is None


def test_newest_below_timeout_fix_ledger_supersedes_older_slow_verify_evidence(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 180\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
        completed_at=datetime(2026, 5, 15, 11, 0, tzinfo=UTC),
    )
    latest = _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=(
            "fix_result: diagnosed_no_change\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 60\n"
            "  review_verify_timeout_seconds: 120\n"
            "recommend_rebase:\n"
            "  recommended: false\n"
            "  reasons: []\n"
            "  target_branch: main\n"
            "  source_ref: origin/feature/stale\n"
            "  behind_count: 0\n"
            "  behind_threshold: 1\n"
            "  verify_duration_seconds: 60\n"
            "  review_verify_timeout_seconds: 120\n"
            "  operator_action: advisory only\n"
        ),
        completed_at=datetime(2026, 5, 15, 11, 1, tzinfo=UTC),
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is not None
    assert evidence.task_id == latest.id
    assert evidence.verify_duration_seconds == 60.0

    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1),
        store=store,
        git=_FakeGit(behind_count=0),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )
    assert stale is None


def test_fix_ledger_origin_comment_file_is_parsed(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)
    assert impl.id is not None

    summary_path = tmp_path / ".gza" / "summaries" / "fix-ledger.md"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        "<!-- origin: /gza-task-fix (manual, 2026-05-15) -->\n"
        "fix_result: diagnosed_no_change\n"
        "verify:\n"
        "  command: ./bin/tests\n"
        "  passed: true\n"
        "  duration_seconds: 121\n"
        "  review_verify_timeout_seconds: 120\n"
    )
    _completed_fix(
        store,
        impl.id,
        branch=impl.branch,
        output_content=None,
        report_file=".gza/summaries/fix-ledger.md",
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is not None
    assert evidence.verify_duration_seconds == 121.0


def test_newer_unrelated_fix_ledger_does_not_mask_same_branch_evidence(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store, branch="feature/canonical")
    assert impl.id is not None

    older_same_branch = _completed_fix(
        store,
        impl.id,
        branch="feature/canonical",
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 122\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
        completed_at=datetime(2026, 5, 15, 11, 0, tzinfo=UTC),
    )
    _completed_fix(
        store,
        impl.id,
        branch="feature/other",
        output_content=(
            "fix_result: repaired_pending_review\n"
            "verify:\n"
            "  command: ./bin/tests\n"
            "  passed: true\n"
            "  duration_seconds: 999\n"
            "  review_verify_timeout_seconds: 120\n"
        ),
        completed_at=datetime(2026, 5, 15, 12, 0, tzinfo=UTC),
    )

    evidence = latest_fix_ledger_evidence(
        store,
        impl,
        project_dir=tmp_path,
        current_review_verify_timeout_seconds=120,
    )
    assert evidence is not None
    assert evidence.task_id == older_same_branch.id


def test_behind_count_threshold_recommends_rebase(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)

    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1),
        store=store,
        git=_FakeGit(behind_count=1),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )

    assert stale is not None
    assert stale.reason == "behind_target"
    assert stale.behind_count == 1


def test_threshold_zero_disables_behind_count_recommendation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)

    git = _FakeGit(behind_count=10)
    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=0),
        store=store,
        git=git,
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )

    assert stale is None
    assert git.calls == []


def test_git_behind_count_errors_return_warning_without_false_recommendation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)

    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1),
        store=store,
        git=_FakeGit(raise_error=True),
        task=impl,
        target_branch="main",
        source_ref="origin/feature/stale",
    )
    assert stale is not None
    assert stale.recommend_rebase is False
    assert stale.reason is None
    assert stale.warning is not None
    assert "behind count unavailable" in stale.warning


def test_diverged_merge_source_warning_shape_does_not_create_false_recommendation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    impl = _completed_impl(store)

    stale = resolve_branch_staleness(
        config=SimpleNamespace(project_dir=tmp_path, review_verify_timeout_seconds=120, recommend_rebase_behind_commits=1),
        store=store,
        git=_FakeGit(behind_count=3),
        task=impl,
        target_branch="main",
        source_ref=None,
    )
    assert stale is None
