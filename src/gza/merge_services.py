"""Shared merge safety helpers used by CLI and landing orchestration."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from gza.query import get_reviews_for_root

from .config import Config
from .db import MERGE_SOURCE_MANUAL, MERGE_SOURCE_MANUAL_FORCE, SqliteTaskStore, Task as DbTask
from .git import Git, GitError, ResolvedMergeSourceRef
from .merge_state import resolve_task_merge_source
from .review_scope import declares_spec_coherence_review_mode
from .review_verdict import ReviewFinding, get_review_content, get_review_report, summarize_review_blockers


@dataclass(frozen=True)
class ResolvedMergeSubject:
    trigger_task: DbTask
    execution_task: DbTask
    merge_subject: DbTask
    merge_unit_id: str | None
    merge_branch: str | None
    merge_source_ref: str | None
    merge_source_warning: str | None
    merge_member_tasks: tuple[DbTask, ...] = ()
    merge_resolution_warning: str | None = None


@dataclass(frozen=True)
class MergeDeferredBlockerDecision:
    review_task: DbTask | None
    blockers: tuple[ReviewFinding, ...]
    should_materialize: bool
    refusal_message: str | None = None


@dataclass(frozen=True)
class MergeDeferredBlockerMaterialization:
    decision: MergeDeferredBlockerDecision
    created: list[DbTask]
    reused: list[DbTask]

    @property
    def tasks(self) -> tuple[list[DbTask], list[DbTask]]:
        return self.created, self.reused


@dataclass(frozen=True)
class ManualMergePreflightResult:
    ok: bool
    status: Literal["ok", "already_merged", "dirty_checkout", "merge_conflict"]
    message: str | None = None
    block_reason: str | None = None


@dataclass(frozen=True)
class ManualMergeExecutionRequest:
    store: SqliteTaskStore
    config: Config
    git: Git
    merge_subject: DbTask
    merge_unit_id: str | None
    merge_branch: str
    merge_source_ref: str
    current_branch: str
    merge_source: str
    merge_preflight_target: str
    squash: bool = False
    delete_branch: bool = False
    no_followups: bool = False
    quiet_mechanics: bool = False
    materialize_side_effects: bool = True
    pre_materialized_deferred_blockers: tuple[list[DbTask], list[DbTask]] | None = None
    pre_materialized_deferred_blockers_printed: bool = False
    pending_squash_reconcile: Any = None


@dataclass(frozen=True)
class ManualMergeExecutionHooks:
    build_commit_message: Callable[[DbTask], str]
    capture_pre_squash_reconcile_state: Callable[[Git, str], Any]
    reconcile_squash_merge: Callable[[Git, str, str, Any], Any]
    print_squash_reconcile_result: Callable[[Any, bool], None]
    rev_parse_head: Callable[[Git], str | None]
    materialize_deferred_blockers: Callable[[DbTask], tuple[list[DbTask], list[DbTask]] | None]
    print_deferred_blockers: Callable[[DbTask, tuple[list[DbTask], list[DbTask]]], None]
    materialize_followups: Callable[[DbTask], tuple[list[DbTask], list[DbTask]]]
    print_followups: Callable[[DbTask, tuple[list[DbTask], list[DbTask]]], None]
    emit: Callable[[str], None] = print


@dataclass(frozen=True)
class ManualMergeExecutionResult:
    rc: int
    status: str = "merged"
    block_reason: str | None = None
    pending_squash_reconcile: Any = None
    created_followups: list[DbTask] | None = None
    reused_followups: list[DbTask] | None = None
    created_deferred_blockers: list[DbTask] | None = None
    reused_deferred_blockers: list[DbTask] | None = None


def latest_completed_review_for_merge_subject(
    store: SqliteTaskStore,
    merge_subject: DbTask,
) -> DbTask | None:
    if merge_subject.id is None:
        return None
    return next(
        (
            review
            for review in get_reviews_for_root(store, merge_subject)
            if review.status == "completed" and review.completed_at is not None
        ),
        None,
    )


def resolve_fresh_merge_source(git: Git, branch: str | None) -> ResolvedMergeSourceRef:
    if not branch:
        return ResolvedMergeSourceRef(None)
    return resolve_task_merge_source(git, branch)


def classify_manual_merge_blockers(
    *,
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    defer_blockers: bool,
    load_review_report: Callable[..., object] = get_review_report,
    load_review_content: Callable[..., str | None] = get_review_content,
    summarize_blockers: Callable[[str], Any] = summarize_review_blockers,
) -> MergeDeferredBlockerDecision:
    review_task = latest_completed_review_for_merge_subject(store, merge_subject)
    if review_task is None:
        return MergeDeferredBlockerDecision(
            review_task=None,
            blockers=(),
            should_materialize=False,
        )

    report = load_review_report(config.project_dir, review_task)
    review_content = load_review_content(config.project_dir, review_task) or ""
    if getattr(report, "verdict", None) != "CHANGES_REQUESTED":
        return MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
        )

    if declares_spec_coherence_review_mode(review_task.review_scope):
        assert merge_subject.id is not None
        assert review_task.id is not None
        return MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
            refusal_message=(
                f"Error: Task {merge_subject.id} has behavior-spec coherence "
                f"CHANGES_REQUESTED review {review_task.id}; review blockers from "
                "behavior-spec coherence reviews are not deferable."
            ),
        )

    blockers = tuple(finding for finding in getattr(report, "findings", ()) if finding.severity == "BLOCKER")
    if not blockers:
        assert merge_subject.id is not None
        assert review_task.id is not None
        return MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
            refusal_message=(
                f"Error: Task {merge_subject.id} has CHANGES_REQUESTED review {review_task.id}, "
                "but no parsed BLOCKER findings were available to defer. Refusing to guess."
            ),
        )

    assert merge_subject.id is not None
    assert review_task.id is not None
    summary = summarize_blockers(review_content)
    if summary.blocker_count != len(blockers):
        return MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=(),
            should_materialize=False,
            refusal_message=(
                f"Error: Task {merge_subject.id} has CHANGES_REQUESTED review {review_task.id}, "
                "but blocker classification did not match the parsed blocker set. Refusing to guess."
            ),
        )

    if defer_blockers:
        return MergeDeferredBlockerDecision(
            review_task=review_task,
            blockers=blockers,
            should_materialize=True,
        )

    return MergeDeferredBlockerDecision(
        review_task=review_task,
        blockers=blockers,
        should_materialize=False,
        refusal_message=(
            f"Error: Task {merge_subject.id} has open BLOCKER findings in review {review_task.id}.\n"
            "Use --defer-blockers to merge anyway and create urgent PR-required follow-up tasks."
        ),
    )


def materialize_merge_deferred_blockers(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    *,
    defer_blockers: bool,
    create_deferred_blockers: Callable[..., tuple[list[DbTask], list[DbTask]]],
    load_review_report: Callable[..., object] = get_review_report,
    load_review_content: Callable[..., str | None] = get_review_content,
    summarize_blockers: Callable[[str], Any] = summarize_review_blockers,
) -> MergeDeferredBlockerMaterialization:
    decision = classify_manual_merge_blockers(
        store=store,
        config=config,
        merge_subject=merge_subject,
        defer_blockers=defer_blockers,
        load_review_report=load_review_report,
        load_review_content=load_review_content,
        summarize_blockers=summarize_blockers,
    )
    if decision.refusal_message is not None:
        return MergeDeferredBlockerMaterialization(decision=decision, created=[], reused=[])
    if not decision.should_materialize or decision.review_task is None or not decision.blockers:
        return MergeDeferredBlockerMaterialization(decision=decision, created=[], reused=[])
    created, reused = create_deferred_blockers(
        store,
        config=config,
        review_task=decision.review_task,
        impl_task=merge_subject,
        findings=decision.blockers,
        trigger_source="manual",
    )
    return MergeDeferredBlockerMaterialization(decision=decision, created=created, reused=reused)


def materialize_merge_followups(
    store: SqliteTaskStore,
    config: Config,
    merge_subject: DbTask,
    *,
    create_followups: Callable[..., tuple[list[DbTask], list[DbTask]]],
) -> tuple[list[DbTask], list[DbTask]]:
    review_task = latest_completed_review_for_merge_subject(store, merge_subject)
    if review_task is None:
        return ([], [])
    report = get_review_report(config.project_dir, review_task)
    findings = tuple(finding for finding in report.findings if finding.severity == "FOLLOWUP")
    if not findings:
        return ([], [])
    return create_followups(
        store,
        config=config,
        review_task=review_task,
        impl_task=merge_subject,
        findings=findings,
        trigger_source="manual",
    )


def resolve_merge_target_task(
    store: SqliteTaskStore,
    task_id: str,
    target_branch: str,
) -> DbTask | None:
    task = store.get(task_id)
    if task is None:
        return None
    if task.id is None:
        return task
    unit = store.resolve_merge_unit_for_task(task.id)
    if unit is None:
        unit = store.get_or_create_merge_unit_for_task(task)
    if unit is None:
        return task
    representative = store.resolve_merge_unit_representative_task(
        unit,
        preferred_task_id=task.id,
        require_actionable=True,
    )
    if representative is not None:
        return representative
    owner = store.resolve_merge_unit_owner_task(unit)
    return owner or task


def resolve_merge_subject(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
) -> ResolvedMergeSubject | None:
    trigger_task = store.get(task_id)
    if trigger_task is None:
        return None
    trigger_source = resolve_fresh_merge_source(git, trigger_task.branch)
    if trigger_task.id is None:
        return ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    unit = store.resolve_merge_unit_for_task(trigger_task.id)
    if unit is None and trigger_task.branch:
        unit = store.get_or_create_merge_unit_for_task(trigger_task)
    if unit is None:
        return ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    merge_subject = store.resolve_merge_unit_owner_task(unit) or trigger_task
    execution_task = store.resolve_merge_unit_representative_task(
        unit,
        preferred_task_id=trigger_task.id,
        require_actionable=True,
    )
    if execution_task is None:
        execution_task = trigger_task if trigger_task.branch == unit.source_branch else merge_subject
    merge_source = resolve_fresh_merge_source(git, unit.source_branch)
    return ResolvedMergeSubject(
        trigger_task=trigger_task,
        execution_task=execution_task,
        merge_subject=merge_subject,
        merge_unit_id=unit.id,
        merge_branch=unit.source_branch,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
    )


def resolve_merge_subject_query_only(
    store: SqliteTaskStore,
    git: Git,
    task_id: str,
    *,
    target_branch: str,
) -> ResolvedMergeSubject | None:
    """Resolve a merge subject without creating or backfilling merge-unit state."""
    trigger_task = store.get(task_id)
    if trigger_task is None:
        return None
    trigger_source = resolve_fresh_merge_source(git, trigger_task.branch)
    if trigger_task.id is None:
        return ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    unit = store.resolve_merge_unit_for_task(trigger_task.id)
    plan_result = store.resolve_merge_unit_plan_result_for_task(trigger_task, target_branch=target_branch)
    if plan_result.diagnostic is not None:
        return ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
            merge_resolution_warning=plan_result.diagnostic.message,
        )
    plan = plan_result.plan
    if unit is None and plan is None:
        return ResolvedMergeSubject(
            trigger_task=trigger_task,
            execution_task=trigger_task,
            merge_subject=trigger_task,
            merge_unit_id=None,
            merge_branch=trigger_task.branch,
            merge_source_ref=trigger_source.ref,
            merge_source_warning=trigger_source.warning,
        )

    if plan is not None:
        merge_subject = plan.owner_task
        execution_task = plan.representative_task
        merge_unit_id = plan.unit.id if plan.unit is not None else None
        merge_branch = plan.source_branch
        merge_member_tasks = plan.effective_member_tasks
    else:
        assert unit is not None
        merge_subject = store.resolve_merge_unit_owner_task(unit) or trigger_task
        execution_task_candidate = store.resolve_merge_unit_representative_task(
            unit,
            preferred_task_id=trigger_task.id,
            require_actionable=True,
        )
        execution_task = (
            execution_task_candidate
            if execution_task_candidate is not None
            else trigger_task if trigger_task.branch == unit.source_branch else merge_subject
        )
        merge_unit_id = unit.id
        merge_branch = unit.source_branch
        merge_member_tasks = ()
    merge_source = resolve_fresh_merge_source(git, merge_branch)
    return ResolvedMergeSubject(
        trigger_task=trigger_task,
        execution_task=execution_task,
        merge_subject=merge_subject,
        merge_unit_id=merge_unit_id,
        merge_branch=merge_branch,
        merge_source_ref=merge_source.ref,
        merge_source_warning=merge_source.warning,
        merge_member_tasks=tuple(merge_member_tasks),
    )


def check_manual_merge_preflight(
    git: Git,
    *,
    merge_subject: DbTask,
    merge_source_ref: str,
    current_branch: str,
    merge_preflight_target: str,
) -> ManualMergePreflightResult:
    if git.is_merged(merge_source_ref, current_branch):
        default_branch = git.default_branch()
        if current_branch != default_branch and not git.is_merged(merge_source_ref, default_branch):
            return ManualMergePreflightResult(
                ok=False,
                status="already_merged",
                message=(
                    f"Error: Branch '{merge_source_ref}' is already merged into current branch "
                    f"'{current_branch}', but still unmerged from default branch '{default_branch}'"
                ),
            )
        return ManualMergePreflightResult(
            ok=False,
            status="already_merged",
            message=f"Error: Branch '{merge_source_ref}' is already merged into {current_branch}",
        )

    if git.has_changes(include_untracked=False):
        return ManualMergePreflightResult(
            ok=False,
            status="dirty_checkout",
            message="Error: You have uncommitted changes. Please commit or stash them first.",
            block_reason="main checkout has uncommitted changes",
        )

    if not git.can_merge(merge_source_ref, merge_preflight_target):
        assert merge_subject.id is not None
        return ManualMergePreflightResult(
            ok=False,
            status="merge_conflict",
            message=(
                f"Error: Branch '{merge_source_ref}' has conflicts against '{merge_preflight_target}' "
                "and cannot be merged cleanly.\n"
                f"Run: uv run gza rebase {merge_subject.id} --resolve\n"
                f"Or preview the lifecycle action with: uv run gza advance {merge_subject.id} --dry-run"
            ),
            block_reason=f"branch '{merge_source_ref}' conflicts against '{merge_preflight_target}'",
        )

    return ManualMergePreflightResult(ok=True, status="ok")


def execute_manual_merge(
    request: ManualMergeExecutionRequest,
    hooks: ManualMergeExecutionHooks,
) -> ManualMergeExecutionResult:
    """Run the shared manual/landing merge mutation boundary."""

    preflight = check_manual_merge_preflight(
        request.git,
        merge_subject=request.merge_subject,
        merge_source_ref=request.merge_source_ref,
        current_branch=request.current_branch,
        merge_preflight_target=request.merge_preflight_target,
    )
    if not preflight.ok:
        if preflight.message:
            hooks.emit(preflight.message)
        if preflight.status == "dirty_checkout":
            return ManualMergeExecutionResult(
                rc=1,
                status="blocked_dirty_checkout",
                block_reason=preflight.block_reason,
            )
        if preflight.status == "merge_conflict":
            return ManualMergeExecutionResult(
                rc=1,
                status="merge_conflict",
                block_reason=preflight.block_reason,
            )
        return ManualMergeExecutionResult(
            rc=1,
            block_reason=preflight.block_reason,
        )

    created_deferred_blockers: list[DbTask] = []
    reused_deferred_blockers: list[DbTask] = []
    if request.materialize_side_effects:
        deferred_blockers = request.pre_materialized_deferred_blockers
        if deferred_blockers is None:
            deferred_blockers = hooks.materialize_deferred_blockers(request.merge_subject)
        if deferred_blockers is None:
            return ManualMergeExecutionResult(rc=1)
        created_deferred_blockers, reused_deferred_blockers = deferred_blockers
        if not request.pre_materialized_deferred_blockers_printed:
            hooks.print_deferred_blockers(request.merge_subject, deferred_blockers)

    try:
        pending_squash_reconcile = request.pending_squash_reconcile
        if not request.quiet_mechanics:
            hooks.emit(f"Merging '{request.merge_source_ref}' into '{request.current_branch}'...")

        commit_message = hooks.build_commit_message(request.merge_subject) if request.squash else None
        pre_squash_state = None
        if request.squash:
            pre_squash_state = hooks.capture_pre_squash_reconcile_state(
                request.git,
                request.merge_branch,
            )

        request.git.merge(
            request.merge_source_ref,
            squash=request.squash,
            commit_message=commit_message,
        )

        if request.squash:
            squash_oid = hooks.rev_parse_head(request.git)
            if squash_oid is not None and request.git.repo_dir == request.config.project_dir:
                hooks.print_squash_reconcile_result(
                    hooks.reconcile_squash_merge(
                        request.git,
                        request.merge_branch,
                        squash_oid,
                        pre_squash_state,
                    ),
                    request.quiet_mechanics,
                )
            elif squash_oid is not None:
                pending_squash_reconcile = pre_squash_state
            if not request.quiet_mechanics:
                hooks.emit(f"✓ Successfully squash merged {request.merge_source_ref} and created commit")
        elif not request.quiet_mechanics:
            hooks.emit(f"✓ Successfully merged {request.merge_source_ref}")

        if request.delete_branch:
            try:
                request.git.delete_branch(request.merge_branch)
                hooks.emit(f"✓ Deleted branch {request.merge_branch}")
            except GitError as exc:
                hooks.emit(f"Warning: Could not delete branch: {exc}")

        created_followups: list[DbTask] = []
        reused_followups: list[DbTask] = []
        if request.git.repo_dir == request.config.project_dir and request.materialize_side_effects:
            mark_merge_subject_merged(
                request.store,
                merge_subject=request.merge_subject,
                merge_unit_id=request.merge_unit_id,
                merge_source=request.merge_source,
            )
            if not request.no_followups:
                created_followups, reused_followups = hooks.materialize_followups(request.merge_subject)
                hooks.print_followups(request.merge_subject, (created_followups, reused_followups))
        return ManualMergeExecutionResult(
            rc=0,
            pending_squash_reconcile=pending_squash_reconcile,
            created_followups=created_followups,
            reused_followups=reused_followups,
            created_deferred_blockers=created_deferred_blockers,
            reused_deferred_blockers=reused_deferred_blockers,
        )
    except GitError as exc:
        operation = "merge"
        hooks.emit(
            f"Error during {operation} for {request.merge_subject.id} "
            f"(branch {request.merge_branch}): {exc}"
        )
        hooks.emit(
            f"\nAborting {operation} for {request.merge_subject.id} "
            f"(branch {request.merge_branch}) and restoring clean state..."
        )
        try:
            try:
                request.git.merge_abort()
                hooks.emit("✓ Merge aborted, working directory restored")
            except GitError:
                request.git.reset_hard_head()
                hooks.emit("✓ Merge cleanup reset tracked files to HEAD")
        except GitError as abort_error:
            hooks.emit(
                f"Warning: Could not abort {operation} for {request.merge_subject.id} "
                f"(branch {request.merge_branch}): {abort_error}"
            )
        return ManualMergeExecutionResult(rc=1)


def mark_merge_subject_merged(
    store: SqliteTaskStore,
    *,
    merge_subject: DbTask,
    merge_unit_id: str | None,
    merge_source: str = MERGE_SOURCE_MANUAL,
) -> None:
    assert merge_subject.id is not None
    if merge_unit_id is not None:
        store.set_merge_unit_state(
            merge_unit_id,
            "merged",
            merged_by_task_id=merge_subject.id,
            merge_source=merge_source,
        )
    else:
        store.set_merge_status(merge_subject.id, "merged")


def manual_force_merge_source(current_source: str) -> str:
    if current_source == MERGE_SOURCE_MANUAL:
        return MERGE_SOURCE_MANUAL_FORCE
    return current_source
