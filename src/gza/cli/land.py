"""CLI adapter for operator-triggered landing."""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from typing import Any, cast

from gza.cli._common import get_store, resolve_id
from gza.config import Config
from gza.git import Git
from gza.merge_services import ManualMergeExecutionResult, ResolvedMergeSubject
from gza.review_verdict import ReviewFinding, get_review_content


def cmd_land(args: argparse.Namespace) -> int:
    """Resolve and plan an operator-triggered landing attempt."""

    from gza import runner as runner_mod
    from gza.cli._common import _create_rebase_task
    from gza.cli.git_ops import _merge_single_task, _run_task_backed_rebase
    from gza.landing import (
        LANDING_POLICIES,
        LandingCoordinator,
        LandingJudgment,
        LandRequest,
    )
    from gza.landing_judge import (
        LandingJudgeBlockerIdentity,
        LandingJudgeIdentity,
        build_landing_judge_prompt,
        obtain_landing_judgment,
    )

    config = Config.load(args.project_dir)
    store = get_store(config, open_mode="query_only" if args.dry_run else "readwrite")
    git = Git(config.project_dir)
    task_id = resolve_id(config, args.task_id)
    policy = args.policy
    if policy not in LANDING_POLICIES:
        print(f"Error: unknown landing policy {policy!r}")
        return 2

    latest_identity: dict[str, Any] = {}
    latest_facts: dict[str, Any] = {}

    def runner(config: Config, task_id: str) -> int:
        return runner_mod.run(config, task_id=task_id)

    def inspect_policy_facts(identity: Any) -> Any:
        coordinator.inspect_policy_facts = None
        try:
            facts = coordinator._landing_policy_facts(identity)
        finally:
            coordinator.inspect_policy_facts = inspect_policy_facts
        latest_identity[identity.owner_task_id] = identity
        latest_facts[identity.owner_task_id] = facts
        return facts

    def durable_judge() -> LandingJudgment:
        if not latest_identity:
            return LandingJudgment("BLOCK", blocking_fact="landing identity is unavailable for guarded judgment")
        identity = next(reversed(latest_identity.values()))
        facts = latest_facts.get(identity.owner_task_id)
        if facts is None or facts.review is None or facts.verify is None or not facts.review.review_id:
            return LandingJudgment("BLOCK", blocking_fact="landing policy facts are incomplete for guarded judgment")
        review_task = store.get(facts.review.review_id)
        if review_task is None:
            return LandingJudgment("BLOCK", blocking_fact=f"current review {facts.review.review_id} is unavailable")
        try:
            evidence = _landing_judge_evidence(store, config, git, identity, facts, review_task)
            judge_identity = LandingJudgeIdentity(
                implementation_id=identity.owner_task_id,
                merge_unit_id=identity.merge_unit_id,
                review_id=facts.review.review_id,
                reviewed_head=facts.review.reviewed_head,
                source_head=identity.source_sha,
                target_head=identity.target_sha,
                verify_identity=evidence["verify_identity"],
                authoritative_scope_identity=evidence["authoritative_scope_identity"],
                adjudication_artifact_identities=facts.adjudication_fingerprints,
                adjudication_content_identity=evidence["adjudication_content_identity"],
                blocker_identities=tuple(
                    LandingJudgeBlockerIdentity(blocker.finding_id, blocker.fingerprint or "")
                    for blocker in facts.open_blockers
                ),
                decision_context_digest=evidence["context"].digest,
            )
            prompt = build_landing_judge_prompt(
                identity=judge_identity,
                task_prompt=evidence["context"].task_prompt,
                authoritative_review_scope=evidence["context"].authoritative_review_scope,
                plan_context=evidence["context"].plan_context,
                implementation_summary=evidence["context"].implementation_summary,
                review_output=evidence["context"].review_output,
                verify_evidence=evidence["context"].verify_evidence,
                diff_context=evidence["context"].diff_context,
                adjudication_context=evidence["context"].adjudication_context,
                blockers=evidence["context"].blockers,
            )
            result = obtain_landing_judgment(
                store=store,
                config=config,
                owner_task=identity.owner_task,
                review_task=review_task,
                identity=judge_identity,
                blockers=evidence["blocker_inputs"],
                prompt=prompt,
                runner=runner,
                trigger_source="manual_land",
            )
            if result.fail_closed_reason:
                return LandingJudgment("BLOCK", blocking_fact=result.fail_closed_reason)
            return result.judgment
        except Exception as exc:
            return LandingJudgment("BLOCK", blocking_fact=f"guarded landing judgment failed: {_exception_identity(exc)}")

    def execute_land_merge(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        authorization = _current_landing_authorization(coordinator, identity, decision, policy=policy)
        if authorization is None:
            return ManualMergeExecutionResult(
                rc=1,
                status="landing_authorization_changed",
                block_reason="landing policy facts are unavailable before merge",
            )
        merge_args = argparse.Namespace(
            mark_only=False,
            squash=False,
            delete=False,
            force=False,
            ignore_verify_gate=False,
            defer_blockers=False,
            no_followups=False,
        )
        merge_result = _merge_single_task(
            identity.owner_task_id,
            config,
            store,
            git,
            merge_args,
            identity.target_branch,
            merge_preflight_ref=identity.target_branch,
            merge_source=provenance,
            quiet_mechanics=True,
            landing_authorization=authorization,
            load_landing_authorization=lambda: _current_landing_authorization(coordinator, identity, decision, policy=policy),
            resolved_subject=ResolvedMergeSubject(
                trigger_task=identity.owner_task,
                execution_task=identity.representative_task,
                merge_subject=identity.owner_task,
                merge_unit_id=identity.merge_unit_id,
                merge_branch=identity.source_branch,
                merge_source_ref=identity.source_ref,
                merge_source_warning=None,
                merge_member_tasks=tuple(
                    task for task_id in identity.member_task_ids if (task := store.get(task_id)) is not None
                ),
            ),
        )
        return ManualMergeExecutionResult(
            rc=merge_result.rc,
            status=merge_result.status,
            block_reason=merge_result.block_reason,
            created_followups=list(merge_result.created_followups),
            reused_followups=list(merge_result.reused_followups),
            created_deferred_blockers=list(merge_result.created_deferred_blockers),
            reused_deferred_blockers=list(merge_result.reused_deferred_blockers),
        )

    coordinator = LandingCoordinator(
        store=store,
        git=git,
        config=config,
        create_rebase_task=_create_rebase_task,
        rebase_executor=_run_task_backed_rebase,
        inspect_policy_facts=inspect_policy_facts,
        landing_judge=durable_judge,
        execute_merge=execute_land_merge,
    )
    result = coordinator.run(LandRequest(task_id=task_id, policy=policy, dry_run=bool(args.dry_run)))
    for step in result.steps:
        print(f"{step.phase}: {step.status} - {step.summary}")

    if result.blocked is not None:
        print(result.blocked.terminal_sentence(task_id))
        return 1
    if result.already_merged:
        print(
            f"Already landed {task_id}: owner {result.owner_task_id} "
            f"on {result.source_ref} -> {result.target_branch}."
        )
        return 0
    if args.dry_run:
        print(
            f"Dry run for {task_id}: owner {result.owner_task_id} "
            f"on {result.source_ref} -> {result.target_branch}; "
            "later outcomes stop at the first execution-required boundary."
        )
        return 0
    if result.merged:
        print(
            f"Landed {task_id}: owner {result.owner_task_id} -> {result.target_branch} "
            f"with {result.merge_provenance} provenance."
        )
        return 0
    print(f"Cannot land {task_id}: landing stopped before a terminal result.")
    return 1


def _landing_diff_context(git: Any, identity: Any) -> str:
    if not identity.source_ref or not identity.target_branch:
        raise ValueError("landing diff context requires exact source and target refs")
    revision_range = f"{identity.target_branch}...{identity.source_ref}"
    getter = getattr(git, "get_diff", None)
    if not callable(getter):
        raise ValueError("git diff reader is unavailable")
    diff = getter(revision_range)
    if not isinstance(diff, str):
        raise ValueError("git diff reader returned non-text evidence")
    stripped = diff.strip()
    if stripped:
        return stripped
    stat_getter = getattr(git, "get_diff_stat", None)
    if callable(stat_getter):
        diff_stat = stat_getter(revision_range)
        if isinstance(diff_stat, str) and diff_stat.strip():
            return f"Diff patch is empty.\n\nDiff stat:\n{diff_stat.strip()}"
    return "Diff patch is empty."


def _landing_judge_evidence(store: Any, config: Any, git: Any, identity: Any, facts: Any, review_task: Any) -> dict[str, Any]:
    from gza.landing import (
        _inspect_authoritative_landing_scope_identity,
        _landing_current_adjudication_records,
    )
    from gza.landing_judge import LandingJudgeBlockerInput, LandingJudgeDecisionContext

    parsed_findings = tuple(
        finding
        for finding in getattr(facts.review, "_parsed_blocker_findings", ())
        if isinstance(finding, ReviewFinding)
    )
    findings_by_fingerprint = {
        blocker.fingerprint: finding
        for blocker in facts.open_blockers
        for finding in parsed_findings
        if blocker.fingerprint and _review_finding_fingerprint(finding) == blocker.fingerprint
    }
    if len(findings_by_fingerprint) != len(facts.open_blockers):
        raise ValueError("current blocker findings do not match landing policy facts")
    blocker_inputs = tuple(
        LandingJudgeBlockerInput.from_open_blocker(
            blocker,
            title=findings_by_fingerprint[blocker.fingerprint].title,
            body=findings_by_fingerprint[blocker.fingerprint].body,
            evidence=(findings_by_fingerprint[blocker.fingerprint].evidence or blocker.source or "review evidence",),
            open_state_citations=(
                findings_by_fingerprint[blocker.fingerprint].open_state_citation
                or blocker.source
                or "review open-state citation"
            ),
            impact=findings_by_fingerprint[blocker.fingerprint].impact or "review impact unavailable",
            required_fix=findings_by_fingerprint[blocker.fingerprint].fix_or_followup
            or "review required fix unavailable",
        )
        for blocker in facts.open_blockers
        if blocker.fingerprint in findings_by_fingerprint
    )
    verify_identity = json.dumps(
        {
            "epoch": facts.verify.epoch,
            "gate": facts.verify.gate_identity,
            "tree": facts.verify.tree_fingerprint,
            "verdict": facts.verify.status,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    adjudication_records = _landing_current_adjudication_records(store, facts.review, identity=identity)
    adjudication_context = "\n".join(adjudication_records) or "proven-empty"
    adjudication_content_identity = (
        "sha256:" + sha256(adjudication_context.encode()).hexdigest()
        if adjudication_records
        else "proven-empty"
    )
    review_scope = review_task.review_scope or f"{facts.review.mode} review {review_task.id}"
    authoritative_scope_identity = _inspect_authoritative_landing_scope_identity(store, facts.review)
    if not authoritative_scope_identity:
        raise ValueError("authoritative review scope identity is unavailable")
    review_output = get_review_content(config.project_dir, review_task) or review_task.output_content or ""
    context = LandingJudgeDecisionContext.from_inputs(
        task_prompt=identity.owner_task.prompt,
        authoritative_review_scope=review_scope,
        plan_context=_landing_plan_context(store, identity.owner_task),
        implementation_summary=identity.owner_task.output_content or identity.owner_task.prompt,
        review_output=review_output,
        verify_evidence=verify_identity,
        diff_context=_landing_diff_context(git, identity),
        adjudication_context=adjudication_context,
        blockers=blocker_inputs,
    )
    return {
        "blocker_inputs": blocker_inputs,
        "verify_identity": verify_identity,
        "adjudication_content_identity": adjudication_content_identity,
        "authoritative_scope_identity": authoritative_scope_identity,
        "context": context,
    }


def _landing_plan_context(store: Any, owner_task: Any) -> str:
    refs = []
    for ref_id in (getattr(owner_task, "depends_on", None), getattr(owner_task, "based_on", None)):
        if not ref_id:
            continue
        try:
            ref = store.get(ref_id)
        except Exception:
            ref = None
        if ref is not None:
            refs.append(f"{ref.id} [{ref.task_type}]: {ref.prompt}")
    refs.append(f"{owner_task.id} [{owner_task.task_type}]: {owner_task.prompt}")
    return "\n".join(refs)


def _current_landing_authorization(coordinator: Any, identity: Any, decision: Any, *, policy: str) -> Any:
    from gza.landing import (
        LandBlocked,
        LandingJudgment,
        LandingPolicyName,
        LandRequest,
        evaluate_landing_policy,
        landing_merge_authorization_from_facts,
    )

    landing_policy = cast(LandingPolicyName, policy)
    inspect_policy_facts = coordinator.inspect_policy_facts
    coordinator.inspect_policy_facts = None
    try:
        current_identity = coordinator._resolve_identity(
            LandRequest(task_id=identity.owner_task_id, policy=landing_policy),
            persist_reconciliation=False,
        )
        if isinstance(current_identity, LandBlocked):
            return None
        facts = coordinator._landing_policy_facts(current_identity)
    finally:
        coordinator.inspect_policy_facts = inspect_policy_facts
    if decision.allowed_overrides:
        if not decision.judgment_artifact_id or not decision.judgment_key:
            return None
        refreshed_decision = evaluate_landing_policy(
            policy="guarded",
            facts=facts,
            judge=lambda: LandingJudgment("LAND", artifact_id=decision.judgment_artifact_id, key=decision.judgment_key),
        )
    else:
        refreshed_decision = evaluate_landing_policy(policy=landing_policy, facts=facts, judge=None)
    if not refreshed_decision.allowed:
        return None
    return landing_merge_authorization_from_facts(identity=current_identity, facts=facts, decision=refreshed_decision)


def _review_finding_fingerprint(finding: ReviewFinding) -> str | None:
    from gza.review_verdict import get_review_finding_fingerprint

    fingerprint = get_review_finding_fingerprint(finding)
    if fingerprint is None:
        return None
    title, anchor = fingerprint
    return json.dumps({"title": title, "anchor": anchor}, sort_keys=True, separators=(",", ":"))


def _exception_identity(exc: Exception) -> str:
    message = " ".join(str(exc).replace("\r", "\n").split()).strip()
    return message or exc.__class__.__name__
