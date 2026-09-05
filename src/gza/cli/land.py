"""CLI adapter for operator-triggered landing."""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from typing import Any

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
        landing_merge_authorization_from_facts,
    )
    from gza.landing_judge import (
        LandingJudgeBlockerIdentity,
        LandingJudgeBlockerInput,
        LandingJudgeDecisionContext,
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
            return LandingJudgment("BLOCK")
        identity = next(reversed(latest_identity.values()))
        facts = latest_facts.get(identity.owner_task_id)
        if facts is None or facts.review is None or facts.verify is None or not facts.review.review_id:
            return LandingJudgment("BLOCK")
        review_task = store.get(facts.review.review_id)
        if review_task is None:
            return LandingJudgment("BLOCK")
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
            return LandingJudgment("BLOCK")
        blocker_inputs = tuple(
            LandingJudgeBlockerInput.from_open_blocker(
                blocker,
                title=findings_by_fingerprint[blocker.fingerprint].title,
                body=findings_by_fingerprint[blocker.fingerprint].body,
                evidence=(findings_by_fingerprint[blocker.fingerprint].evidence or blocker.source or "review evidence",),
                open_state_citations=(
                    findings_by_fingerprint[blocker.fingerprint].open_state_citation
                    or blocker.source
                    or "review open-state citation",
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
        adjudication_content_identity = (
            "sha256:" + sha256("\n".join(sorted(facts.adjudication_fingerprints)).encode()).hexdigest()
            if facts.adjudication_fingerprints
            else "proven-empty"
        )
        review_scope = review_task.review_scope or f"plain_full review {review_task.id}"
        review_output = get_review_content(config.project_dir, review_task) or review_task.output_content or ""
        diff_context = _landing_diff_context(git, identity)
        context = LandingJudgeDecisionContext.from_inputs(
            task_prompt=identity.owner_task.prompt,
            authoritative_review_scope=review_scope,
            plan_context=identity.owner_task.prompt,
            implementation_summary=identity.owner_task.output_content or identity.owner_task.prompt,
            review_output=review_output,
            verify_evidence=verify_identity,
            diff_context=diff_context,
            adjudication_context="\n".join(facts.adjudication_fingerprints) or "proven-empty",
            blockers=blocker_inputs,
        )
        try:
            judge_identity = LandingJudgeIdentity(
                implementation_id=identity.owner_task_id,
                merge_unit_id=identity.merge_unit_id,
                review_id=facts.review.review_id,
                reviewed_head=facts.review.reviewed_head,
                source_head=identity.source_sha,
                target_head=identity.target_sha,
                verify_identity=verify_identity,
                authoritative_scope_identity="sha256:" + sha256(review_scope.encode()).hexdigest(),
                adjudication_artifact_identities=facts.adjudication_fingerprints,
                adjudication_content_identity=adjudication_content_identity,
                blocker_identities=tuple(
                    LandingJudgeBlockerIdentity(blocker.finding_id, blocker.fingerprint or "")
                    for blocker in facts.open_blockers
                ),
                decision_context_digest=context.digest,
            )
            prompt = build_landing_judge_prompt(
                identity=judge_identity,
                task_prompt=context.task_prompt,
                authoritative_review_scope=context.authoritative_review_scope,
                plan_context=context.plan_context,
                implementation_summary=context.implementation_summary,
                review_output=context.review_output,
                verify_evidence=context.verify_evidence,
                diff_context=context.diff_context,
                adjudication_context=context.adjudication_context,
                blockers=context.blockers,
            )
            result = obtain_landing_judgment(
                store=store,
                config=config,
                owner_task=identity.owner_task,
                review_task=review_task,
                identity=judge_identity,
                blockers=blocker_inputs,
                prompt=prompt,
                runner=runner,
                trigger_source="manual_land",
            )
            return result.judgment
        except Exception:
            return LandingJudgment("BLOCK")

    def execute_land_merge(identity: Any, decision: Any, provenance: str) -> ManualMergeExecutionResult:
        facts = latest_facts.get(identity.owner_task_id)
        if facts is None:
            return ManualMergeExecutionResult(
                rc=1,
                status="landing_authorization_changed",
                block_reason="landing policy facts are unavailable before merge",
            )
        authorization = landing_merge_authorization_from_facts(identity=identity, facts=facts, decision=decision)
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
            load_landing_authorization=lambda: _current_landing_authorization(coordinator, identity, decision),
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
    getter = getattr(git, "get_diff_stat", None)
    if callable(getter) and identity.source_ref and identity.target_branch:
        try:
            diff_stat = getter(f"{identity.target_branch}...{identity.source_ref}")
            if isinstance(diff_stat, str) and diff_stat.strip():
                return diff_stat.strip()
        except Exception:
            pass
    return f"source={identity.source_sha or 'unknown'} target={identity.target_sha or 'unknown'}"


def _current_landing_authorization(coordinator: Any, identity: Any, decision: Any) -> Any:
    from gza.landing import landing_merge_authorization_from_facts

    inspect_policy_facts = coordinator.inspect_policy_facts
    coordinator.inspect_policy_facts = None
    try:
        facts = coordinator._landing_policy_facts(identity)
    finally:
        coordinator.inspect_policy_facts = inspect_policy_facts
    return landing_merge_authorization_from_facts(identity=identity, facts=facts, decision=decision)


def _review_finding_fingerprint(finding: ReviewFinding) -> str | None:
    from gza.review_verdict import get_review_finding_fingerprint

    fingerprint = get_review_finding_fingerprint(finding)
    if fingerprint is None:
        return None
    title, anchor = fingerprint
    return json.dumps({"title": title, "anchor": anchor}, sort_keys=True, separators=(",", ":"))
