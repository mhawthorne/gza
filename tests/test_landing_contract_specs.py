"""Contract checks for the specified-but-not-yet-implemented landing coordinator."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    next_start = text.find("\n#### ", start + len(heading))
    if next_start == -1:
        next_start = text.find("\n### ", start + len(heading))
    return text[start:] if next_start == -1 else text[start:next_start]


def _squash(text: str) -> str:
    return re.sub(r"\s+", " ", text)


def test_landing_post_rebase_review_rules_are_scoped_to_review_enabled_lineages() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    prerequisites = _squash(_section(spec, "#### Deterministic prerequisites"))
    post_rebase = _squash(_section(spec, "#### Post-rebase review bound"))

    assert "apply only to these review-enabled lineages" in prerequisites
    assert "For review-enabled lineages" in post_rebase
    assert "provider-resolved" in post_rebase
    assert "changed diff, unknown diff, recovered/resumed rebase" in post_rebase
    assert "exactly one current-head review" in post_rebase

    review_disabled_index = post_rebase.index("For review-disabled lineages")
    review_disabled_rules = post_rebase[review_disabled_index:]
    assert (
        "MUST NOT create, run, reuse, or wait on a code review or resolution review"
        in review_disabled_rules
    )
    assert "MUST NOT create or run a landing judgment" in review_disabled_rules
    assert "current canonical green lifecycle verify evidence" in review_disabled_rules
    assert "ordinary no-review landing with `manual_land` provenance" in review_disabled_rules


def test_landing_materializes_followups_and_deferred_blockers_before_merge_state_mutation() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    final_preflight = _squash(
        _section(spec, "#### Deferred blockers, provenance, and final preflight")
    )

    assert "all ordinary `FOLLOWUP` tasks from the current review" in final_preflight
    assert "Every landing path" in final_preflight
    assert "before recording success whenever the current review contains" in final_preflight
    assert "urgent PR-required deferred-blocker" in final_preflight
    assert "preserve both urgent handling and PR-required semantics" in final_preflight
    assert "creation failure" in final_preflight
    assert "reuse-validation failure" in final_preflight
    assert "property-reconciliation failure" in final_preflight
    assert "persistence failure MUST block landing" in final_preflight
    assert "before any merge-state mutation" in final_preflight


def test_landing_transition_bound_has_named_policy_and_precedes_side_effects() -> None:
    overview = _read("specs/behavior/00-overview.md")
    spec = _read("specs/behavior/lifecycle-engine.md")
    dry_run_and_bounds = _squash(_section(spec, "#### Dry run, idempotency, and refusal output"))

    assert "writable landing transitions" in overview
    assert "named, swappable per-invocation transition-limit policy" in overview
    assert "`LandingTransitionLimitPolicy`" in dry_run_and_bounds
    assert "visited-state detector is the earlier non-progress guard" in dry_run_and_bounds
    assert "If the transition count is exhausted first" in dry_run_and_bounds
    assert "one precedence-compatible `LandBlocked` fact" in dry_run_and_bounds
    assert "regardless of whether every observed fingerprint was distinct" in dry_run_and_bounds
    assert "MUST NOT launch later provider work" in dry_run_and_bounds
    assert "create or reuse follow-up or deferred tasks" in dry_run_and_bounds
    assert "merge, or mark anything merged" in dry_run_and_bounds


def test_landing_dry_run_requires_identity_evidence_phases_and_first_unknown_boundary() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    docs = _read("docs/configuration.md")
    dry_run_and_bounds = _squash(_section(spec, "#### Dry run, idempotency, and refusal output"))

    for required in (
        "resolved owner",
        "local source",
        "canonical target",
        "current evidence",
        "ordered conditional phases",
        "first execution-required boundary",
    ):
        assert required in dry_run_and_bounds
        assert required in docs

    assert "MUST explicitly label that phase as conditional or unknown" in dry_run_and_bounds
    assert "instead of synthesizing later outcomes" in dry_run_and_bounds
    assert "without creating tasks, running providers, verifying, rebasing, or merging" in docs


def test_landing_scope_and_dependency_proof_precede_rebase_and_are_recomputed() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    phase_order = _squash(_section(spec, "### §8a — Operator-triggered land"))
    prerequisites = _squash(_section(spec, "#### Deterministic prerequisites"))

    ordered = [
        "resolve identity and active merge unit",
        "prove dependency readiness",
        "prove project scope from reliable changed-path inspection",
        "run or exact-reuse the one required rebase",
        "re-read and re-prove dependency readiness and project scope",
        "acquire or exact-reuse current lifecycle source verify",
        "acquire or exact-reuse required spec-coherence evidence",
        "acquire or exact-reuse the required code/resolution review",
    ]
    positions = [phase_order.index(item) for item in ordered]
    assert positions == sorted(positions)
    assert "Out-of-scope and scope-unverifiable branches MUST stop before rebase" in phase_order
    assert "reliable changed-path inspection for the exact live source head" in prerequisites
    assert "MUST be recomputed after every rebase or other source-head-changing step" in prerequisites
    assert "before verify, spec-coherence, code review, judgment" in prerequisites
    assert "All merge-required dependencies are proven satisfied" in prerequisites


def test_landing_writable_mode_requires_available_prerequisite_actions() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    land_section = _squash(_section(spec, "### §8a — Operator-triggered land"))
    prerequisites = _squash(_section(spec, "#### Deterministic prerequisites"))

    assert "MUST perform or exact-reuse deterministic prerequisites" in land_section
    assert "If the source does not contain the target tip, `land` MUST run or exact-reuse exactly one task-backed rebase" in prerequisites
    assert "If evidence is absent or stale, writable `land` MUST run or exact-reuse the shared direct verify acquisition path" in prerequisites
    assert "writable `land` MUST create/run the exact task through the shared launch route" in prerequisites
    assert "If an exact pending spec-coherence task exists, `land` MUST run or exact-reuse that task" in prerequisites
    assert "Missing or stale required code/resolution review evidence" in prerequisites
    assert "not a generic prerequisite-missing stop" in prerequisites
    assert "MUST create/run or exact-reuse the required plain-full or resolution review" in prerequisites

    for inability in (
        "acquisition disabled",
        "identity conflict",
        "launch/capacity failure",
        "terminal worker failure",
        "unavailable proof",
        "in-progress",
        "incompatible exact reuse",
        "exhausted invocation budget",
    ):
        assert inability in prerequisites


def test_landing_noop_rebase_outcomes_define_carry_refresh_and_fail_closed() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    post_rebase = _squash(_section(spec, "#### Post-rebase review bound"))

    assert "Every durable `no-op` subtype MUST carry explicit proof" in post_rebase
    assert "`no-op:already-contained` MAY preserve an eligible current review only when" in post_rebase
    assert "exact attempted source-head identity" in post_rebase
    assert "exact attempted target-head identity" in post_rebase
    assert "exact target-tip containment" in post_rebase
    assert "`changed_diff == false`" in post_rebase
    assert "`provider_conflict_resolution == false`" in post_rebase
    assert "`no-op:superseded-contained` MAY proceed without another rebase only when" in post_rebase
    assert "MAY preserve existing review evidence only when the same exact source-head identity" in post_rebase
    assert "otherwise it MUST obtain the single current-head review" in post_rebase
    assert "missing, mismatched, prose-only, unsupported, or ambiguous proof" in post_rebase
    assert "MUST fail closed with a named `LandBlocked` reason" in post_rebase
    assert "MUST refresh through the single current-head review path" in post_rebase


def test_landing_execution_status_and_merge_state_are_separate_predicates() -> None:
    overview = _read("specs/behavior/00-overview.md")
    spec = _read("specs/behavior/lifecycle-engine.md")
    vocabulary = _squash(_section(overview, "## Vocabulary (the data model, abstractly)"))
    merge_rules = _squash(_section(spec, "### §8 — Merge"))
    prerequisites = _squash(_section(spec, "#### Deterministic prerequisites"))

    assert "Compatibility execution status" in vocabulary
    assert "`status=\"unmerged\"`" in vocabulary
    assert "not the canonical merge state" in vocabulary
    assert "owning work unit's merge state independently" in vocabulary
    assert "owning merge unit is independently unmerged" in merge_rules
    assert "compatibility task execution status `unmerged`" in merge_rules
    assert "Pending, in-progress, and failed representatives MUST be rejected" in merge_rules
    assert "owning merge unit is independently in merge state `unmerged`" in prerequisites
    assert "A pending, in-progress, failed, missing, or ambiguous representative is rejected" in prerequisites


def test_landing_landblocked_is_typed_total_and_precedence_ordered() -> None:
    overview = _read("specs/behavior/00-overview.md")
    spec = _read("specs/behavior/lifecycle-engine.md")
    table = _squash(_section(overview, "## Operator command-refusal table"))
    dry_run_and_bounds = _squash(_section(spec, "#### Dry run, idempotency, and refusal output"))

    for required in ("`reason_code`", "`fact`", "`evidence_refs`"):
        assert required in dry_run_and_bounds
        assert required in table

    expected_order = [
        "`identity-proof-unavailable`",
        "`dirty-checkout`",
        "`rebase-or-conflict`",
        "`verify-unavailable-or-red`",
        "`required-review-unavailable`",
        "`nondeferrable-blocker`",
        "`policy-or-judge-refused`",
        "`materialization-or-persistence-failed`",
        "`bounded-attempt-exhausted`",
        "`merge-failed`",
    ]
    positions = [dry_run_and_bounds.index(f"{index}. {reason}") for index, reason in enumerate(expected_order, 1)]
    assert positions == sorted(positions)

    assert "When multiple refusal facts are simultaneously true" in dry_run_and_bounds
    assert "perform no lower-precedence side effects" in dry_run_and_bounds
    assert "Repeated-state, transition-cap, follow-up/deferred materialization, reuse-validation, and persistence failures" in dry_run_and_bounds
    assert "not generic \"prerequisite missing\" stops" in dry_run_and_bounds
    assert "exactly one sentence" in dry_run_and_bounds
    assert "Cannot land <task-id>: <fact>." in dry_run_and_bounds


def test_guarded_changes_requested_followups_are_urgent_and_pr_required() -> None:
    spec = _read("specs/behavior/lifecycle-engine.md")
    docs = _read("docs/configuration.md")
    final_preflight = _squash(
        _section(spec, "#### Deferred blockers, provenance, and final preflight")
    )

    assert "Ordinary `FOLLOWUP` findings materialized from a guarded `CHANGES_REQUESTED` escalation" in final_preflight
    assert "each such follow-up MUST be urgent and PR-required" in final_preflight
    assert "Exact-key reuse of a pre-existing ordinary follow-up from that guarded escalation MUST validate both properties" in final_preflight
    assert "reconcile them before merge" in final_preflight
    assert "if either property is missing and cannot be reconciled, landing MUST refuse" in final_preflight
    assert "Ordinary follow-up semantics for non-escalated `APPROVED_WITH_FOLLOWUPS` paths remain unchanged" in final_preflight
    assert "Guarded `CHANGES_REQUESTED` escalation creates or reuses both deferred `BLOCKER` tasks and ordinary `FOLLOWUP` tasks as urgent PR-required work" in docs
