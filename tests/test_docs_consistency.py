"""Regression checks for canonical operator docs."""

import re
from pathlib import Path


def _normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def _extract_markdown_table_rows(document: str, heading: str) -> list[tuple[str, str]]:
    """Return normalized condition/action rows from the Markdown table under a heading."""
    lines = document.splitlines()
    start_index: int | None = None
    heading_line = heading.strip()
    for index, line in enumerate(lines):
        if line.strip() == heading_line:
            start_index = index + 1
            break

    assert start_index is not None, f"Heading not found: {heading}"

    rows: list[tuple[str, str]] = []
    for line in lines[start_index:]:
        stripped = line.strip()
        if not stripped:
            if rows:
                break
            continue
        if not stripped.startswith("|"):
            if rows:
                break
            continue

        match = re.match(r"^\|\s*(.*?)\s*\|\s*(.*?)\s*\|$", stripped)
        if match is None:
            continue
        cells = [match.group(1), match.group(2)]
        if set(cells[0]) == {"-"} and set(cells[1]) == {"-"}:
            continue

        rows.append(tuple(_normalize_whitespace(cell) for cell in cells))

    assert rows, f"No table rows found under heading: {heading}"
    return rows


def _find_markdown_table_row(
    rows: list[tuple[str, str]], *, condition_contains: str
) -> tuple[int, str, str]:
    normalized_needle = _normalize_whitespace(condition_contains)
    for index, (condition, action) in enumerate(rows):
        if normalized_needle in condition:
            return index, condition, action
    raise AssertionError(f"Table row not found for condition: {condition_contains}")


def test_importer_cleanup_has_no_stale_references_in_operator_surfaces() -> None:
    """Tracked operator-facing surfaces should not refer to the removed importer module."""
    repo_root = Path(__file__).resolve().parents[1]
    stale_module_name = "importer" ".py"
    roots = [
        repo_root / "src",
        repo_root / "tests",
        repo_root / "docs",
        repo_root / "src" / "gza" / "skills",
        # Exclude .claude/skills: it is gitignored, per-worktree install state rather than
        # tracked repo source, so scanning it would make this regression depend on stale local
        # artifacts that cannot be fixed in a commit.
        repo_root / "scripts",
        repo_root / "specs",
        repo_root / "etc",
    ]

    assert repo_root / ".claude" not in roots

    stale_references: list[str] = []
    for root in roots:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            content = path.read_text(errors="ignore")
            if stale_module_name in content:
                stale_references.append(str(path.relative_to(repo_root)))

    assert not stale_references, (
        f"Found stale {stale_module_name} references in tracked operator-facing surfaces: "
        + ", ".join(stale_references)
    )


def test_docs_task_type_use_internal_not_learn() -> None:
    """Docs should reflect internal task type in authoritative task-type lists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    learnings_content = (docs_root / "internal" / "learnings.md").read_text()

    # configuration.md should list internal in task type filters
    assert "explore`, `plan`, `plan_review`, `plan_improve`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal`" in config_content

    # learnings doc should describe internal task mechanics
    assert "skip_learnings=True" in learnings_content
    assert "`gza history --type internal`" in learnings_content

    # Stale "learn" references should not appear
    for content in (config_content, learnings_content):
        assert "--type learn" not in content
        assert "A `learn` task is created" not in content


def test_agents_document_pytest_hang_triage_rules() -> None:
    """AGENTS.md should tell operators to treat silent pytest runs as suspect."""
    repo_root = Path(__file__).resolve().parents[1]
    agents_content = (repo_root / "AGENTS.md").read_text()

    required_snippets = [
        "## Pytest hangs",
        "If `uv run pytest tests/` produces no new output for about 2 minutes, kill it and bisect by file or class.",
        "CPU usage is a poor liveness signal because an infinite loop also pegs a core.",
        "the mock must also mark the spawned task complete or the loop spins forever",
        "test_iterate_failed_improve_non_attention_skip_does_not_emit_needs_attention",
    ]
    for snippet in required_snippets:
        assert snippet in agents_content


def test_behavior_specs_cross_link_watch_supervisor_boundary() -> None:
    """Behavior-spec index and engine overview should keep the supervisor boundary explicit."""
    repo_root = Path(__file__).resolve().parents[1]
    behavior_readme = (repo_root / "specs" / "behavior" / "README.md").read_text()
    overview = (repo_root / "specs" / "behavior" / "00-overview.md").read_text()
    engine = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    supervisor = (repo_root / "specs" / "behavior" / "watch-supervisor.md").read_text()

    assert "[watch-supervisor.md](watch-supervisor.md)" in behavior_readme
    assert "slot accounting, detached-worker adoption, drift restart, and pass ordering live" in overview
    assert 'The pass-ordering invariant "land fresh code first" is owned by' in overview
    assert "Cycle cadence, slot accounting, detached-worker adoption, and watch-process restart are" in engine
    assert "## Boundary with the engine" in supervisor
    assert "Installed-code drift triggers re-exec at the next" in supervisor


def test_main_verify_self_heal_contract_is_part_of_behavior_spec_set() -> None:
    """The red-main convergence contract should stay indexed and cross-linked into the runtime specs."""
    repo_root = Path(__file__).resolve().parents[1]
    behavior_readme = (repo_root / "specs" / "behavior" / "README.md").read_text()
    lifecycle = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    supervisor = (repo_root / "specs" / "behavior" / "watch-supervisor.md").read_text()
    contract = (repo_root / "specs" / "behavior" / "main-verify-self-heal.md").read_text()
    contract_flat = " ".join(contract.split())

    assert "[main-verify-self-heal.md](main-verify-self-heal.md)" in behavior_readme
    assert "North-star convergence contract for red local-target integration verify" in behavior_readme

    assert "### MV1 — Red verify state MUST converge" in contract
    assert "### MV2 — Red verdicts MUST be re-verified before automation acts on them" in contract
    assert "### MV3 — Red checkpoints MUST have a bounded lifetime even on an unchanged tree" in contract
    assert "### MV4 — Confirmed deterministic red MUST trigger bounded repair plus alert" in contract
    assert "### MV5 — Red merge freezes MUST NOT hard-park downstream work" in contract
    assert "### MV6 — Operators MUST have a force-refresh escape hatch" in contract
    assert "A merge stall MUST NOT convert into a launch stall." in contract
    assert "The shared no-progress backstop MUST count only actually executed unchanged actions." in contract
    assert "Remediation task dedup is by failure identity" in contract
    assert "failure signature plus the exact local-target tree fingerprint" in contract
    assert "fall back to signature-only reuse" in contract
    assert "There MUST be a first-class operator command that forces a fresh local-target verify run" in contract
    assert "Future behavior-check findings against this area MUST classify implementation drift" in contract_flat

    assert "[main-verify-self-heal.md](main-verify-self-heal.md)" in lifecycle
    assert "[main-verify-self-heal.md](main-verify-self-heal.md)" in supervisor
    assert "main-integration-verify-red" in lifecycle


def test_off_topic_verify_contract_is_indexed_and_cross_linked() -> None:
    """The off-topic verify unblock contract should stay discoverable and linked from lifecycle."""
    repo_root = Path(__file__).resolve().parents[1]
    behavior_readme = (repo_root / "specs" / "behavior" / "README.md").read_text()
    lifecycle = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    contract = (repo_root / "specs" / "behavior" / "off-topic-verify-failures.md").read_text()
    contract_flat = " ".join(contract.split())

    assert "[off-topic-verify-failures.md](off-topic-verify-failures.md)" in behavior_readme
    assert "Verify-only off-topic unblock contract" in behavior_readme

    assert "[off-topic-verify-failures.md](off-topic-verify-failures.md)" in lifecycle
    assert "`advance_off_topic_verify_unblock`" in lifecycle
    assert "With `advance_off_topic_verify_unblock` off, lifecycle MUST keep the blocker" in lifecycle
    assert "the full failing-node set was enumerated" in lifecycle
    assert "exact reviewed head SHA and exact tree fingerprint" in lifecycle
    assert "`REPRODUCE-OR-RECORD` investigation record" in lifecycle

    required_contract_snippets = [
        "The lifecycle policy knob `advance_off_topic_verify_unblock` MUST exist and MUST default to **off**.",
        "Lifecycle MUST enumerate the full failing-node set before classifying a red verify result as off-topic.",
        "A failing node is deterministic off-topic only when the same node also fails on the canonical local merge target",
        "A failing node is intermittent off-topic only when all of the following are true:",
        "One branch-introduced or unknown node keeps the entire review blocking",
        "the clearance MUST be bound to the exact reviewed head SHA and exact tree fingerprint",
        "Every off-topic clearance MUST create or reuse exactly one non-blocking investigation",
        "The investigation contract is `REPRODUCE-OR-RECORD`",
    ]
    for snippet in required_contract_snippets:
        assert snippet in contract_flat

    assert "Trusted green evidence MUST be runner-owned or otherwise durably recorded lifecycle evidence." in contract_flat
    assert "If exact reviewed-head or exact tree-fingerprint matching cannot be established" in contract_flat
    assert "If lifecycle cannot durably create or reuse the required investigation record, it MUST fail closed" in contract_flat
    assert (
        "the target-side stress baseline is conclusive only when it reproduces the same normalized "
        "failing-node identity plus the same normalized failure signature"
    ) in contract_flat
    assert "The stress baseline is inconclusive and MUST keep the review blocking when the local target stays green" in contract_flat
    assert "reproduces a different node identity or failure signature" in contract_flat
    assert "times out without parseable same-signature evidence" in contract_flat
    assert "otherwise yields unparseable or ambiguous results" in contract_flat
    assert "fail closed" in contract_flat


def test_off_topic_verify_lifecycle_wording_stays_reconciled_with_focused_contract() -> None:
    """Lifecycle wording should not contradict the focused off-topic verify contract."""
    repo_root = Path(__file__).resolve().parents[1]
    lifecycle = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    lifecycle_flat = " ".join(lifecycle.split())
    contract = (repo_root / "specs" / "behavior" / "off-topic-verify-failures.md").read_text()
    contract_flat = " ".join(contract.split())

    assert (
        "The exception is any knob whose focused contract explicitly makes its default part of the "
        "safety boundary."
    ) in lifecycle_flat
    assert (
        "`advance_off_topic_verify_unblock` is one such exception because "
        "[off-topic-verify-failures.md](off-topic-verify-failures.md) requires the knob to exist "
        "and default to **off**."
    ) in lifecycle_flat
    assert "The *values* above are non-normative defaults." not in lifecycle_flat

    assert (
        "Verify-only blocker clearing remains governed by the verify-only rules above: rule A "
        "for same-head runner-owned green recapture and rule A2 for the audited off-topic "
        "unblock contract."
    ) in lifecycle_flat
    assert "Verify-only blocker clearing remains governed exclusively by rule A above." not in lifecycle_flat

    assert "The lifecycle policy knob `advance_off_topic_verify_unblock` MUST exist and MUST default to **off**." in contract_flat


def test_main_verify_remediation_identity_docs_match_fingerprint_aware_runtime_contract() -> None:
    """Specs and operator docs should describe fingerprint-aware remediation dedup consistently."""
    repo_root = Path(__file__).resolve().parents[1]
    contract = (repo_root / "specs" / "behavior" / "main-verify-self-heal.md").read_text()
    supervisor = (repo_root / "specs" / "behavior" / "watch-supervisor.md").read_text()
    config_docs = (repo_root / "docs" / "configuration.md").read_text()

    assert "failure signature plus the exact local-target tree fingerprint" in contract
    assert "fall back to signature-only reuse" in contract

    assert "dedup is by failure identity: failure" in supervisor
    assert "signature plus exact local-target tree fingerprint" in supervisor
    assert "If the tree fingerprint is unavailable, watch MUST" in supervisor
    assert "fall back to signature-only reuse" in supervisor

    assert "deduplicated by failure signature plus tree fingerprint" in config_docs
    assert "If the tree fingerprint is unavailable, watch falls back to signature-only reuse" in config_docs
    assert "main-integration-verify-red" in supervisor
    assert "S7 — Watch owns bounded stateful work creation." in supervisor
    assert "advance` MAY surface the red-main condition from the shared state" in supervisor
    assert "create these remediation tasks itself." in supervisor


def test_worktree_isolation_contract_and_internal_docs_stay_aligned() -> None:
    """Behavior spec and internal docs should agree on isolated rebase git ownership."""
    repo_root = Path(__file__).resolve().parents[1]
    contract = _normalize_whitespace((repo_root / "specs" / "behavior" / "worktree-reclaim.md").read_text())
    docker_bug = _normalize_whitespace((repo_root / "docs" / "internal" / "docker-worktree-bug.md").read_text())
    lifecycle = _normalize_whitespace((repo_root / "docs" / "internal" / "worktree-lifecycle.md").read_text())
    rebase_flow = _normalize_whitespace((repo_root / "docs" / "internal" / "advance-rebase-flow.md").read_text())
    internal_index = (repo_root / "docs" / "internal" / "README.md").read_text()

    assert "Provider or agent git activity MUST be isolated" in contract
    assert "MUST NOT be able to mutate the registration of some other in-progress task's worktree" in contract
    assert "If a task needs agent-side git, it MUST run in a git context whose registry is private to that task" in contract

    assert "no longer receive an implicit bind mount of the canonical repository's shared `.git`" in docker_bug
    assert "private checkout with its own `.git/`" in docker_bug
    assert "imports the rewritten tip back into the canonical branch" in docker_bug

    assert "no longer receive an implicit bind mount of the canonical repository's shared `.git`" in rebase_flow
    assert "private rebase checkout with its own real `.git/` directory" in rebase_flow
    assert "skip canonical worktree setup entirely" in rebase_flow
    assert "private checkout is the provider worktree" in rebase_flow
    assert "imports the private checkout tip back into the canonical branch" in rebase_flow

    assert "Docker-backed providers do not receive an implicit bind mount of the canonical repository's shared `.git` directory." in lifecycle
    assert "private rebase checkout" in lifecycle
    assert "host-side after the result is imported back" in lifecycle

    assert "fixed isolation architecture" in internal_index


def test_configuration_docs_include_main_verify_escape_hatch() -> None:
    """Canonical command docs should describe the main-verify operator escape hatch."""
    repo_root = Path(__file__).resolve().parents[1]
    config_content = (repo_root / "docs" / "configuration.md").read_text()
    normalized = " ".join(config_content.split())

    assert "### main-verify" in config_content
    assert "uv run gza main-verify [options]" in config_content
    assert "`--force` | Force a fresh local main verify run now" in config_content
    assert "uv run gza main-verify --force" in normalized
    assert "inspect-first operator check" in normalized
    assert "Watch still owns the automatic remediation lane" in normalized


def test_behavior_check_skill_tracks_main_verify_assertion_namespace() -> None:
    """Behavior-check instructions should define a stable namespace for the red-main contract."""
    repo_root = Path(__file__).resolve().parents[1]
    behavior_check = (repo_root / "src" / "gza" / "skills" / "gza-behavior-check" / "SKILL.md").read_text()
    behavior_check_flat = " ".join(behavior_check.split())

    required_snippets = [
        "- `MV` — `main-verify-self-heal.md`",
        "`MV-MV2-RERUN-BEFORE-REUSE`",
        "`MV-MV4-REMEDIATE-DEDUP-BUMP`",
        "`MV-MV5-NO-LAUNCH-STALL`",
        "`MV-MV6-FORCE-REFRESH`",
        "`WS-S7-BOUNDED-WORK-CREATION`",
    ]

    for snippet in required_snippets:
        assert snippet in behavior_check

    assert "Current tracked behavior-spec prefixes:" in behavior_check
    assert "stable short prefix for the source behavior spec" in behavior_check_flat
    assert (
        "If a new behavior spec is added, assign it a stable doc prefix before reporting findings "
        "from it."
    ) in behavior_check_flat


def test_behavior_check_skill_tracks_open_ended_principle_inventory() -> None:
    """Behavior-check extraction rules should not cap numbered principles below the current spec set."""
    repo_root = Path(__file__).resolve().parents[1]
    behavior_check = (repo_root / "src" / "gza" / "skills" / "gza-behavior-check" / "SKILL.md").read_text()

    assert "The numbered **invariants** (overview) and **principles** (`P*`, for example `P1`–`P6`)." in behavior_check
    assert "`LIN-P6-TERMINAL-LANDED-NOT-ACTIONABLE`" in behavior_check


def test_watch_feature_spec_distinguishes_worker_consuming_capacity_from_direct_recovery() -> None:
    """Feature spec should match the watch scheduler contract for recovery slot usage."""
    repo_root = Path(__file__).resolve().parents[1]
    feature_spec = (repo_root / "specs" / "features" / "watch-loop.md").read_text()

    assert "min(slots, recovery_slots, worker_consuming_recovery_count)" in feature_spec
    assert (
        "| `--recovery-slots N` | 1 | Slots per cy"
        "cle reserved for worker-consuming failed-task recovery before pending pickup |"
        in feature_spec
    )
    assert "Direct reconcile-style recovery remains actionable for mode gating even when it does not spend a worker slot in plain watch." in feature_spec
    assert "min(slots, recovery_slots, actionable_recovery_count)" not in feature_spec


def test_practices_document_gitignored_derived_artifacts_as_non_blockers() -> None:
    """Internal practices should forbid review blockers on gitignored installed artifacts."""
    repo_root = Path(__file__).resolve().parents[1]
    practices_content = (repo_root / "docs" / "internal" / "practices.md").read_text()

    required_snippets = [
        "## Gitignored derived artifacts are not review blockers",
        "`.claude/skills/` is installed per-worktree by `gza skills-install`",
        "Reviewers must not",
        "flag drift between an installed copy and its bundled source as",
        "property of the installed copy genuinely matters",
        "installer into `tmp_path`",
    ]
    for snippet in required_snippets:
        assert snippet in practices_content


def test_practices_document_verify_timeout_diagnostics_recipe() -> None:
    """Internal practices should describe the verify-command SIGTERM diagnostic contract."""
    repo_root = Path(__file__).resolve().parents[1]
    practices_content = (repo_root / "docs" / "internal" / "practices.md").read_text()

    required_snippets = [
        "## Verify commands must flush diagnostics on timeout",
        "The lifecycle runner sends",
        "SIGTERM to the verify process group before escalating to SIGKILL",
        "emits a slow-test summary during normal operation",
        "faulthandler.register(signal.SIGTERM, chain=True)",
        "unit and functional pytest",
        "register_sigterm_faulthandler()` helper at",
        "python -m gza.test_latency",
        "summary before re-raising termination",
    ]
    for snippet in required_snippets:
        assert snippet in practices_content


def test_noop_verify_docs_and_spec_describe_bounded_isolated_reverify_contract() -> None:
    """Tracked lifecycle docs/spec should describe the current bounded isolated reverify contract."""
    repo_root = Path(__file__).resolve().parents[1]
    tracked_docs = {
        "advance_workflow": (repo_root / "docs" / "internal" / "advance-workflow.md").read_text(),
        "overview": (repo_root / "specs" / "behavior" / "00-overview.md").read_text(),
        "lifecycle_engine": (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text(),
    }
    normalized_docs = {
        name: " ".join(content.split())
        for name, content in tracked_docs.items()
    }

    required_snippets = [
        "lifecycle MAY run one bounded fresh verify in an isolated worktree for the current evaluated head",
        "that execution path MUST fail closed on head drift",
        "MUST record SHA-bound clearance metadata before the next merge decision can treat the review as cleared",
    ]
    for snippet in required_snippets:
        assert any(snippet in content for content in normalized_docs.values())

    retired_snippets = [
        "safe reverify",
        "no-op reverify path",
        "fresh autonomous verify evidence still cannot validate the branch",
        "autonomous current-tip reverify",
        "current-tip reverify",
    ]
    for snippet in retired_snippets:
        for content in tracked_docs.values():
            assert snippet not in content


def test_advance_workflow_has_single_noop_improve_limit_row() -> None:
    """Advance workflow doc should describe the no-op improve limit once, with the exception inline."""
    repo_root = Path(__file__).resolve().parents[1]
    workflow = (repo_root / "docs" / "internal" / "advance-workflow.md").read_text()

    condition = (
        "| Consecutive completed no-op improves for the latest `(impl, review)` pair >= "
        "`max_noop_improve_cycles` |"
    )
    action = (
        "`needs_discussion` — reason=`improve-no-op`; stop repeated no-op improve loops "
        "unless runner-owned current passing verify evidence already cleared the review "
        "before lifecycle evaluation"
    )

    assert workflow.count(condition) == 1
    assert action in workflow


def test_disputed_blocker_contract_is_tracked_consistently() -> None:
    """Specs/docs should align on adjudication-first disputed-blocker routing."""
    repo_root = Path(__file__).resolve().parents[1]
    overview = (repo_root / "specs" / "behavior" / "00-overview.md").read_text()
    lifecycle = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    workflow = (repo_root / "docs" / "internal" / "advance-workflow.md").read_text()
    lifecycle_flat = " ".join(lifecycle.split())
    workflow_flat = " ".join(workflow.split())
    review_active_rows = _extract_markdown_table_rows(workflow, "#### 6b. Review is active (not cleared)")
    adjudication_row_index, adjudication_condition, adjudication_action = _find_markdown_table_row(
        review_active_rows,
        condition_contains="adjudication-eligible disputed non-verify CODE blocker",
    )
    generic_noop_row_index, generic_noop_condition, _ = _find_markdown_table_row(
        review_active_rows,
        condition_contains=(
            "Consecutive completed no-op improves for the latest `(impl, review)` pair >= "
            "`max_noop_improve_cycles`"
        ),
    )

    assert "adjudication marking a disputed blocker `INVALID` for lifecycle" in overview
    assert "blocker adjudication needed" in overview
    assert "Disputed non-verify CODE blocker adjudication" in lifecycle
    assert "structured current-state evidence" in lifecycle
    assert "This lane applies only to non-verify CODE blockers." in lifecycle
    assert "review-blocker-adjudication-needed" in lifecycle
    assert lifecycle.index("**B. Disputed non-verify CODE blocker adjudication.**") < lifecycle.index(
        "Otherwise, consecutive no-op improves reach `max_noop_improve_cycles`"
    )
    assert (
        "This generic no-op park applies only after ruling out rule B adjudication-eligible disputed "
        "non-verify CODE blockers."
    ) in lifecycle_flat

    assert "Every `BLOCKER` must be falsifiable." in workflow
    assert "structured `## Disputed Blockers` section" in workflow
    assert "Verdict = `CHANGES_REQUESTED`" in adjudication_condition
    assert (
        "consecutive completed no-op improves for the latest `(impl, review)` pair >= "
        "`max_noop_improve_cycles`"
    ) in adjudication_condition
    for action_name in (
        "`create_review_adjudication`",
        "`run_review_adjudication`",
        "`wait_review_adjudication`",
    ):
        assert action_name in adjudication_action
    assert "generic no-op park" in adjudication_action
    assert generic_noop_condition.startswith(
        "Consecutive completed no-op improves for the latest `(impl, review)` pair >= "
    )
    assert adjudication_row_index < generic_noop_row_index
    assert "required lifecycle contract is adjudication before the generic `improve-no-op`," in workflow
    assert "or once the same non-verify CODE blocker repeats across the duplicate-blocker review" in workflow_flat
    assert "`duplicate-blocker-no-progress`, and `review-max-cycles` parks." in workflow
    assert "lifecycle consumes those persisted outcomes immediately" in workflow
    assert "`NEEDS_HUMAN` parks with `review-blocker-adjudication-needed`" in workflow
    assert (
        "Verify-only blockers remain governed by runner-owned same-branch, same-head verify provenance."
        in workflow_flat
    )


def test_lifecycle_spec_preserves_typed_review_comment_contract() -> None:
    """Lifecycle spec should keep only feedback comments improve-actionable."""
    repo_root = Path(__file__).resolve().parents[1]
    lifecycle = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    lifecycle_flat = " ".join(lifecycle.split())

    assert (
        "Unresolved `feedback` comments newer than the latest completed review MUST be "
        "addressed via the improve flow **before** any merge, even on an approved verdict."
    ) in lifecycle_flat
    assert (
        "Unresolved comments of other kinds (for example `review_scope`) MUST remain visible to "
        "operators but MUST NOT create, reuse, resume, wait on, or freshness-block an improve task."
    ) in lifecycle_flat
    assert "Unresolved review comments newer than the latest completed review MUST be addressed" not in lifecycle_flat


def test_verify_only_noop_improve_contract_does_not_claim_generic_recapture() -> None:
    """Spec/report text should stay aligned with the narrowed same-head failed-review recapture path."""
    repo_root = Path(__file__).resolve().parents[1]
    lifecycle_engine = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()
    behavior_check = (repo_root / "reviews" / "20260618084043-behavior-check.md").read_text()
    overview = (repo_root / "specs" / "behavior" / "00-overview.md").read_text()

    assert "it re-runs verify for a no-op improve that is eligible to clear a verify-only review" in lifecycle_engine
    assert "blocker, keyed by branch + head SHA. That no-op improve-side re-run applies only when" in lifecycle_engine
    assert "the current review row already carries runner-owned review-time failure evidence for" in lifecycle_engine
    assert "the same branch/head." in lifecycle_engine
    assert "lifecycle MUST first conservatively classify the blocker set as" in lifecycle_engine
    assert "verify-only before same-head runner-owned evidence can clear it" in lifecycle_engine
    assert "prose alone MUST NOT" in lifecycle_engine
    assert "decide stale/non-stale provenance." in lifecycle_engine
    assert "ordinary non-timeout" in lifecycle_engine
    assert "review-fail then no-op-improve-pass evidence pair" in lifecycle_engine
    assert "each time it runs a no-op improve" not in lifecycle_engine

    assert "LE-\u00a76-VERIFY-ONLY-CLEAR" in behavior_check
    assert "verify-only review classification plus runner-owned review-time failure evidence" in behavior_check
    assert "review-fail/no-op-improve-pass evidence" in behavior_check
    assert "persists `clear_review_state(...)`" in behavior_check
    assert "detached re-verify action" in behavior_check
    assert "ordinary `verify_command` failures as well as timeouts" in behavior_check
    assert "runner.py:" not in behavior_check
    assert "advance_engine.py:" not in behavior_check

    assert "review FAILED and the no-op improve later PASSED at the same branch head" in overview
    assert "Ordinary verify-failure-only stale reviews are handled by that same-head evidence-clear path" in overview
    assert "lifecycle first classifies the blocker set as verify-only" in overview


def test_tests_integration_module_guidance_avoids_stale_test_paths() -> None:
    """Integration test module docstrings should not point at the removed tests/test_integration.py path."""
    repo_root = Path(__file__).resolve().parents[1]
    integration_tests_root = repo_root / "tests_integration"

    stale_references: list[str] = []
    for path in sorted(integration_tests_root.glob("*.py")):
        content = path.read_text()
        if "tests/test_integration.py" in content:
            stale_references.append(str(path.relative_to(repo_root)))

    assert not stale_references, (
        "Found stale tests/test_integration.py guidance under tests_integration/: "
        + ", ".join(stale_references)
    )


def test_configuration_docs_require_full_prefixed_ids_for_strict_commands() -> None:
    """Strict-ID command reference entries should consistently require full prefixed IDs."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `task_id` | Specific full prefixed task ID(s) to run",
        "| `--based-on ID` | Base on previous task by full prefixed task ID",
        "| `--depends-on ID` | Set dependency on another task by full prefixed task ID",
        "| `task_id` | Full prefixed task ID to edit",
        "| `--based-on ID` | Set lineage/parent relationship using a full prefixed task ID",
        "| `--depends-on ID` | Set execution dependency using a full prefixed task ID",
        "| `--clear-depends-on` | Remove the execution dependency",
        "| `task_id` | Full prefixed task ID to kill",
        "| `task_id` | Full prefixed task ID to mark as completed",
        "| `task_id` | Full prefixed task ID(s) to merge",
        "| `task_id_or_branch` | Full prefixed task ID or branch name to checkout",
        "| `task_id` | Full prefixed task ID to diff",
        "| `task_id` | Full prefixed task ID for the completed task to open as a PR",
        "| `task_id` | Full prefixed task ID to delete",
        "| `task_id` | Full prefixed task ID to show",
        "| `task_id` | Full prefixed task ID to resume",
        "| `task_id` | Full prefixed task ID to retry",
        "| `task_id` | Full prefixed task ID to rebase",
        "| `impl_task_id` | Full prefixed task ID (implement, improve, review, or fix",
        "| `--review-id ID` | Explicit full prefixed review task ID to base the improve on",
        "| `task_id` | Full prefixed task ID (implement, improve, review, or fix",
        "| `plan_task_id` | Full prefixed completed plan task ID to implement",
        "| `task_id` | Full prefixed task ID for a completed `plan` or `plan_improve` source |",
        "| `task_id` | Full prefixed completed `CHANGES_REQUESTED` `plan_review` task ID to revise |",
        "| `task_id` | Specific full prefixed task ID to advance",
        "| `impl_task_id` | Full prefixed implementation task ID to iterate",
        "| `task_id` | Full prefixed task ID(s) whose branch cohorts should be synced",
        "`task_id` must be a full prefixed task ID (for example `gza-1234`).",
    ]

    for snippet in required_snippets:
        assert snippet in config_content
    assert "{prefix}-{base36}" not in config_content
    assert "`gza-1a2b`" not in config_content


def test_configuration_docs_cover_force_execution_flags_and_prerequisite_unmerged_guidance() -> None:
    """Operator docs should stay in sync with execution --force and failure-recovery behavior."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Compatibility flag; dependency-blocked tasks still will not run |",
        "| `--force` | Bypass numeric retry/review caps only; dependency-blocked tasks still will not run |",
        "| `--force` | Bypass numeric retry/review caps only; dependency-blocked tasks still will not run |",
        "`PREREQUISITE_UNMERGED`: the resolved completed dependency is not yet marked merged",
    ]

    for snippet in required_snippets:
        assert snippet in config_content

    assert "Use `--force` only when you intentionally want to bypass this guard." not in config_content
    assert "`--force` does not bypass dependency merge/readiness guards." in config_content


def test_retry_docs_and_examples_describe_same_branch_retry_split() -> None:
    """Retry docs should describe fresh conversations without implying every retry forks fresh."""
    repo_root = Path(__file__).resolve().parents[1]
    config_content = (repo_root / "docs" / "configuration.md").read_text()
    examples_readme = (repo_root / "docs" / "examples" / "README.md").read_text()

    retry_section = config_content.split("### retry", 1)[1].split("### mark-completed", 1)[0]

    assert "creating a new attempt with a fresh conversation" in retry_section
    assert "Implement retries may fork a fresh branch; same-branch follow-up retries stay attached to the shared branch." in retry_section
    assert "from scratch" not in retry_section
    assert "Starts a fresh conversation." not in retry_section
    assert "| Create a new retry attempt immediately | `gza retry <task_id> --run` |" in examples_readme
    assert "| Retry from scratch | `gza retry <task_id>` |" not in examples_readme


def test_configuration_docs_describe_unimplemented_lineage_guidance() -> None:
    """advance docs should explain completed-source surfacing and truthful follow-up actions."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `--unimplemented` | List completed plan/explore source rows that still need an implementation path |",
        "| `--create` | With `--unimplemented`: queue implement tasks for the listed source rows |",
        "Completed `explore` roots with an active",
        "queued follow-up work through `uv run gza next`, `uv run gza next --all`, or other queue surfaces.",
        "Only completed plan rows are directly runnable with `uv run gza implement <id>`;",
        "use `uv run gza advance --unimplemented --create` to queue implement tasks",
        "for listed explore rows.",
        "Completed held plan tasks surface `awaiting_human` until you run `uv run gza implement <plan-id>`",
        "Held completed plans use `next_action = awaiting_human`",
    ]

    for snippet in required_snippets:
        assert snippet in config_content

    assert "It may surface a newer pending" not in config_content


def test_configuration_docs_describe_recovery_vs_pending_operating_surface() -> None:
    """Operator docs should make queue/work/advance/watch lane ownership explicit."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "### work / advance / watch operating surface",
        "| `uv run gza work` | Yes. Pending lane only. | No. | No. | No. |",
        "| `uv run gza advance` | No by default. Yes with `--new` after lifecycle/recovery planning. |",
        "| `uv run gza watch` | Yes. Maintains the configured batch from the pending lane. |",
        "Recovery lane entries belong to `advance` / `watch`, not `work`.",
        "Lifecycle-action entries belong to `advance` / `watch`, not `work`.",
        "Pending lane entries belong to `work` / `watch`.",
        "`gza next` now renders three distinct sections:",
        "`uv run gza queue` shows the pending lane by default; add `--full`, `--recovery`, or `--recovery-first` for broader dispatch previews.",
    ]

    for snippet in required_snippets:
        assert snippet in config_content


def test_configuration_docs_describe_sync_as_broader_explicit_reconciliation_surface() -> None:
    """Canonical docs should keep `uv run gza sync` as the broader explicit branch and PR maintenance surface."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    workflow_example = (docs_root / "examples" / "plan-implement-review.md").read_text()

    required_snippets = [
        "### sync",
        "uv run gza sync [task_id ...] [options]",
        "Use `uv run gza unmerged` for the daily \"what still needs to be merged?\" check.",
        "`uv run gza sync` remains the broader explicit branch and PR reconciliation command.",
        "The only GitHub-side exceptions outside `uv run gza sync` are improve and fix completion with `--review`",
        "`gza pr` does not reconcile or close stale GitHub PRs",
        "`gza merge` only performs the local git merge/rebase path",
        "`uv run gza sync <impl_id>`",
    ]
    for snippet in required_snippets:
        assert snippet in config_content or snippet in workflow_example


def test_skills_docs_do_not_advertise_unsupported_gza_log_task_flag() -> None:
    """docs/skills.md examples should avoid invalid gza log --task invocations."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    skills_content = (docs_root / "skills.md").read_text()

    assert "gza log --task" not in skills_content
    assert "gza log gza-p --task" not in skills_content


def test_configuration_docs_include_comment_command_reference() -> None:
    """Canonical command reference should document `gza comment` and comment visibility output."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "### comment",
        "gza comment <task_id> <text> [options]",
        "| `task_id` | Full prefixed task ID to comment on",
        "`feedback` remains the default improve-actionable comment kind",
        "`--kind review_scope`",
        "When task comments exist, `gza show` also includes a `Comments:` section",
        "When tasks have comments, `gza history` includes a `comments: N` indicator",
    ]
    for snippet in required_snippets:
        assert snippet in config_content


def test_review_scope_resolution_order_docs_and_spec_stay_aligned() -> None:
    """Review-scope docs/spec must advertise the same fallback order and plan-context role."""
    repo_root = Path(__file__).resolve().parents[1]
    config_content = (repo_root / "docs" / "configuration.md").read_text()
    review_isolation = (repo_root / "docs" / "internal" / "review-isolation.md").read_text()
    lifecycle_spec = (repo_root / "specs" / "behavior" / "lifecycle-engine.md").read_text()

    required_snippets = [
        "persisted `review_scope`",
        "latest typed `review_scope` task comment",
        "legacy sliced-prompt parsing",
        "implementation prompt metadata",
        "`## Original plan context (out of scope except for the review scope):`",
    ]
    for snippet in required_snippets:
        assert snippet in config_content or snippet in review_isolation or snippet in lifecycle_spec

    assert "persisted `Task.review_scope`, latest typed `review_scope` task comment" in review_isolation
    assert "then legacy sliced-prompt parsing, then a conservative plan-backed fallback" in config_content
    assert "then legacy sliced-prompt parsing, then a" in lifecycle_spec
    assert "MUST be rendered only as labeled background context" in lifecycle_spec


def test_configuration_docs_keep_fix_comment_and_run_inline_surfaces() -> None:
    """run-inline docs additions must not replace existing fix/comment operator docs."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "### run-inline",
        "gza run-inline <task_id> [options]",
        "### search",
        "gza search <term> [options]",
        "| `--status-not CSV` | Exclude statuses (comma-separated) |",
        "| `--lineage-of TASK_ID` | Restrict to the canonical lineage containing TASK_ID |",
        "| `--related-to-not TASK_ID` | Deprecated alias for `--lineage-of-not` |",
        "Positive and negative filters on the same field are applied in order",
        "### incomplete",
        "gza incomplete [options]",
        "Show unresolved task lineages that still need attention.",
        "| `--tag TAG` | Only show unresolved lineage owners matching tag filters (repeatable) |",
        "| `--all-tags` | With repeated `--tag` values, require all requested tags instead of the default any-tag matching |",
        "| `--blocked-by-dropped` | Switch to pending tasks blocked by dropped dependencies instead of unresolved lineages |",
        "### tv",
        "gza tv [task_id ...] [options]",
        "### comment",
        "gza comment <task_id> <text> [options]",
        "### fix",
        "gza fix <task_id> [options]",
        "| `--type TYPE` | Filter by task type: `explore`, `plan`, `plan_review`, `plan_improve`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal` |",
        "| `--status-not STATUS` | Exclude the given status |",
        "| `--tag-not TAG` | Exclude by tag (repeatable; uses the same all-tags vs any-tag matching mode as `--tag`) |",
    ]
    for snippet in required_snippets:
        assert snippet in config_content


def test_merge_first_docs_and_fix_skill_schema_stay_in_sync() -> None:
    """Tracked operator docs and bundled fix-skill source should stay aligned."""
    repo_root = Path(__file__).resolve().parents[1]
    config_content = (repo_root / "docs" / "configuration.md").read_text()
    advance_workflow = (repo_root / "docs" / "internal" / "advance-workflow.md").read_text()
    fix_skill = (repo_root / "src" / "gza" / "skills" / "gza-task-fix" / "SKILL.md").read_text()

    shared_required_snippets = [
        "autonomous_verify_timeout_seconds",
        "review_verify_timeout_grace_seconds",
        "recommend_rebase_behind_commits",
        "Deprecated compatibility key; accepted but ignored",
        "Projected `next_action` values come from the shared live lifecycle planner",
        "Cleanly mergeable branches continue to the normal review or merge actions",
        "verify:",
        "autonomous_verify_timeout_seconds: <int>",
        "blockers:",
    ]
    for snippet in shared_required_snippets:
        assert (
            snippet in config_content
            or snippet in advance_workflow
            or snippet in fix_skill
        )

    assert "review_verify_timeout_grace_seconds: <number >= 1>" in fix_skill
    assert (
        "Grace period after SIGTERM before autonomous review verification escalates to SIGKILL; "
        "accepts float values >= 1 second"
    ) in advance_workflow

    stale_grace_schema = "review_verify_timeout_grace_seconds: <int>"
    assert stale_grace_schema not in fix_skill
    assert stale_grace_schema not in advance_workflow
    assert stale_grace_schema not in config_content

    retired_snippets = [
        "recommend_rebase:",
        "branch_behind_target",
        "recommend_rebase.recommended=true",
        "stale-branch recommendation",
    ]
    for snippet in retired_snippets:
        assert snippet not in fix_skill


def test_summary_docs_and_skill_use_dedicated_triage_surfaces() -> None:
    """`/gza-summary` docs should synthesize dedicated surfaces instead of reviving `gza incomplete`."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_doc_content = (repo_root / "docs" / "skills.md").read_text()
    skill_content = (repo_root / "src" / "gza" / "skills" / "gza-summary" / "SKILL.md").read_text()

    required_snippets = [
        "uv run gza history --status failed",
        "uv run gza advance --unimplemented",
        "uv run gza unmerged",
        "uv run gza next --all",
        "/gza-summary",
        "Failed Recovery",
        "Queue State",
    ]
    for snippet in required_snippets:
        assert snippet in skills_doc_content
        assert snippet in skill_content

    assert "git merge" not in skills_doc_content
    assert "git merge" not in skill_content
    assert "factual failed-attempt history" in skills_doc_content
    assert "factual failed-task history" in skill_content
    assert "unresolved failed tasks" not in skill_content
    assert "not a canonical replacement for `gza incomplete`" in skill_content
    assert "Keep failed history, unmerged work, unimplemented follow-up, and queue state on their dedicated surfaces." in skill_content


def test_task_triage_skill_defaults_no_id_sweeps_to_recent_effective_window() -> None:
    """No-ID triage sweeps should ask for a recent window and avoid default all-time backlog scans."""
    repo_root = Path(__file__).resolve().parents[1]
    skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-triage" / "SKILL.md"
    ).read_text()

    required_snippets = [
        "Without ID:** ask how far back to look before gathering rows. Default to a recent sweep, not an all-time backlog walk.",
        "AskUserQuestion and offer:",
        "`Last 1 hour`",
        "`Last 24 hours` (default/recommended)",
        "`Last 7 days`",
        "`All time`",
        'If the caller\'s intent is clearly recent',
        "uv run gza incomplete --json --days 1 --date-field effective --last 0",
        "`effective_at` — use this as the default recency timestamp.",
        "Do the recency filtering in this step, before any `gza show` follow-up work.",
    ]
    for snippet in required_snippets:
        assert snippet in skill_content


def test_redundant_no_work_operator_docs_and_skill_stay_in_sync() -> None:
    """Tracked triage/docs guidance should not strand redundant no-work rows as unknown."""
    repo_root = Path(__file__).resolve().parents[1]
    skill_content = (repo_root / "src" / "gza" / "skills" / "gza-task-triage" / "SKILL.md").read_text()
    config_content = (repo_root / "docs" / "configuration.md").read_text()

    assert "merge-unit-empty" in skill_content
    assert "merge-unit-redundant" in skill_content
    assert "merge-unit-empty\") |" not in skill_content
    assert 'lifecycle "target merged", "merge-unit-merged", or "merge-unit-empty"' not in skill_content
    assert "already present on target (`redundant` merge state)" in config_content

    assert "- **Without ID:** sweep the whole list. Run `uv run gza incomplete --json --last 0` and process every row." not in skill_content


def test_task_triage_skill_keeps_explicit_id_path_outside_recency_window() -> None:
    """Explicit-ID triage should still bypass recency filtering and inspect the requested lineage."""
    repo_root = Path(__file__).resolve().parents[1]
    skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-triage" / "SKILL.md"
    ).read_text()

    assert (
        "- **With ID:** triage just that lineage. Resolve the merge-unit owner via `gza show <id>` and look at the row that owns it."
        in skill_content
    )
    assert (
        "- **With ID:** run `uv run gza incomplete --json --last 0` and filter to the merge-unit owner row that contains the requested task"
        in skill_content
    )
    assert "If no row matches, fall back to `uv run gza show <id>` and report the lineage state directly." in skill_content


def test_gza_rebase_docs_match_final_verify_contract() -> None:
    """Operator docs should describe the rebase skill's final verify_command contract."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_doc_content = (repo_root / "docs" / "skills.md").read_text()
    skill_content = (repo_root / "src" / "gza" / "skills" / "gza-rebase" / "SKILL.md").read_text()
    advance_workflow_content = (repo_root / "docs" / "internal" / "advance-workflow.md").read_text()

    assert "configured project `verify_command`" in skills_doc_content
    assert "after any stash restoration" in skills_doc_content
    assert "before declaring success" in skills_doc_content
    assert "default mode, checks for uncommitted changes before starting and stops if any exist" in skills_doc_content
    assert "In `--auto` mode, stashes uncommitted changes before rebasing" in skills_doc_content
    assert "relies only on local refs already present" in skills_doc_content
    assert "Honors the caller-provided local target branch" in skills_doc_content
    assert "origin/HEAD" in skills_doc_content
    assert "uv run gza config` only as an optional confirmation" in skills_doc_content

    assert "project `verify_command`" in skill_content
    assert "after any stashed changes have been restored" in skill_content
    assert "Do not report success yet." in skill_content
    assert "Do NOT use remote git operations." in skill_content
    assert "In default mode: if any exist, stop and ask the user to commit or stash them" in skill_content
    assert 'In `--auto` mode: if any exist, run `git -C "$GZA_WORKTREE_ROOT" stash push -u` to save them.' in skill_content
    assert "If the caller named a target branch (for example `master`), use that exact branch name." in skill_content
    assert 'git -C "$GZA_WORKTREE_ROOT" symbolic-ref --quiet --short refs/remotes/origin/HEAD' in skill_content
    assert "Do not substitute `main`" in skill_content
    assert "read `verify_command` directly from `gza.yaml`" in skill_content

    assert "Rebases onto the already-present local target branch without fetching or other remote operations" in advance_workflow_content
    assert "Restores stashed changes before final verification" in advance_workflow_content

    assert "verifies Python syntax" not in skills_doc_content
    assert "origin/main` (default)" not in skills_doc_content
    assert "Checks for uncommitted changes before starting (stops if any exist)" not in skills_doc_content
    assert "Fetches and rebases onto the target branch" not in advance_workflow_content


def test_gza_test_and_fix_docs_lead_with_gza_yaml_verify_command_lookup() -> None:
    """Worker-facing verify docs should prefer gza.yaml over gza CLI lookup."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_doc_content = (repo_root / "docs" / "skills.md").read_text()
    skill_content = (repo_root / "src" / "gza" / "skills" / "gza-test-and-fix" / "SKILL.md").read_text()

    assert "reads `verify_command` from `gza.yaml` first" in skills_doc_content
    assert "treats `uv run gza config` as optional" in skills_doc_content
    assert "Read `verify_command` directly from `gza.yaml`" in skill_content
    assert "do not treat `gza config` failure as an error when `gza.yaml` was readable" in skill_content

def test_spec_examples_use_tags_not_retired_group_aliases() -> None:
    """Operator spec examples should teach canonical tags syntax only."""
    docs_root = Path(__file__).resolve().parents[1] / "docs" / "examples"
    checked_files = [
        docs_root / "using-specs.md",
    ]

    for path in checked_files:
        content = path.read_text()
        assert "group:" not in content
        assert "to group:" not in content
        assert "tags:" in content


def test_source_skills_use_impl_tags_not_retired_group_placeholders() -> None:
    """Bundled source skills should pass tags through improve/review flows."""
    skills_root = Path(__file__).resolve().parents[1] / "src" / "gza" / "skills"
    checked_files = [
        skills_root / "gza-task-fix" / "SKILL.md",
        skills_root / "gza-task-review" / "SKILL.md",
    ]

    for path in checked_files:
        content = path.read_text()
        assert "impl_group" not in content
        assert "group=<" not in content
        assert "group='" not in content
        assert "impl_tags" in content
        assert "tags=<" in content


def test_fix_skill_treats_installed_claude_skills_as_local_install_state() -> None:
    """The bundled fix skill should not treat `.claude/skills/` as committable blocker state."""
    skill_content = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "gza"
        / "skills"
        / "gza-task-fix"
        / "SKILL.md"
    ).read_text()

    assert "git add .claude/skills/" not in skill_content
    assert "bundled source under `src/gza/skills/` is the only committable source of truth" in skill_content
    assert "classify it as an environment/refresh issue rather than a review blocker" in skill_content


def test_operator_facing_unmerged_examples_use_uv_run_prefix() -> None:
    """Operator-facing docs should use the canonical uv-run invocation for unmerged."""
    repo_root = Path(__file__).resolve().parents[1]
    operator_docs = [
        repo_root / "docs" / "examples" / "README.md",
        repo_root / "docs" / "examples" / "simple-task.md",
        repo_root / "docs" / "examples" / "rebasing.md",
        repo_root / "docs" / "quickstart.md",
    ]

    for path in operator_docs:
        content = path.read_text()
        assert "gza unmerged" not in content.replace("uv run gza unmerged", "")
        assert "uv run gza unmerged" in content


def test_configuration_docs_describe_comments_only_improve_path() -> None:
    """Improve docs should reflect comments-only fallback when no review exists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    assert "unresolved task comments as feedback context" in config_content
    assert "review exists but unresolved comments do" in config_content
    assert "improve still runs using comments-only feedback" in config_content


def test_configuration_docs_cover_unmerged_conflict_output_and_color_override() -> None:
    """Unmerged docs should mention the conflict output line and matching color override field."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| Unmerged | `review_approved`, `review_followups`, `review_changes`, `review_discussion`, `review_none`, `merge_conflicts` |",
        "When live merge analysis detects unresolved conflicts, a dedicated `merge: has conflicts` line.",
    ]
    for snippet in required_snippets:
        assert snippet in config_content


def test_docker_docs_describe_digest_based_rebuild_freshness() -> None:
    """Docker docs should reflect digest-label freshness checks, not mtime heuristics."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    docker_content = (docs_root / "docker.md").read_text()

    required_snippets = [
        "gza.dockerfile_sha256",
        "content digest differs",
        "missing Dockerfile content label",
        "Dockerfile.<cli> content changed",
    ]
    stale_snippets = [
        "modification time",
        "Docker image's creation time",
        "if the Dockerfile is newer",
        'Prints "Dockerfile changed, rebuilding..." when this happens',
    ]

    for snippet in required_snippets:
        assert snippet in docker_content
    for snippet in stale_snippets:
        assert snippet not in docker_content


def test_plan_implement_review_example_uses_uv_run_gza_shell_snippets() -> None:
    """Workflow example should not mix bare gza shell snippets with uv run gza guidance."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    example_content = (docs_root / "examples" / "plan-implement-review.md").read_text()

    required_snippets = [
        "$ uv run gza add --type implement --based-on gza-1 --review \"Implement...\"",
        "$ uv run gza add --type implement --based-on gza-1 --review --pr \"Implement...\"",
    ]
    for snippet in required_snippets:
        assert snippet in example_content

    for line in example_content.splitlines():
        stripped = line.lstrip()
        assert not stripped.startswith("$ gza ")
        assert not stripped.startswith("> $ gza ")


def test_plan_implement_review_example_describes_pr_as_completion_time_request() -> None:
    """Workflow example should describe `--pr` as a best-effort completion-time request."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    example_content = (docs_root / "examples" / "plan-implement-review.md").read_text()

    required_snippets = [
        "request PR creation or reuse after it completes successfully",
        "evaluated at completion time and skipped without failing when PRs are unavailable",
        "post PR comments automatically when a PR exists",
    ]
    stale_snippets = [
        "open or reuse a PR as soon as it first completes",
        "post PR comments automatically:",
    ]

    for snippet in required_snippets:
        assert snippet in example_content
    for snippet in stale_snippets:
        assert snippet not in example_content


def test_recovery_docs_use_uv_run_gza_on_touched_recovery_surfaces() -> None:
    """Touched recovery docs should keep canonical `uv run gza ...` command wording."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    failed_tasks_content = (docs_root / "examples" / "failed-tasks.md").read_text()

    advance_section = config_content.split("### advance", 1)[1].split("### iterate", 1)[0]
    iterate_section = config_content.split("### iterate", 1)[1].split("### watch", 1)[0]
    watch_section = config_content.split("### watch", 1)[1].split("### learnings", 1)[0]

    assert "uv run gza advance [task_id] [options]" in advance_section
    assert "uv run gza iterate <impl_task_id> [options]" in iterate_section
    assert "uv run gza watch [options]" in watch_section
    assert "If that manual resume completes successfully, operator-facing lifecycle readouts move forward from the completed resume descendant" in iterate_section
    assert "The same manual-only warning path also applies when an older failed task is blocked by a newer failed recovery descendant" in iterate_section
    assert "`uv run gza watch --recovery-only --dry-run` is the recovery inspection surface" in watch_section
    assert "default `watch.recovery_slots = 1` means each watch pass allocates up to one slot to worker-consuming failed-task recovery before pending pickup" in watch_section
    assert "suppresses pending pickup until actionable recovery drains, even for direct reconcile actions that do not consume a worker slot" in watch_section
    assert "use `uv run gza queue --tag TAG` to preview the matching pending pickup order, or add `--full` to also preview matching recovery candidates and lifecycle actions" in watch_section
    assert "Scoped watch reports out-of-scope derived blockers but does not start them" in watch_section
    assert "add `--full` to preview matching recovery candidates and lifecycle actions too" in config_content
    assert "Only list pending tasks matching tag filters by default" in config_content
    assert "Only list recovery, lifecycle, and pending lanes matching tag filters" not in config_content

    assert "\ngza advance [task_id] [options]\n" not in advance_section
    assert "\ngza iterate <impl_task_id> [options]\n" not in iterate_section
    assert "\ngza watch [options]\n" not in watch_section
    assert "`gza watch --restart-failed --dry-run` is the recovery inspection surface" not in watch_section
    assert "Plain `gza watch` and `--restart-failed` both use the same bounded shared recovery policy" not in watch_section

    assert "| `uv run gza resume` | Continue from where it left off |" in failed_tasks_content
    assert (
        "| `uv run gza retry` | Create a new retry attempt | Task needs another run; "
        "implement retries fork fresh, same-branch follow-ups stay on the shared branch |"
        in failed_tasks_content
    )
    assert "| `uv run gza watch --recovery-only` | Send the full watch batch to failed-task recovery, choosing `resume` or `retry` per task |" in failed_tasks_content
    assert "`uv run gza watch` now has a built-in two-lane split." in failed_tasks_content

    for line in failed_tasks_content.splitlines():
        assert not line.lstrip().startswith("$ gza ")


def test_watch_attention_docs_describe_changed_only_inline_attention_behavior() -> None:
    """Watch docs should describe changed-only inline ATTENTION plus full roundups."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    internal_content = (docs_root / "internal" / "advance-workflow.md").read_text()

    watch_section = config_content.split("### watch", 1)[1].split("### learnings", 1)[0]

    assert "surfaced as `ATTENTION` lines in watch output instead of one-shot deduped `SKIP` lines" in watch_section
    assert "Inline `ATTENTION` is emitted only when an attention row is newly visible" in watch_section
    assert "Each watch pass still prints a counted `Needs attention (...)` roundup for the full current visible set" in watch_section
    assert "Each watch pass also emits one counted `Lifecycle actions (...)` summary line before execution when actionable lifecycle work is queued for that pass" in watch_section
    assert "Guarded pending routing skips use the same centralized attention path on the first observed guarded skip" in watch_section
    assert "watch does not re-select them for a fresh iterate worker in the meantime" in watch_section
    assert "Ordinary wait/skip states keep the existing `SKIP` dedupe behavior." in watch_section
    assert "Inline `ATTENTION` appears only when an attention key is newly visible" in internal_content
    assert "Each watch pass that emits visible attention also prints a counted `Needs attention (...)` section with the same formatted task rows for the full current visible set" in internal_content
    assert "each watch pass now emits one concise `Lifecycle actions (...)` summary line" in internal_content
    assert "Guarded-pending routing skips are promoted through the same centralized attention path on the first observed guarded skip" in internal_content
    assert "watch reuses that parked action instead of recomputing a fresh lifecycle step" in internal_content
    assert "Ordinary watch skip/wait lines remain deduped across passes." in internal_content


def test_watch_supervisor_spec_pins_per_cycle_human_required_owner_parity() -> None:
    """The watch supervisor spec should pin owner-based standing attention parity."""
    behavior_root = Path(__file__).resolve().parents[1] / "specs" / "behavior"
    supervisor = (behavior_root / "watch-supervisor.md").read_text()
    compact = " ".join(supervisor.split())
    cycle_word = "cy" "cle"

    assert "**S6 — Human-required states are standing operator signals.**" in supervisor
    assert f"For every watch {cycle_word}, `watch` MUST emit an operator-visible `Needs attention` signal" in compact
    assert "The failed leaf ID is detail within that owner's signal, never a separate top-level entry." in compact
    assert "This set, compared by **owner / merge-unit ID**, MUST be identical to the set surfaced by" in compact
    assert "`gza incomplete` from the same shared failed-task recovery computation for the same" in compact
    assert "`--restart-failed` and `--show-skipped` MUST NOT control whether" in compact
    assert "No failure reason, empty-branch state," in compact
    assert "landed-lineage state, or lack of an in-session status transition may remove a" in compact
    assert f"### 2A. Per-{cycle_word} human-required parity belongs to phase 5" in supervisor
    assert "the supervisor MUST recompute the in-scope" in compact
    assert f"human-required failed-task set on **every** {cycle_word} from the same shared failed-task" in compact
    assert "already-landed suppression" in compact
    assert "[recovery.md](recovery.md) R5" in supervisor
    assert "[lineage.md](lineage.md) P1 and P4" in supervisor
    assert "When that shared recovery policy returns a failed-task decision that parks the owner for" in compact
    assert "human intervention, phase 5 MUST emit `Needs attention` for that owner even when the" in compact
    assert "decision is represented internally as a `skip`." in compact
    assert "Human-required parity is owner-based" in compact
    assert "Non-human skips and hidden recovery decisions MAY remain silent or appear only in" in compact


def test_internal_advance_workflow_task_collection_tracks_shared_recovery_policy() -> None:
    """Internal advance workflow docs should describe shared failed-task recovery collection policy."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    internal_content = (docs_root / "internal" / "advance-workflow.md").read_text()
    task_collection_section = internal_content.split("## Task Collection", 1)[1].split("## Configuration", 1)[0]

    assert "Advance collects owner rows from one shared source" in task_collection_section
    assert "query_lineage_owner_rows(...)" in task_collection_section
    assert "decide_failed_task_recovery(...)" in task_collection_section
    assert "resume`, `retry`, or manual review required" in task_collection_section
    assert "advance --dry-run` surfaces one warning that only git branch reachability suppression is unavailable for this run" in task_collection_section
    assert "metadata-based same-lineage merged-task suppression may still apply" in task_collection_section
    assert "failed-row visibility remains conservative only for the git-reachability decision" in task_collection_section
    assert "The only exception is an explicit no-gate project with no configured `verify_command`" in task_collection_section
    assert '`status="unavailable"` / `exit_status="not configured"`' in task_collection_section
    assert "keeps the failed rows visible" not in task_collection_section
    assert "failure_reason IN ('MAX_STEPS', 'MAX_TURNS')" not in task_collection_section
    assert "session_id IS NOT NULL" not in task_collection_section
    assert "**Resumable failed tasks**" not in task_collection_section


def test_internal_advance_workflow_failed_task_recovery_is_not_resume_only() -> None:
    """Internal advance workflow docs should describe retry as a first-class worker-spawning recovery action."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    internal_content = (docs_root / "internal" / "advance-workflow.md").read_text()

    failed_task_section = internal_content.split("### 9. Failed task recovery", 1)[1].split("## Improve chain semantics", 1)[0]
    worker_actions_section = internal_content.split("### Worker-spawning actions", 1)[1].split("### Direct actions", 1)[0]
    output_section = internal_content.split("## Output", 1)[1].split("## Idempotency", 1)[0]

    assert "Failed task recovery rules run in the same ordered rule engine." in failed_task_section
    assert "| Shared failed-task recovery policy returns `resume` | `resume` — create resume task and spawn worker |" in failed_task_section
    assert "| Shared failed-task recovery policy returns `retry` | `retry` — create retry task and spawn worker |" in failed_task_section
    assert "Failed task resume rules run in the same ordered rule engine." not in failed_task_section
    assert "| Otherwise | `resume` — create resume task and spawn worker |" not in failed_task_section

    assert "| `resume` | Creates resume task, spawns worker |" in worker_actions_section
    assert "| `retry` | Creates retry task, spawns worker |" in worker_actions_section

    assert "`create_plan_review`, `create_plan_improve`, `create_review`, `create_implement`, `resume`, `retry`, `needs_rebase`" in output_section
    assert "created/reused task ID" in output_section
    assert "`awaiting_human` — review the plan, then run `uv run gza implement <id>`" in internal_content


def test_docker_setup_command_docs_describe_prewarm_hook_and_race_avoidance() -> None:
    """Docker config docs should explain pre-warm semantics and why first-use lazy installs race."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    docker_content = (docs_root / "docker.md").read_text()

    required_config_snippets = [
        "### Docker Pre-Warm Hook (`docker_setup_command`)",
        "Runs synchronously inside the container before the provider CLI process starts.",
        "so setup does not race with parallel tool calls or subagents.",
        "dependency installs are often lazy on the first CLI invocation.",
        'docker_setup_command: "uv sync"',
        "poetry install --no-interaction",
        "pip install -e .",
        "npm ci",
    ]
    for snippet in required_config_snippets:
        assert snippet in config_content

    required_docker_snippets = [
        "## Pre-Warm Dependencies with `docker_setup_command`",
        "Runs inside the container before the provider CLI starts.",
        "Runs synchronously in a single process.",
        "Completes before the agent can issue tool calls.",
    ]
    for snippet in required_docker_snippets:
        assert snippet in docker_content


def test_improve_related_skills_describe_comments_as_feedback_source() -> None:
    """Bundled improve-related skills should mention unresolved task comments as a first-class
    feedback source and describe the comments-only fallback when no review exists.

    Regression: `gza-task-improve/SKILL.md` and `gza-task-add/SKILL.md` previously framed improve
    purely around review feedback, which steered operators and agents away from valid
    comments-only improve flows after the feature landed.
    """
    repo_root = Path(__file__).resolve().parents[1]

    improve_skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-improve" / "SKILL.md"
    ).read_text()
    assert "unresolved task comments" in improve_skill_content
    assert "comments-only" in improve_skill_content
    assert "resolve_comments" in improve_skill_content
    assert "kinds=('feedback',)" in improve_skill_content
    assert not re.search(
        r"store\.get_comments\(\s*impl_task\.id,\s*unresolved_only=True\s*\)",
        improve_skill_content,
    )
    assert not re.search(
        r"store\.resolve_comments\(\s*'<IMPL_TASK_ID>'\s*\)",
        improve_skill_content,
    )

    add_skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-add" / "SKILL.md"
    ).read_text()
    assert "unresolved task comments" in add_skill_content
    assert "comments-only improve is supported" in add_skill_content

def test_skill_install_docs_and_internal_task_model_match_importer_cleanup() -> None:
    """Skills docs and internal task-model docs should reflect current refresh/import guidance."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_content = (repo_root / "docs" / "skills.md").read_text()
    task_model_content = (repo_root / "docs" / "internal" / "task-model-canonical.md").read_text()

    assert "refresh existing copies with `gza skills-install --update`" in skills_content
    assert "get overwritten by `gza skills-install`" not in skills_content
    assert "retired importer-specific entry points" in task_model_content
    assert "importer/config flows" not in task_model_content


def test_skills_docs_describe_spec_coherence_as_behavior_spec_set_gate() -> None:
    """Operator docs should describe the new behavior-spec coherence gate accurately."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_content = (repo_root / "docs" / "skills.md").read_text()

    assert "## gza-spec-coherence" in skills_content
    assert "author-side gate on `specs/behavior/**`" in skills_content
    assert "not against the code" in skills_content
    assert "repeated vocabulary or invariants that should cross-reference `00-overview.md`" in skills_content
    assert "reviews/<timestamp>-spec-coherence.md" in skills_content


def test_lineage_spec_and_operator_docs_define_stale_unmerged_sweep_contract() -> None:
    """The stale-unmerged maintenance rule should be codified in both the behavior spec and operator docs."""
    repo_root = Path(__file__).resolve().parents[1]
    lineage_spec = (repo_root / "specs" / "behavior" / "lineage.md").read_text()
    config_docs = (repo_root / "docs" / "configuration.md").read_text()

    assert "### L5 — Stale unmerged sweep" in lineage_spec
    assert "The sweep MUST NOT drop a candidate when any external" in lineage_spec
    assert "`depends_on` edge still points to or from a lineage that remains unresolved" in lineage_spec
    assert "non-network merge-truth semantics as plain default-target `gza unmerged`" in lineage_spec
    assert "Dry-run by default." in lineage_spec
    assert "MUST NOT delete branches or discard branch provenance as part of the sweep." in lineage_spec

    assert "live unresolved lineages" in config_docs
    assert "Historical edges to already resolved external work" in config_docs
    assert "re-checks those candidates against the canonical default target" in config_docs
    assert "proof error aborts the command before mutation" in config_docs


def test_cli_help_and_skill_docs_use_decimal_task_id_examples() -> None:
    """CLI help and bundled skills should avoid legacy base36 task-ID examples."""
    repo_root = Path(__file__).resolve().parents[1]
    main_content = (repo_root / "src" / "gza" / "cli" / "main.py").read_text()
    config_cmds_content = (repo_root / "src" / "gza" / "cli" / "config_cmds.py").read_text()

    assert "gza-1234" in main_content
    assert "Full prefixed implementation task ID to iterate" in main_content
    assert "gza-1a2b" not in main_content
    assert "{prefix}-{decimal}" in config_cmds_content
    assert "{prefix}-{base36}" not in config_cmds_content
    assert "gza-1a2b" not in config_cmds_content

    skill_names = [
        "gza-explore-summarize",
        "gza-plan-review",
        "gza-plan-improve",
        "gza-task-run",
        "gza-task-resume",
        "gza-task-improve",
        "gza-task-fix",
        "gza-task-review",
        "gza-task-info",
        "gza-task-debug",
    ]
    for skill_name in skill_names:
        content = (repo_root / "src" / "gza" / "skills" / skill_name / "SKILL.md").read_text()
        assert "gza-1234" in content
        assert "gza-1a2b" not in content
