# Main verify self-heal contract

> **Status: Draft north-star contract.** This document defines the required convergence
> behavior for a red local-target integration verify verdict. The implement tasks
> (`gza-5778`, `gza-5856`, the checkpoint-TTL task, and the deterministic-red repair
> task) realize this contract over time; until then, code/spec mismatches are behavior
> findings, not license to weaken the contract.

## What this owns

This document owns one question:

- When the shared local-target integration verify gate goes red, how must automation
  recover or escalate so the pipeline still converges?

It does **not** own:

- The ordinary lifecycle transition table. That lives in
  [lifecycle-engine.md](lifecycle-engine.md).
- The watch-loop phase ordering, capacity accounting, or restart-safe no-progress
  mechanics. Those live in [watch-supervisor.md](watch-supervisor.md).

This file is the north-star contract that those two documents must apply whenever the
local-target verify gate is not green.

## Terms

- **Red verdict** — any configured local-target integration verify result whose status is
  not `passed`.
- **Checkpoint** — the durable recorded result reused to decide whether more merges onto
  the canonical local target are allowed.
- **Merge freeze** — the state where automation halts further merges onto the canonical
  local target because the checkpoint is red or freshness is unproven.
- **Launch stall** — downstream work stops launching or making progress for reasons that
  are only an artifact of the merge freeze rather than the work's own state.

## Contract

### MV1 — Red verify state MUST converge

A configured local-target integration verify gate MUST NOT leave the system in an
unbounded freeze. A merge freeze MAY stop further merges onto the canonical local target,
but it MUST always converge by one of these bounded outcomes:

1. the gate reruns and turns green;
2. the gate reruns and confirms a deterministic red, which then enters bounded repair
   plus visible alerting; or
3. the gate becomes a visible human-required condition with an explicit bounded reason.

A merge stall MUST NOT convert into a launch stall.

### MV2 — Red verdicts MUST be re-verified before automation acts on them

Before automation reuses a red checkpoint to keep merges halted, park work, or emit a
durable red-main attention row, it MUST rerun the local-target verify gate against the
current canonical local-target tree.

- A stale red verdict MUST NOT be reused indefinitely.
- The rerun sequence MUST be bounded but MUST include at least one retry of a red
  verdict before automation treats it as actionable red state.
- A flake that passes on rerun MUST self-clear: the prior red checkpoint is replaced, the
  merge freeze ends, normal merge planning resumes without requiring a human to manually
  delete or override the old red state, and the failure is reclassified as flaky rather
  than deterministic.
- The rerun freshness proof MUST be against the exact current local-target tree. If exact
  tree freshness cannot be proven, automation MUST fail closed but treat that as a
  freshness problem to be refreshed again, not as permanent proof that the old red
  verdict remains valid forever.

### MV3 — Red checkpoints MUST have a bounded lifetime even on an unchanged tree

The durable checkpoint for a red local-target verify result MUST auto-expire after a
bounded TTL, even when the local-target tree fingerprint and verify-gate identity are
unchanged. Verify-gate identity includes the environment identity that produced the
checkpoint whenever the gate is configured.

- The bound itself is policy; the existence of the bound and its enforcement are
  contract.
- After the TTL expires, the next lifecycle decision that would reuse that red checkpoint
  MUST rerun the local-target verify gate and replace the checkpoint with fresh evidence.
- Automation MUST NOT treat "same tree, same gate, same old red checkpoint" as sufficient
  reason to freeze merges forever.
- Automation MUST also fail closed on legacy or mismatched configured-gate checkpoints:
  if the current gate requires an environment identity and the stored checkpoint either
  lacks that identity or records a different one, the checkpoint is stale and MUST be
  refreshed before it can justify a merge freeze.

### MV4 — Confirmed deterministic red MUST trigger bounded repair plus alert

When rerun verify confirms a real deterministic failure on the current canonical
local-target tree, automation MUST halt further merges onto that target and MUST trigger
both:

- one visible durable alert naming the red-main condition and conveying how long the
  gate has been continuously red; and
- one bounded automatic repair path aimed at restoring a green local target or reaching a
  clear human-required stop.

The repair path MUST distinguish flaky from deterministic verify failures:

- A verdict that turns green during the bounded rerun sequence is **flaky**. Automation
  MUST NOT keep merges halted for that failure, and the supervisor MUST create or reuse
  exactly one active remediation attempt for that failure identity, backed by a
  remediation task that aims to de-flake it.
- A verdict that stays red across the full bounded rerun sequence is **deterministic**.
  Automation MUST halt merges for that failure, and the supervisor MUST create or reuse
  exactly one active remediation attempt for that failure identity, backed by a
  remediation task that aims to fix the failing phase or gate.
- That automatic remediation path MUST also be representative of the observed failing
  verify environment. The bounded rerun evidence MUST carry the observed environment
  identity (at minimum runner class plus host/container-relevant runtime traits) into
  remediation metadata and prompt text. Before watch launches or requeues remediation,
  it MUST compare that observed environment with the execution environment the worker
  would actually use. If the worker environment cannot represent the observed failure
  (for example a host/Darwin red that would be retried only in a Linux container), or if
  Docker probing cannot prove the actual worker runtime and it is therefore
  unknown/unavailable, watch MUST NOT queue or requeue an ordinary code-remediation
  task. Instead it MUST keep the merge freeze in place and surface exactly one durable
  human-attention condition for that mismatch identity until fresh representative
  evidence replaces it.
- A verify phase or verify tool that cannot be launched because the environment is
  misconfigured (for example: missing executable, not-on-PATH tool, non-executable
  tool, or shell-level `command not found`/exit-127 launch failure) is **not** a
  deterministic red. Automation MUST surface that as a visible operator attention
  condition that names the missing or non-runnable tool and tells the operator to fix
  the environment rather than the code. That condition MUST NOT mark main red, MUST NOT
  halt merges, and MUST NOT create or reuse a code-remediation task.
- Remediation task dedup is by failure identity, not by watch cycle. That identity is
  the normalized failure signature only. The exact local-target tree fingerprint from
  the bounded rerun evidence remains prompt context and freshness evidence, but it MUST
  NOT decide whether the supervisor creates a new remediation row. Re-observing the
  same unresolved signature with a different, newly available, stale, or unavailable
  fingerprint MUST reuse the existing active remediation attempt for that identity
  instead of filing another copy. Pending, in-progress, and completed-but-unmerged
  remediation tasks all qualify as that one active attempt until post-merge verify
  classifies the outcome. If the current bounded rerun evidence changes the required
  remediation kind, fingerprint context, or other prompt evidence for that same
  signature, the reused task MUST be updated in place so its prompt still matches the
  current classification, except that a row already `in_progress` MUST keep the prompt
  evidence, tags, urgency, and queue state the running worker actually received. If a
  same-signature non-live row also exists, watch MUST prefer that non-live row for
  refresh or requeue and leave the live duplicate untouched until worker-aware
  reconciliation can retire it. If the live row is the only same-signature match, watch
  MUST keep it as the signature-owned open row but limit any updates to safe freshness
  bookkeeping that does not misrepresent the running worker.
  A post-merge verify rerun that turns green for the same remediation identity MUST clear
  the active attempt without consuming the budget. A post-merge rerun that is red for a
  different identity or lacks a trustworthy identity match MUST fail closed on reuse for
  the old task, but MUST NOT consume that old task's attempt budget.
- Reused or newly created remediation tasks for this gate MUST be bumped to the front of
  the runnable queue, because a red or flaky local-target verify is pipeline-critical
  system work.
- Reused or newly created remediation tasks for this gate MUST carry the distinctive tag
  `system-main-verify` in addition to the inherited `system` and scope tags so operators
  can filter main-verify state rows and remediation work together.
- Reused or newly created remediation tasks for this gate MUST include bounded rerun
  evidence in the prompt: the failure signature, the observed tree fingerprint context,
  the spent-attempt metadata line `Remediation attempts spent: N/2`,
  a persisted verify artifact reference only when the referenced artifact file is still
  readable and yields content-bearing output, parsed failing pytest node IDs when
  available from existing verify evidence, and a trimmed verify-output excerpt. If the
  preferred persisted artifact reference is missing, unreadable, invalid, empty, or
  whitespace-only, the supervisor MUST keep scanning newer `verify_command_output`
  artifacts newest-first and use the first readable content-bearing one; if no
  content-bearing verify artifact exists, it MUST omit the artifact reference, parsed
  node IDs, and excerpt instead of surfacing stale prompt evidence.
  The prompt MUST keep that evidence bounded and deterministic; it MUST NOT embed an
  unbounded verify log.
- Reusing the same remediation row after a failed automatic attempt MUST be bounded and
  sequential. Watch MUST track the consumed automatic attempts on that single row,
  increment that state before requeueing a failed remediation, and stop requeueing once
  the configured bound is spent. Legacy failed remediation rows that predate explicit
  attempt metadata MUST be treated conservatively as already having spent one automatic
  attempt.
- When the automatic remediation bound is exhausted for a failure signature, watch MUST
  leave the single remediation row failed, persist an explicit exhausted reason on that
  row, and emit one signature-scoped human-attention condition instead of creating or
  queueing another remediation task for the same unresolved signature.
- When watch parks remediation because the available worker environment is not
  representative of the observed failure, it MUST emit one durable mismatch attention
  row for that observed failure identity instead of creating or queueing a normal
  remediation task. Re-observing the same mismatch on the same failure identity MUST
  reuse that existing durable attention rather than churning new rows or task IDs.
- When main verify later turns green and automation can safely identify the cleared
  failure signature, watch MUST retire matching open remediation rows for that signature
  as moot so stale main-verify fixes do not remain runnable after the gate has already
  recovered. If a matching remediation row is still `in_progress` during the green
  transition, watch MUST preserve the live worker's prompt and queue metadata, persist
  a durable signature-scoped retire marker outside the prompt, and apply that
  retirement before later lifecycle merge handling once the worker exits. A same-
  signature red dedupe pass MUST NOT rewrite prompt evidence or queue position for a
  live `in_progress` remediation row unless watch actually restarts that worker with
  the new prompt.
- While that deterministic red freeze is active, watch MUST continue skipping unrelated
  merge actions for the cycle, but it MUST allow the merge of the one completed
  remediation implement task that is the active `system-main-verify` **fix** for the
  current failure identity. That exemption is owned by the watch supervisor and MUST be
  authorized only when the merge subject matches the active remediation by trigger
  source, remediation kind, failure signature, and exact tree fingerprint when one is
  available from the bounded rerun evidence. If the active evidence has no tree
  fingerprint, the exemption MUST stay conservative and only match a remediation prompt
  that likewise records fingerprint unavailability.
- After that exempt remediation merge, watch MUST immediately rerun local-target verify
  against the post-merge local target tree before allowing any later merge in the same
  cycle. Only a green rerun clears the freeze. If the rerun is still red, automation
  MUST keep the freeze in place, MUST surface the durable red-main attention, and MUST
  create or reuse the next remediation task through the same failure-identity dedup path.

That repair path MUST itself be bounded. It MUST NOT silently freeze the merge lane
without either making bounded repair attempts or surfacing a human-required condition.

### MV5 — Red merge freezes MUST NOT hard-park downstream work

A merge freeze caused by red local-target verify MUST NOT hard-park downstream tasks only
because merges are currently halted.

- Work that is otherwise runnable MUST remain runnable.
- Work whose next meaningful action is blocked by the freeze MAY remain waiting, but it
  MUST stay visible and re-evaluable rather than being converted into a permanent parked
  state solely because the target is red.
- The shared no-progress backstop MUST ignore repeated evaluation of a blocked merge lane
  by itself. But once watch has already selected the same downstream subject/action on an
  unchanged subject, both executed no-op repeats and undispatched selected repeats count
  toward the shared backstop.

This is what prevents a merge stall from cascading into a launch stall.

### MV6 — Operators MUST have a force-refresh escape hatch

There MUST be a first-class operator command that forces a fresh local-target verify run
for the gate, ignoring a cached checkpoint.

- If the forced rerun goes green, it MUST replace the cached red checkpoint and clear the
  merge freeze without requiring code edits or a direct commit to the canonical target.
- If the forced rerun stays red, it MAY leave the freeze in place, but it MUST still
  leave behind fresh evidence rather than the stale cached checkpoint.

### MV7 — Candidate verify MUST prevent isolated red-main promotion without poisoning canonical state

When watch stages default-branch merges in an isolated detached checkout, that staged
tree is only a candidate. A red or freshness-unproven candidate verify verdict MUST keep
the canonical default branch unchanged and MUST NOT be persisted as proof that canonical
main itself is red.

- Watch MAY batch multiple isolated merge candidates into one staged detached tree, but
  it MUST verify that exact staged tree before promotion.
- A passing candidate verify verdict MAY become the canonical checkpoint only after
  promotion proves the canonical target now matches the exact verified candidate tree.
  If exact identity cannot be proven cheaply, automation MUST rerun canonical post-merge
  verify instead of trusting the candidate evidence.
- If a batched staged tree verifies red, watch MUST isolate the first red-producing
  merge unit with bounded replay, route that unit to visible rework, and leave unrelated
  canonical-main freeze state unchanged.
- Candidate-red routing MUST create or reuse exactly one queued rework task for the
  failure identity and emit distinct operator-visible blocked-candidate attention.
- A blocked candidate MUST NOT create a global red-main merge freeze for unrelated work.

## Cross-document requirements

- [lifecycle-engine.md](lifecycle-engine.md) MUST own the action semantics for the
  `main-integration-verify-red` attention path without weakening MV1-MV5.
- [watch-supervisor.md](watch-supervisor.md) MUST own the loop-level freshness checks,
  rerun timing, remediation-task creation/dedup/bumping, and no-progress accounting
  without weakening MV1-MV6.
- Future behavior-check findings against this area MUST classify implementation drift
  against **this** document as the source contract, not treat the current implementation
  as normative.

## Implementation note

The intended realization is split deliberately:

- `gza-5778` supplies rerun-before-reuse so flakes self-clear.
- The checkpoint-TTL task bounds red lifetime on unchanged trees.
- The deterministic-red repair task supplies bounded auto-repair plus alerting.
- `gza-5856` ensures merge freezes do not cascade into watch no-progress launch stalls.
