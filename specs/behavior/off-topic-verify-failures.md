# Off-topic verify failures

> **Status: Draft.** This file owns the contract for the optional lifecycle path that
> clears a verify-only review block when later red verify evidence is audited as
> off-topic to the reviewed branch. Read [00-overview.md](00-overview.md) first for the
> shared model, then [lifecycle-engine.md](lifecycle-engine.md) for rule ordering. This
> file defines the classification contract only; it does not authorize runtime shortcuts
> outside that lifecycle flow.

## Boundary with the lifecycle engine

- [lifecycle-engine.md](lifecycle-engine.md) owns when lifecycle may consult this
  contract and what action it takes afterward.
- This file owns what **off-topic** means, what evidence MUST exist before lifecycle may
  clear a verify-only review blocker, and what investigation record MUST be created or
  reused when it does.
- This contract applies only to verify-only `CHANGES_REQUESTED` review blockers. Any
  substantive review blocker remains governed by the ordinary improve/review flow.

## Policy knob

The lifecycle policy knob `advance_off_topic_verify_unblock` MUST exist and MUST default
to **off**.

- With the knob **off**, lifecycle MUST keep the current conservative behavior: a fresh
  red verify result after a no-op improve remains blocking and the lineage follows the
  normal park path.
- With the knob **on**, lifecycle MAY clear the review block only when every precondition
  and classification rule in this file holds. It MUST otherwise fail closed and keep the
  review blocking.

This is a single swappable policy point. Projects MAY tune only whether the off-topic
lane is enabled; they MUST NOT weaken the evidence requirements below implicitly.

## Preconditions

Lifecycle MUST consult this contract only when all of the following are true:

1. The latest completed review verdict is `CHANGES_REQUESTED`.
2. The blocker set is verify-only: there are no substantive code, scope, or other
   non-verify blockers still open on that latest review.
3. The branch has current trusted green verify evidence for the exact reviewed head SHA
   and exact tree fingerprint now under consideration.
4. The later red verify evidence was produced for that same reviewed head SHA and exact
   tree fingerprint.

Trusted green evidence MUST be runner-owned or otherwise durably recorded lifecycle
evidence. Reviewer prose, historical anecdotes, or green evidence from a different head
or tree fingerprint MUST NOT satisfy this precondition.

If exact reviewed-head or exact tree-fingerprint matching cannot be established for both
the green and red evidence, lifecycle MUST fail closed and keep the review blocking.

## Full failing-node enumeration

Lifecycle MUST enumerate the full failing-node set before classifying a red verify result
as off-topic.

- The enumeration run MUST disable fail-fast behavior so lifecycle can see the full set
  of failing nodes for that red outcome.
- The classifier MUST identify every failing node it relies on by stable node identity
  plus the parsed assertion or failure signature when available.
- If lifecycle can see only the first failure, cannot rerun without fail-fast, cannot
  parse stable failing-node identities, or cannot tell whether the observed failures are
  complete, classification MUST be `unknown` and the review MUST remain blocking.

Lifecycle MUST classify every enumerated failing node. One unclassified node is enough to
keep the review blocking.

## Off-topic classification

Classification is per enumerated failing node, but lifecycle MAY clear the review only
when the full failing-node set resolves to off-topic under one of the branches below.

### OTV1 — Deterministic off-topic

A failing node is deterministic off-topic only when the same node also fails on the
canonical local merge target under an equivalent non-fail-fast verify command.

- The target-side run MUST use the local canonical merge target, not `origin/*` or any
  remote-only reference.
- If a node fails on the reviewed branch but does not fail on the local target under the
  required target-side check, lifecycle MUST NOT classify that node as deterministic
  off-topic.
- Deterministic off-topic classification MUST still fail closed if lifecycle cannot prove
  that the target-side failure and reviewed-branch failure refer to the same normalized
  node identity plus failure signature.

### OTV2 — Intermittent off-topic

A failing node is intermittent off-topic only when all of the following are true:

1. The same reviewed head SHA and exact tree fingerprint already produced trusted green
   verify evidence and later produced trusted red verify evidence.
2. The failing node and every parsed assertion, traceback, or failure-location path that
   lifecycle can attribute to the failure are outside the reviewed branch diff.
3. The reviewed diff does not touch a shared, global, orchestration, concurrency, or
   similarly cross-cutting area that could plausibly affect the failure without appearing
   in the node path itself.

If condition 3 is not true because the diff touches such a cross-cutting area, lifecycle
MUST require a bounded target-side stress baseline before clearing the review. Without
that extra target-side evidence, intermittent classification MUST fail closed.

For this cross-cutting intermittent path, the target-side stress baseline is conclusive
only when it reproduces the same normalized failing-node identity plus the same
normalized failure signature that appeared on the reviewed branch, or when lifecycle has
another equally explicit proof that the reviewed-branch red is off-topic to the diff.
The stress baseline is inconclusive and MUST keep the review blocking when the local
target stays green, reproduces a different node identity or failure signature, times out
without parseable same-signature evidence, or otherwise produces unparseable or
ambiguous stress results.

## Branch-introduced and unknown failures

Lifecycle MUST keep the review blocking when any enumerated failing node is:

- Branch-introduced: the failing node itself, or any parsed assertion/traceback/failure
  location for that node, points into the reviewed diff.
- Unknown: lifecycle cannot confidently determine whether the failure is deterministic
  off-topic, intermittent off-topic, or branch-introduced.
- Unscoped: lifecycle cannot obtain a trustworthy reviewed diff or cannot relate the
  failure evidence to that diff.
- Untrusted: the required green or red evidence is missing, stale, or not bound to the
  same reviewed head SHA and exact tree fingerprint.

One branch-introduced or unknown node keeps the entire review blocking even if other
enumerated nodes appear off-topic.

## Clearance must be audit-bound

When lifecycle clears a verify-only review blocker through this contract, the clearance
MUST be bound to the exact reviewed head SHA and exact tree fingerprint that satisfied
the preconditions and classification rules above.

- Lifecycle MUST NOT treat a clearance recorded for one head or fingerprint as reusable
  proof for a different head or fingerprint.
- If later automation cannot prove it is still looking at that same head and exact tree
  fingerprint, it MUST discard the clearance and reevaluate or fail closed.
- The clearance record MUST be inspectable after the fact and MUST include the reviewed
  head SHA, exact tree fingerprint, and the full enumerated failing-node set that was
  classified off-topic.

## Investigation lane: REPRODUCE-OR-RECORD

Every off-topic clearance MUST create or reuse exactly one non-blocking investigation
record for each normalized failing-node signature.

- The investigation is non-blocking for merge eligibility; lifecycle MUST NOT hold the
  cleared reviewed branch open waiting for that investigation to finish.
- Dedup intent is by normalized failing-node signature: at minimum stable node identity
  plus normalized assertion or failure signature when available. Equivalent later
  recurrences SHOULD reuse the open investigation instead of spawning duplicates.
- The investigation contract is `REPRODUCE-OR-RECORD`: it MUST either reproduce the
  failure under a bounded stress harness and then fix with proof, or close with a
  structured inconclusive record that preserves the attempts, environment, and observed
  evidence for future recurrence handling.
- The investigation lane MUST prefer root-cause evidence. It MUST NOT default to blanket
  sleeps, retries, `@flaky`, or broad timeout inflation as a speculative remedy.

If lifecycle cannot durably create or reuse the required investigation record, it MUST
fail closed and keep the original review blocker in place.

## Fail-closed summary

Lifecycle MUST keep the review blocking whenever any of the following is true:

- `advance_off_topic_verify_unblock` is off.
- The latest review is not verify-only blocked.
- Trusted green evidence is missing for the same reviewed head SHA and exact tree
  fingerprint.
- The later red evidence is not bound to that same reviewed head SHA and exact tree
  fingerprint.
- Full failing-node enumeration is unavailable or incomplete.
- Any enumerated failing node is branch-introduced, unknown, or untrusted.
- Intermittent classification needs target-side stress baseline evidence and that
  evidence is absent or inconclusive.
- A required target-side stress baseline stays green or otherwise fails to reproduce the
  same normalized failing-node identity plus normalized failure signature.
- A required target-side stress baseline reproduces only a different node identity or
  failure signature.
- A required target-side stress baseline times out without parseable same-signature
  evidence, or otherwise yields unparseable or ambiguous results.
- The clearance cannot be durably audit-bound to the reviewed head SHA and exact tree
  fingerprint.
- The required non-blocking investigation record cannot be created or reused.
