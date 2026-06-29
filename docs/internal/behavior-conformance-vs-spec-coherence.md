# Behavior Conformance Vs Spec Coherence

This note captures the core design split behind the behavior-spec automation added for `gza-4551`.

## Why these are two different checks

There are two distinct questions:

1. Does the whole current system still conform to the behavior-spec contract?
2. Are the behavior-spec edits on this branch well-authored and coherent?

Those questions have different scope, cost, and failure modes, so Gza automates them differently.

## Why whole-system conformance is cadence-based

`/gza-behavior-check` is a whole-system check. It reads the full behavior-spec set, compares that contract to the current implementation, and can surface divergence that predates or is unrelated to the branch an operator is trying to merge.

That makes it a poor pre-merge gate:

- It can block unrelated work on historical divergence.
- It is evidence-heavy and agent-mediated rather than a cheap deterministic unit test.
- Its value is continuous detection of regressions and surviving contract drift, not branch-local authorship validation.

So the load-bearing automation is a separate host-side cadence loop: `uv run gza behavior-monitor`. The monitor re-runs the whole-system check on a schedule, dedupes findings, and files repair tasks into the ordinary queue. This matches Gza's async-resilience model: the system keeps checking, and missed human vigilance is not the control plane.

## Why spec coherence is a per-change merge gate

`/gza-spec-coherence` is different. It is branch-scoped and only applies when the merge diff touches `specs/behavior/**` (or other configured `spec_coherence.paths`).

Its job is not to judge the implementation. Its job is to judge the changed specs as contract text:

- plain and atomic normative clauses
- non-overlapping ownership
- correct cross-references to shared vocabulary and invariants
- open questions called out explicitly
- implementation details confined to clearly marked notes

Because that check is scoped to the changed spec files on the branch, it is appropriate as a hard pre-merge gate. If the contract text itself is unclear or overlapping, letting it merge would make later code-review and conformance automation less trustworthy.

## Operational consequence

The split is intentional:

- `behavior-monitor` is continuous, asynchronous, and whole-system. It files work.
- `spec-coherence` is synchronous, branch-local, and merge-blocking. It guards contract authorship quality.

Do not collapse them into one gate. That would either make unrelated historical divergence block unrelated work, or weaken the spec-authoring gate until ambiguous contract text could merge unchecked.
