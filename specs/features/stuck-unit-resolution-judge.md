# Stuck-Unit Resolution Judge

An automated, budget-capped LLM judge that resolves merge units parked in
*judgment* terminal states — the cases a deterministic reviver provably can't
clear — so they stop requiring a human operator.

## Problem

The lifecycle deliberately halts a merge unit when automatic recovery runs out
of *safe* moves and the next step needs judgment. These guardrails are correct:
they exist to stop infinite retries and unbounded spend without an intelligent
observer. Today that observer is a human, and clearing the parked pile is a
recurring, hours-per-day manual task.

The parked population splits cleanly into two kinds (the `next_action` /
`next_action_reason` that `gza incomplete --json` already computes — the same
classification `advance` emits):

- **Mechanical / transient** — `advance`-able lifecycle steps, infra `retry`,
  in-progress rebases, satisfied dependencies, and `moot` units (which should
  auto-resolve to `redundant`; see [`improve-task-type.md`] and the no-op
  classification work). A deterministic reviver (`scripts/revive_stuck.py`)
  handles these.
- **Judgment** — `retry limit reached`, `no-op improve`
  (`advance_engine.py` emits these as `needs_discussion`), `GIT_ERROR requires
  manual` (often a *false positive*: a read-only DB or corrupt worktree, not a
  real conflict), `rebase circuit breaker tripped`, `max_plan_review_cycles`,
  `invalid slice manifest`. Deterministic rules **cannot** safely resolve these,
  and narrow per-shape predicates that have tried (e.g. "verify-blocked ⇒
  mergeable", "reviewer-is-wrong") repeatedly mis-fire.

A blind `advance --force` on a parked unit is **not** an intelligent observer —
it removes the guardrail with a rate limiter. Randomness and slowness bound the
blast radius, not the correctness: forcing a genuinely-stuck unit either no-ops
(wasted spend) or pushes bad work toward `main`. The decision these units need —
*"is this halt a false positive, does it deserve one more targeted attempt, or
is it dead?"* — requires reading the diff, the review, and the logs and
reasoning about them. That is irreducibly a judgment call.

## Approach

Add a judgment tier that runs *behind* the deterministic reviver, in the
"automated tool the operator runs" pattern (like `gza watch -y` /
`scripts/revive_stuck.py`). For each unit still parked in a judgment bucket, make
one focused LLM call that reads the unit's evidence and returns a structured
decision, then dispatch the corresponding release / retry / drop action through
existing CLI primitives — all under hard spend and loop guardrails.

The guiding principle is **one general judge at the park boundary, not another
narrow per-shape predicate.** That generality is the whole point: it is the
antidote to the mis-fire history of one-off heuristics.

This is the park-boundary scoping of the budget-capped auto-retry + relevance-
judge concept already on the roadmap (gza-6207); it should most likely be
implemented under that plan rather than as a parallel thread.

## Scope

**In scope** — units whose `next_action` the system itself flags as needing
judgment:

| Bucket | `next_action` | Typical resolution |
| --- | --- | --- |
| Retry limit reached | `skip` | `retry_once` with guidance, or `drop` |
| No-op improve | `needs_discussion` | `release` (reviewer satisfied / moot feedback) or `retry_once` |
| GIT_ERROR manual | `skip` | `release` if false-positive, else `escalate_human` |
| Rebase circuit breaker | `needs_discussion` | `retry_once` (fresh rebase) or `escalate_human` |
| Max plan-review cycles | `needs_discussion` | `release` or `escalate_human` |
| Invalid slice manifest | `needs_discussion` | `retry_once` (regenerate manifest) |

**Out of scope** — handled elsewhere, must not be touched here:

- **Moot** → auto-resolves to `redundant` (no-op-misclassification work); never
  `drop` a moot unit from this judge.
- **Mechanical / transient** (`resume`, `create_review`, `improve`, infra
  `retry`, `reconcile_branch_divergence`, in-progress rebases) → deterministic
  `scripts/revive_stuck.py`.

## Decision schema

For each parked unit the judge reads: `next_action` + `next_action_reason` (why
it parked); the unit diff vs target (`gza diff`); the latest review findings /
unresolved comments; recent lineage task logs and failure reasons; and the
target-branch state (is the work already landed?). It returns exactly one:

```python
@dataclass
class ParkVerdict:
    decision: str        # release | retry_once | drop | escalate_human
    rationale: str       # why — logged, not posted as a gza comment
    guidance: str | None # for retry_once: concrete, targeted instructions
```

- **`release`** — the halt is a false positive (reviewer wrong, verify-blocked
  but actually passing, work already satisfied). Clear the park/exclusion state
  so `watch` re-engages the unit normally.
- **`retry_once`** — deserves one more *targeted* attempt. Emit a fix/improve
  task carrying `guidance` (never a blind re-run).
- **`drop`** — genuinely dead or superseded.
- **`escalate_human`** — truly ambiguous; leave parked. This is the residual,
  and it should shrink as the judge improves.

## Guardrails

These bound spend and prevent the judge from re-creating the infinite-loop
problem the original park guardrails were protecting against:

- **Budget cap** — a per-period token/$ ceiling (shared with gza-6207's budget).
  When exhausted, stop and leave units parked.
- **Per-unit cooldown** — a unit cannot be re-judged until its state changes or
  N hours pass. Kills the judge → retry → park → judge cycle.
- **Per-unit attempt cap (K)** — after K judge-driven attempts on one unit, force
  `escalate_human`. Honors the original "do not spend infinitely" intent.
- **Audit, not comments** — log every decision and rationale to the tool's own
  log/state. Do **not** use `gza comment` (unresolved comments spawn improve
  tasks and would feed the loop).
- **Dry-run mode** — mirror `revive_stuck.py --dry-run`: show every decision,
  execute nothing.

The operator has stated they accept a small mistake rate; the budget cap and
attempt cap bound the downside.

## Integration & reuse

- **Discovery:** `gza incomplete --json` (`next_action` / `next_action_reason`),
  same source `scripts/revive_stuck.py` already uses.
- **Evidence:** `gza show` / `gza diff`; review verdict + issue extraction can
  reuse `parse_review()` from [`llm-judgment-points.md`], and failure
  categorization can reuse / extend `analyze_failure()` from the same module.
- **LLM call:** reuse the lightweight `llm._call()` pattern (cheap model,
  structured JSON out, deterministic fallback) from [`llm-judgment-points.md`].
- **Actions:** existing release primitives — `clear_review_state`, and the
  clear-only `gza unstick` (backstop + reconcile) from the PRK unstick work.
  Retry-limit and no-op-improve likely need their own release path built.
- **Pairing:** runs as the second tier of the same operator loop —
  `revive_stuck.py` clears the mechanical buckets first, then this judge takes
  whatever remains in a judgment bucket.

## Cost

One judgment per parked unit, gated by cooldown so a unit is judged at most once
per state-change. Context is bounded (reason + truncated diff + truncated review
+ log tail). With a cheap model this is well under a cent per judgment; the
period budget cap is the hard ceiling regardless.

## Testing

- The judge has a deterministic fallback (`escalate_human` when the LLM call
  fails or returns invalid JSON), so tests run without API access.
- Unit tests mock the LLM call to return each `decision` and assert the correct
  dispatch (release verb / retry task / drop / leave) and that guardrails fire
  (cooldown blocks re-judge; attempt cap forces escalate).
- Dry-run integration test over a snapshot of `gza incomplete --json` asserting
  the bucket routing.

## Success metric

Not throughput or queue depth — the auto-merge-health dashboard's
**human-required terminal-unit count** should fall. Additionally track judge
decisions against outcomes: did `release`d units actually merge, and did
`retry_once` units land? A rising `escalate_human` share signals the judge is
hitting its competence boundary and the prompt/evidence needs work.

## Open questions

- **Where it lives** — a new `gza` subcommand, an extension of
  `scripts/revive_stuck.py`, or folded directly into gza-6207's implementation.
- **Release primitives** — `clear_review_state` and `gza unstick` exist
  (backstop/reconcile only); retry-limit and no-op-improve parks likely need a
  dedicated release path.
- **Model / context budget** — which model, and how much diff/review/log context
  per judgment.
- **Relationship to gza-6207** — confirm this is implemented *as* the park-
  boundary scope of that plan rather than a separate feature.

## Related

- [`llm-judgment-points.md`] — reusable `llm._call()`, `parse_review()`,
  `analyze_failure()`; complementary (heuristic replacement at existing decision
  points vs. this autonomous park-boundary loop).
- [`async-human-in-the-loop.md`] — the human-escalation side this judge feeds
  via `escalate_human`.
- [`review-improve-loop.md`] — the loop whose terminal parks this judge resolves.
