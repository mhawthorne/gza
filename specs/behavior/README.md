# Behavior specs

This directory holds gza's **behavior specs** — the prescriptive, normative description of
*how we want the system to behave*, not how any particular piece of code currently happens
to behave. They function as a **contract**: when the code disagrees with a behavior spec,
that disagreement is a finding to resolve, not a doc to silently update.

**Behavior specs vs. feature specs.** `specs/features/` holds *feature specs* — proposals
for features, codified in a file before becoming tasks. Those are *wishes*. Behavior specs
here (`specs/behavior/`) are *requirements*: the binding behavior the running system must
satisfy. Same root (`specs/`), qualified by type, so the two never get confused.

It exists to do three jobs:

1. **Shared understanding.** One place to learn how the system is *supposed* to work,
   in domain terms, without reading the implementation.
2. **A baseline to evaluate against.** Point an agent (or a human) at this contract
   plus the code and ask: *where does the system diverge from the intended behavior?*
   A divergence is either a bug in the code or a gap in this spec — and naming which
   is the point. The planned **`gza-behavior-check`** skill automates exactly this.
3. **A basis to re-implement.** Precise enough that the system could be rebuilt in
   another language from this document alone.

## How this is different from `specs/features/` and `docs/internal/`

| Location | Kind | Authority |
|----------|------|-----------|
| `specs/behavior/` (here) | **Prescriptive** — intent, "should" | The contract. When code and this doc disagree, that disagreement is a finding to resolve, not a doc to silently update. |
| `docs/internal/` | **Descriptive** — design notes, "how it works today," implementation-coupled | Useful background. Mirrors the code; cannot be used to *find* code bugs because it was written *from* the code. |
| `specs/features/` | **Mixed** — feature specs, some implemented, some aspirational | Historical / forward-looking. No status guarantee. |

The critical distinction: `docs/internal/advance-workflow.md` is an excellent
*description* of the engine, but it was reverse-engineered from the code and carries a
"Status: Implemented" banner. It therefore documents the behavior *including any
bug-shaped behavior*. The documents here are written **from intent**, so they can be
held up against the code to find where the code is wrong.

## Writing convention

Every normative statement uses [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119)
keywords:

- **MUST / MUST NOT** — invariant. A violation is a bug, always.
- **SHOULD / SHOULD NOT** — strong default. Deviation requires a stated reason.
- **MAY** — permitted, not required.

Additional rules for authors:

- **Intent first, mechanism second.** Describe *what must be true* and *why*. Refer to
  implementation (function names, file paths, column names) only in clearly marked
  *Implementation note* asides, never in the normative text. The normative text must
  survive a rewrite in another language unchanged.
- **Name policy knobs explicitly.** Where a threshold or a "hold for a human" default
  was chosen conservatively, say so, give it a name, and state that it is a single
  swappable policy point. Conservative defaults are a starting position, not the goal.
- **Minimizing human involvement is the goal.** Every state that requires a human is a
  cost. Each such state MUST name the trigger, how a human clears it, and — where
  known — what automation would let us remove it.
- **Mark genuine open questions.** When the intended behavior is not yet settled, write
  an **Open question** rather than inventing a contract or copying current behavior.
- **Status banner.** Each doc carries a status line distinguishing *agreed contract*
  from *draft / under discussion*.

## Contents

| Doc | Scope | Status |
|-----|-------|--------|
| [00-overview.md](00-overview.md) | The lifecycle state machine: states, transition diagram, and the consolidated human-escalation table. | Draft — invariants + 5 decisions ratified 2026-06-01 |
| [lifecycle-engine.md](lifecycle-engine.md) | The prescriptive transition rules the engine evaluates each pass (plan → implement → review → improve → rebase → merge). | Draft — 5 decisions ratified 2026-06-01 |

### Planned (not yet written)

These follow the same template once the lifecycle engine above is settled:

- CLI interface contract (the observable command surface and its guarantees)
- Concurrency & watch loop (batching, worker accounting, drift/restart)
- Recovery & failure (resume vs retry vs give-up policy)
- Merge units & lineage (the ownership model that defines a "unit of work")
</content>
