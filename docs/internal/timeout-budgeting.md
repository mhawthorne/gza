# Timeout Budgeting

Gza now treats large code tasks differently from small ones without making
timeouts elastic.

## Why deterministic scaling first

The immediate failure mode was wasted wall clock during repeated heavy verify
runs, not the lack of an activity-sensitive supervisor. Deterministic
timeout scaling solves that narrower problem with less control-plane risk:

- resolve a task-specific base timeout from provider/task-type config;
- scale code-task budgets by reviewable diff size;
- keep a hard cap, even when the resolved base timeout override is higher;
- log the resolved budget and reason up front.

This gives legitimate large tasks more runway without introducing an
open-ended progress detector that can extend loops indefinitely.

## Why not elastic extension yet

Elastic timeouts require stronger notions of durable progress than "the
process is still printing output." The outer provider process is currently
bounded by the system `timeout` wrapper, so safe extension would require new
supervision mechanics plus a milestone model that distinguishes real
progress from repeated failing commands.

That remains a possible follow-up once provider progress is structured
enough to extend only on durable events such as:

- a new provider step;
- a file edit;
- a new commit;
- a recorded verification phase result.

Until then, deterministic scaling keeps the behavior understandable,
bounded, and operationally visible.

## Timeout resume checkpoints

Timeout resume guidance may summarize successful verify phases, but those
checkpoints are reusable only for the exact saved tree state that resume
will restore.

- Phase fingerprints are recorded against the tree state that produced each
  successful verify phase.
- When timeout handling saves WIP as a commit, resume checkpoint persistence
  must translate matching pre-save phase fingerprints onto that saved WIP
  commit tree before advertising reuse.
- Any later edit after the WIP save invalidates those checkpoints again.
- Legacy phase logs without explicit fingerprints may still contribute
  timeout context, but they must not advertise reusable verify phases.
- Verification wrappers should omit `tree_fingerprint=` entirely when exact
  fingerprinting is unavailable, rather than emitting placeholder values.
