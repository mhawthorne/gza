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

Timed-out runs also persist best-effort token and cost stats from streamed
provider transcript usage when the outer timeout wrapper kills the process
before a final provider summary is emitted.

Autonomous review verification has a second timeout layer aimed at diagnostics,
not longer budgets:

- the runner enforces `autonomous_verify_timeout_seconds`;
- on timeout it sends SIGTERM to the verify process group;
- it waits `review_verify_timeout_grace_seconds` for the harness to flush
  diagnostics such as slow-test summaries or faulthandler dumps;
- if the process tree is still alive, it escalates to SIGKILL and persists the
  captured stdout/stderr as review-verify evidence.

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

## Unit and functional parallel-only rerun bridge

The unit and functional verify lanes keep their wall-clock behavior unchanged
on the all-green path, but they now have a bounded bridge for contention-style
parallel failures:

- the parallel xdist pass runs first with `--maxfail=GZA_UNIT_RERUN_CAP+1`, so
  the harness can see the whole bounded failure set instead of stopping at the
  first failure;
- if that pass fails only because a bounded set of per-test node IDs failed,
  the harness reruns just those node IDs serially with the normal per-test
  timeout and logs each `PARALLEL-ONLY FAILURE (passed serially)` line;
- collection errors, internal errors, unattributable exits, or failure sets
  over the cap fail immediately with `unit-rerun: NOT masking - ...` or
  `functional-rerun: NOT masking - ...`.

This bridge is intentionally narrow. It is an interim run-level arbiter for
known xdist contention artifacts, not a replacement for the durable per-test
budgeting work. `GZA_UNIT_SERIAL_RERUN=0` or
`GZA_FUNCTIONAL_SERIAL_RERUN=0` disables the bridge for operator triage, and
`GZA_UNIT_RERUN_CAP` plus `GZA_FUNCTIONAL_RERUN_CAP` keep broad failure sets
fail-closed.

## Unit-suite per-test guards

The unit suite now separates two concerns that used to be conflated by one
short wall-clock timeout:

- `GZA_UNIT_TEST_CPU_BUDGET_MS` is the strict latency bar. `tests/conftest.py`
  measures `time.process_time()` across the test call phase and fails after the
  fact if the test used too much in-process CPU. This is contention-proof under
  xdist because descheduled wall time does not accrue CPU time.
- `GZA_UNIT_TEST_HANG_TIMEOUT_MS` is the generous hang guard. The collection
  hook still injects `pytest.mark.timeout(..., method="signal")`, but with
  enough headroom that contention should never trip it. Its job is interruption,
  not latency policing.

Per-test overrides stay narrow:

- `@pytest.mark.cpu_budget(ms=...)` raises only that test's CPU budget.
- An explicit `@pytest.mark.timeout(...)` opts the test out of the CPU budget
  entirely; the author owns that test's latency classification.

The functional suite intentionally does not use a CPU budget. Functional tests
legitimately spend time in child processes, and `time.process_time()` would miss
that work. `GZA_FUNCTIONAL_TEST_TIMEOUT_SECONDS` is therefore only a generous
wall-clock hang-guard knob in `tests_functional/conftest.py`, layered under the
outer 120s verify cap rather than acting as a latency budget.
