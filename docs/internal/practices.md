# Engineering Practices

Rules of practice that govern how work happens here. Not system design — that
belongs in `principles.md`. When a decision argues against one of these, the
burden is on the decision to name the practice and justify the exception.

## Load-bearing guardrails stay

Per-test timeouts, fail-fast flags, verify gates, type checks, linters — when
one fires, fix the underlying problem, re-classify the unit to a looser bound
deliberately, or add a narrow per-unit override with a comment. Do not relax
the suite-wide default. The default catches drift at the smallest unit, at
the moment a regression is introduced. Replacing it with a downstream bound
trades early-localized detection for late-aggregate detection.

For the unit suite, keep median latency below 25ms, p95 below 150ms, and p99
below 250ms. Those are warning thresholds, not budgets to spend.

The unit-suite watchdog is split into two knobs with different jobs:
`GZA_UNIT_TEST_HANG_TIMEOUT_MS` keeps a generous wall-clock
`pytest-timeout` SIGALRM guard so genuinely hung tests still get interrupted,
while `GZA_UNIT_TEST_CPU_BUDGET_MS` is the real post-hoc "this unit test
did too much work" budget. Keep the default CPU budget at 1000ms while driving
the slow tail down. The rollout target remains 500ms of CPU time, and any unit
test approaching that limit is a latency bug. Tests at or above 400ms require
deliberate classification: optimize in place, move subprocess or real-shell
coverage to `tests_functional/`, or add a narrow
`@pytest.mark.cpu_budget(ms=...)` override with a comment explaining why the
test must remain in `tests/`. Reserve explicit `@pytest.mark.timeout(...)`
for the rare case where the test intentionally owns its wall-clock watchdog
and CPU-latency classification.

The unit CPU guard measures the test call phase, not fixture setup/teardown.
Do not use that narrower scope as cover for expensive fixtures; heavy setup is
still a unit-suite latency smell and belongs either in shared cheap fixtures or
in a looser suite with explicit classification.

## Don't concrete over removals

When undoing a protection, leave the door open to re-add it. Assertions that
"X cannot exist" turn reversible policy into a one-way door. Before adding
any such assertion, ask: is the absence of X a load-bearing invariant, or
just the current preference? If the latter, a comment or convention is
enough; an automated check is too strong.

## Prefer errors over silent fallbacks

When a boundary input is ambiguous, refuse and ask the caller to disambiguate
— don't pick a "reasonable" default. Silent fallbacks accumulate as hidden
behavior: small and defensible in isolation, but together they hide the gap
between what the system claims to do and what it actually does.

## Match the scope of the change to the scope of the problem

A bug fix is not an invitation to refactor neighbors. Test brittleness is not
an invitation to revisit the policy the test happens to check. When a narrow
patch grows to touch unrelated machinery, treat the growth as a signal —
either the diagnosis was wrong, or unrelated concerns are being smuggled in.

A specific failure mode: pairing an infrastructure refactor with a policy
change in one PR. ("Refactor tests that assert X, and while we're here,
change X.") Different decisions; separate them.

## A subprocess belongs in `tests_functional/`

If a test spawns a subprocess, it's a functional test — not a unit test —
and belongs under `tests_functional/`. The unit suite has tight latency
requirements, enforced by a short per-test watchdog, so the inner dev loop
stays fast and regressions surface quickly. Subprocess startup routinely
blows that budget; the dedicated functional suite gives those tests the
headroom they actually need without stretching the unit watchdog.

The unit suite also installs a default-on runtime subprocess guard in
`tests/conftest.py`. If a unit test still needs a temporary escape hatch
while being cleaned up, use a narrow exemption that cites a follow-up task;
use `GZA_ENABLE_UNIT_SUBPROCESS_GUARD=0` only for emergency local triage.

Turning this guard on red-lines the existing unit tests that drive real
`git` in temp repos. The guard-installing change does **not** rewrite those
offenders inline — that would balloon an infrastructure change into a mass
edit of unrelated test modules. Instead, each offending module the guard
surfaces gets a narrow, module-scoped exemption that cites a dedicated
follow-up implement task (one `gza-<n>` per cluster of related modules). The
guard ships with that small exemption table, and the follow-up tasks then
convert each module's subprocess/git tests to in-process mocks (or relocate
them to `tests_functional/` with `@pytest.mark.functional`) and remove the
matching exemption. The rollout rule: install the guard and file separate
follow-ups for its offenders — do not convert offenders in the same commit.

## Skip `uv run` inside the test suite

When a test must spawn the CLI as a subprocess, use `sys.executable -m gza`
rather than `uv run gza`. Pytest already runs inside the project venv (the
suite is invoked via `uv run pytest`), so `sys.executable` points at the
same interpreter `uv run` would launch — but without the per-invocation
lockfile revalidation that `uv run` performs. Skipping that step removes a
real chunk of the per-test budget and a common source of flakes.

The same principle applies to repo verification scripts: once the project
environment already exists, prefer invoking `.venv/bin` tools directly over
paying repeated `uv run` startup costs inside every phase.

## Fast inner loop, full final gate

Code-task verification has two distinct jobs:

- The inner loop should be fast and high-signal so agents can keep making
  progress without burning the whole wall-clock budget on repeated heavy
  suite launches.
- The final gate should stay strict. A code task is not complete until the
  configured full `verify_command` passes after the last code change.

In practice:

- Use `inner_verify_command` or targeted tests while editing.
- Run the full `verify_command` once after the last planned edit.
- If the full gate fails, fix the failures and rerun it. Do not keep
  relaunching the full suite after every intermediate edit.

## Verify commands must flush diagnostics on timeout

Autonomous review verification treats timeout diagnostics as part of the
contract, not optional nice-to-have logging. The lifecycle runner sends
SIGTERM to the verify process group before escalating to SIGKILL, and it
persists whatever stdout/stderr the harness flushes during that grace period
as review evidence.

A good `verify_command` therefore does three things:

- emits a slow-test summary during normal operation for the expensive test
  phases;
- registers a SIGTERM-triggered stack dump for in-flight Python tests, for
  example `faulthandler.register(signal.SIGTERM, chain=True)`;
- flushes any best-effort summary or other diagnostics before allowing the
  SIGTERM to terminate the process.

For gza's own harness, that recipe means `./bin/tests` keeps `--durations=25`
on the unit and functional pytest phases, the unit and functional pytest
suite conftests call the shared `register_sigterm_faulthandler()` helper at
import time, and `python -m gza.test_latency --summary` emits its current
summary before re-raising termination.
The wrapper also defaults its xdist worker count to the same fixed `-n 2`
that CI uses under `--dist loadscope`, so local `./bin/tests` reproduces the
same worker grouping on high-core developer machines unless an operator
explicitly overrides `PYTEST_XDIST_WORKERS`.

The unit and functional lanes both use guarded serial-rerun bridges as interim
arbiters for bounded parallel-only flakes. The bridge keeps the normal parallel
xdist pass, then reruns only the captured failing node IDs serially when, and
only when, the entire failure set is a bounded set of per-test failures.
Collection errors, internal pytest errors, unattributable non-zero exits, or
failure sets above the configured cap fail the phase without masking.
Operators can disable the bridge with `GZA_UNIT_SERIAL_RERUN=0` or
`GZA_FUNCTIONAL_SERIAL_RERUN=0` when debugging suspected parallel-only real
failures. Treat the `unit-rerun: PARALLEL-ONLY FAILURE (passed serially)` and
`functional-rerun: PARALLEL-ONLY FAILURE (passed serially)` lines as real
signals to follow up, not as noise to suppress.

## Gitignored derived artifacts are not review blockers

`.claude/skills/` is installed per-worktree by `gza skills-install` from the
tracked `src/gza/skills/` source. It is gitignored — no commit can change it,
so no `improve` task can close a blocker scoped to it. Reviewers must not
flag drift between an installed copy and its bundled source as
`CHANGES_REQUESTED`; the installer enforces alignment at runtime, not
review. The same applies to any other gitignored derived artifact. If a
property of the installed copy genuinely matters, test it by invoking the
installer into `tmp_path` (see `tests/test_skills_install.py`) — not by
asserting against the on-disk install state of the current worktree.
