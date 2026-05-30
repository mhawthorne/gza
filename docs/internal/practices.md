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

The unit-suite watchdog is configured through `GZA_UNIT_TEST_TIMEOUT_MS`. In
the cleanup stage, keep the effective default at 1000ms while driving the
slow tail down. The rollout target is 500ms, and any unit test approaching
that limit is a latency bug. Tests at or above 400ms require deliberate
classification: optimize in place, move subprocess or real-shell coverage to
`tests_functional/`, or add a narrow `@pytest.mark.timeout(1.0,
method="signal")` override with a comment explaining why the test must remain
in `tests/`.

Fixture setup and teardown time count against the unit watchdog. Do not assume
fixture cost is free, and do not switch to `func_only=True` just to hide slow
setup or teardown.

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

## Skip `uv run` inside the test suite

When a test must spawn the CLI as a subprocess, use `sys.executable -m gza`
rather than `uv run gza`. Pytest already runs inside the project venv (the
suite is invoked via `uv run pytest`), so `sys.executable` points at the
same interpreter `uv run` would launch — but without the per-invocation
lockfile revalidation that `uv run` performs. Skipping that step removes a
real chunk of the per-test budget and a common source of flakes.

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
