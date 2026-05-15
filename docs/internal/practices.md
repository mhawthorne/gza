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
