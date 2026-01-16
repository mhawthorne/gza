# Exploration Tasks

Use exploration tasks for research, investigation, and codebase analysis.

## Create an exploration task

```bash
$ gza add --explore "Investigate why the test suite is slow. Profile the tests and identify the top 5 slowest tests and what makes them slow."

Created task: 20260108-investigate-why-the-test (explore)
```

The `--explore` flag is shorthand for `--type explore`.

## Run it

```bash
$ gza work
Running task: 20260108-investigate-why-the-test
...
Task completed in 5m 18s (15 turns, $0.12)
```

## View the exploration results

Exploration outputs are saved to `.gza/explorations/`:

```bash
$ cat .gza/explorations/20260108-investigate-why-the-test.md

# Exploration: Test Suite Performance

## Summary
Identified 5 slowest tests consuming 73% of total test time...

## Findings

### 1. test_full_sync_integration (42s)
   - Makes real HTTP calls to external service
   - Recommendation: Mock external calls

### 2. test_database_migration (28s)
   - Recreates full schema on each run
   - Recommendation: Use transaction rollback
...
```

## Key differences from regular tasks

| Aspect | Regular Task | Exploration Task |
|--------|--------------|------------------|
| Creates branch | Yes | No |
| Modifies code | Yes | No |
| Output location | Git branch | `.gza/explorations/` |
| Purpose | Implementation | Research & analysis |

## Use cases for exploration

- Investigate bugs before fixing
- Analyze performance bottlenecks
- Understand unfamiliar code
- Evaluate architectural options
- Audit dependencies or security
- Document existing behavior
