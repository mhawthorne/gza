---
name: gza-code-review-full
description: Comprehensive pre-release code review assessing test coverage, code duplication, and component interactions
allowed-tools: Read, Glob, Grep, Bash(uv run pytest:*), Bash(uv run python:*), Bash(uv run mypy:*), Bash(ls:*), Bash(wc:*)
version: 1.0.0
public: false
---

# Full Codebase Code Review

Perform a comprehensive code review of the gza codebase, suitable for pre-release assessment. This review covers:
1. Unit test coverage
2. Functional test coverage
3. Code duplication
4. Component interaction patterns
5. Error handling consistency
6. API/interface consistency
7. Configuration and hardcoding audit
8. Logging and observability
9. Resource management
10. Type safety

## When to Use

- Before a release to assess codebase health
- When you want a comprehensive quality check
- To identify areas needing more tests or refactoring

## Output

Write findings to `reviews/<timestamp>-code-review-full.md` in the project root, where `<timestamp>` is the current date/time in `YYYYmmddHHMMSS` format (e.g., `reviews/20260212143022-code-review-full.md`).

## Process

### Step 1: Inventory the codebase

Map out the source modules and test files:

1. **List all source modules:**
   ```bash
   ls -la src/gza/*.py
   ls -la src/gza/providers/*.py
   ```

2. **List all test files:**
   ```bash
   ls -la tests/*.py
   ls -la tests_integration/*.py 2>/dev/null || echo "No integration tests dir"
   ```

3. **Create a mapping** of source file → test file(s):
   - `db.py` → `test_db.py`
   - `cli.py` → `test_cli.py`
   - etc.

4. **Identify untested modules** - source files with no corresponding test file

### Step 2: Assess unit test coverage

For each source module:

1. **Read the source file** to understand its public interface (functions, classes, methods)

2. **Read the corresponding test file** (if exists)

3. **Check coverage by listing:**
   - Functions/methods that ARE tested
   - Functions/methods that are NOT tested
   - Edge cases that aren't covered (error paths, boundary conditions)

4. **Run the tests** to verify they pass:
   ```bash
   uv run pytest tests/ -v --tb=short
   ```

Focus especially on:
- **`db.py`** - Core task storage, critical for correctness
- **`cli.py`** - User-facing commands, all subcommands should have tests
- **`runner.py`** - Task execution logic
- **`git.py`** - Git operations (mocked tests preferred)
- **`github.py`** - GitHub integration

### Step 3: Assess functional test coverage

Functional tests verify end-to-end workflows. Check for:

1. **Core workflows that should have integration tests:**
   - Creating a task → running it → verifying completion
   - Task dependencies (task B waits for task A)
   - PR creation workflow
   - Review workflow
   - Improve workflow

2. **Read `tests_integration/`** (if exists) to see what's covered

3. **Identify missing functional tests** - workflows documented in AGENTS.md that aren't tested

### Step 4: Analyze code duplication

Look for patterns of duplicated code:

1. **Search for similar code blocks:**
   - Similar function signatures doing similar things
   - Copy-pasted error handling
   - Repeated patterns that could be extracted

2. **Check specific areas prone to duplication:**
   - CLI command handlers (do they share common patterns that could be unified?)
   - Database queries (repeated query patterns)
   - Git operations (similar git command sequences)

3. **Use grep to find suspicious patterns:**
   ```bash
   # Find similar function definitions
   grep -n "def.*task" src/gza/*.py

   # Find repeated patterns
   grep -n "subprocess.run" src/gza/*.py
   grep -n "click.echo" src/gza/cli.py
   ```

4. **Read AGENTS.md** section on "Single code path principle" and verify it's followed

### Step 5: Check error handling consistency

Review how errors are handled across the codebase:

1. **Identify error handling patterns:**
   ```bash
   # Find exception raising
   grep -n "raise " src/gza/*.py

   # Find try/except blocks
   grep -n "except " src/gza/*.py

   # Find custom exceptions
   grep -rn "class.*Exception" src/gza/
   grep -rn "class.*Error" src/gza/
   ```

2. **Check for consistency:**
   - Are errors handled uniformly? (always raise vs sometimes return None)
   - Are custom exceptions used where appropriate vs generic `Exception`?
   - Do error messages provide actionable information?
   - Are exceptions caught too broadly? (`except Exception` vs specific types)

3. **Look for problematic patterns:**
   - Silent failures (bare `except:` or `except: pass`)
   - Swallowed exceptions without logging
   - Inconsistent error return values (None vs empty list vs raise)
   - Missing error handling on I/O operations

4. **Document findings:**
   - List any inconsistencies in error handling approach
   - Note functions that should raise but return None (or vice versa)
   - Identify error messages that aren't helpful for debugging

### Step 6: Check API/interface consistency

Review function signatures and naming conventions:

1. **Check naming consistency:**
   ```bash
   # Find all public function definitions
   grep -n "^def " src/gza/*.py
   grep -n "    def " src/gza/*.py | grep -v "__"
   ```

2. **Look for inconsistencies:**
   - Similar operations with different names (`get_task` vs `fetch_task` vs `retrieve_task`)
   - Parameter ordering inconsistencies (does `db` come first or last?)
   - Return type inconsistencies (objects vs dicts vs tuples)

3. **Check function signatures:**
   - Do similar functions have similar signatures?
   - Are there functions with too many parameters (>5)?
   - Are boolean parameters used where enums would be clearer?

4. **Review public interfaces:**
   - Are module `__all__` exports defined?
   - Is it clear what's public vs private? (underscore prefix convention)
   - Are there functions that should be private but aren't?

### Step 7: Audit configuration and hardcoding

Look for magic values that should be configurable:

1. **Find hardcoded values:**
   ```bash
   # Find numeric literals (potential magic numbers)
   grep -En "[^a-zA-Z_][0-9]{2,}[^0-9]" src/gza/*.py

   # Find string literals that might be paths or config
   grep -n '"/.*"' src/gza/*.py
   grep -n "'/.*'" src/gza/*.py
   ```

2. **Check for:**
   - Magic numbers (timeouts, retry counts, limits)
   - Hardcoded file paths
   - Hardcoded URLs or endpoints
   - Default values that should be configurable

3. **Review path handling:**
   - Are paths constructed safely using `pathlib`?
   - Are there string concatenations for paths? (`dir + "/" + file`)
   - Are relative vs absolute paths handled correctly?

4. **Check configuration loading:**
   - Is `config.py` the single source for configuration?
   - Are there config values scattered in other modules?
   - Are defaults documented?

### Step 8: Review logging and observability

Assess the ability to debug and monitor the system:

1. **Check logging usage:**
   ```bash
   # Find logging calls
   grep -n "logging\." src/gza/*.py
   grep -n "logger\." src/gza/*.py
   grep -n "log\." src/gza/*.py

   # Find print statements (should these be logs?)
   grep -n "print(" src/gza/*.py
   ```

2. **Assess logging quality:**
   - Is there consistent logging for key operations?
   - Can you trace a task's execution through the logs?
   - Are log levels used appropriately? (debug vs info vs warning vs error)
   - Are there operations that fail silently without logging?

3. **Check for sensitive data exposure:**
   ```bash
   # Look for potential credential logging
   grep -in "api.key\|token\|password\|secret\|credential" src/gza/*.py
   ```
   - Are API keys, tokens, or passwords properly excluded from logs?
   - Are there any `repr()` or `str()` methods that might expose secrets?

4. **Review error logging:**
   - Are exceptions logged with stack traces where needed?
   - Are error messages actionable?
   - Is there enough context to debug issues?

### Step 9: Check resource management

Look for resource leaks and cleanup issues:

1. **Check file handling:**
   ```bash
   # Find file operations
   grep -n "open(" src/gza/*.py
   grep -n "with open" src/gza/*.py
   ```
   - Are all file opens using context managers (`with`)?
   - Are there any `open()` calls without corresponding `close()`?

2. **Check database connections:**
   ```bash
   grep -n "connect(" src/gza/*.py
   grep -n "cursor" src/gza/*.py
   ```
   - Are database connections properly closed?
   - Are cursors managed with context managers?
   - Is there connection pooling or is it connect-per-operation?

3. **Check subprocess management:**
   ```bash
   grep -n "subprocess" src/gza/*.py
   grep -n "Popen" src/gza/*.py
   ```
   - Are subprocesses properly waited on?
   - Are there potential zombie processes?
   - Are stdin/stdout/stderr handles closed?

4. **Check for memory issues:**
   - Are there unbounded caches or growing lists?
   - Are large objects cleaned up after use?
   - Are there circular references that prevent garbage collection?

5. **Check temp file cleanup:**
   ```bash
   grep -n "tempfile\|mktemp\|NamedTemporaryFile" src/gza/*.py
   ```
   - Are temp files cleaned up after use?
   - Are temp directories removed?

### Step 10: Assess type safety

Review type hints and type correctness:

1. **Check type hint coverage:**
   ```bash
   # Find functions without return type hints
   grep -n "def.*):$" src/gza/*.py

   # Find functions with type hints
   grep -n "def.*) ->" src/gza/*.py
   ```

2. **Run mypy (if configured):**
   ```bash
   uv run mypy src/gza/ --ignore-missing-imports 2>&1 | head -100
   ```

3. **Look for type safety issues:**
   - Functions with `Any` types that could be more specific
   - `Optional` types without proper `None` checks
   - Type: ignore comments (are they justified?)
   - Inconsistent types (function returns `str | None` but callers don't check)

4. **Check for common type issues:**
   ```bash
   # Find potential None issues
   grep -n "\.get(" src/gza/*.py  # dict.get returns Optional
   grep -n "or None" src/gza/*.py
   grep -n "if.*is None" src/gza/*.py
   ```

### Step 11: Analyze component interaction patterns

Understand how modules interact and assess the clarity of these interactions:

1. **Map the import graph:**
   ```bash
   grep -h "^from gza" src/gza/*.py | sort | uniq -c | sort -rn
   grep -h "^import gza" src/gza/*.py | sort | uniq -c | sort -rn
   ```

2. **Identify the layering:**
   - Which modules are "lower level" (few dependencies)?
   - Which are "higher level" (many dependencies)?
   - Are there circular dependencies?

3. **Check separation of concerns:**
   - Does `cli.py` only handle CLI concerns, delegating to other modules?
   - Does `db.py` only handle database concerns?
   - Does `runner.py` only handle execution concerns?

4. **Look for unclear interfaces:**
   - Functions with too many parameters
   - Functions that do too many things
   - Tight coupling between modules that should be loosely coupled

5. **Document the interaction patterns:**
   ```
   cli.py → db.py (task CRUD)
   cli.py → runner.py (task execution)
   runner.py → providers/* (AI execution)
   runner.py → git.py (git operations)
   etc.
   ```

### Step 12: Compile the review report

Create a structured report at `reviews/code-review-full.md`:

```markdown
# Gza Code Review - Pre-Release Assessment

Date: YYYY-MM-DD
Reviewer: Claude

## Executive Summary

[2-3 sentence overview of codebase health]

## Test Coverage

### Unit Tests

| Module | Test File | Coverage Assessment |
|--------|-----------|---------------------|
| db.py | test_db.py | Good - covers CRUD, queries |
| cli.py | test_cli.py | Partial - missing `gza pr` tests |
| ... | ... | ... |

#### Well-Tested Areas
- [List modules/features with good coverage]

#### Under-Tested Areas
- [List modules/features needing more tests]
- [Specific functions that lack tests]

### Functional Tests

| Workflow | Test Status | Notes |
|----------|-------------|-------|
| Task creation → execution | ✓ Tested | integration test exists |
| PR creation | ✗ Not tested | needs integration test |
| ... | ... | ... |

## Code Duplication

### Issues Found

1. **[Description]** - [location]
   - Suggestion: [how to fix]

2. **[Description]** - [location]
   - Suggestion: [how to fix]

### Single Code Path Violations
- [Any violations of the principle from AGENTS.md]

## Error Handling

### Consistency Assessment
- [Are errors handled uniformly?]
- [Custom exceptions defined and used appropriately?]

### Issues Found
| Location | Issue | Suggestion |
|----------|-------|------------|
| file.py:123 | Bare except clause | Catch specific exception |

## API/Interface Consistency

### Naming Conventions
- [Assessment of naming consistency]

### Signature Consistency
- [Assessment of parameter ordering, return types]

### Issues Found
- [List any inconsistencies]

## Configuration & Hardcoding

### Magic Values Found
| Value | Location | Suggestion |
|-------|----------|------------|
| 30 | runner.py:45 | Move to config as DEFAULT_TIMEOUT |

### Path Handling
- [Assessment of pathlib usage vs string concatenation]

## Logging & Observability

### Coverage Assessment
- [Can operations be traced through logs?]
- [Are log levels appropriate?]

### Sensitive Data
- [Any exposure risks found?]

### Issues Found
- [Silent failures, missing logging, etc.]

## Resource Management

### File Handling
- [Context manager usage assessment]

### Database Connections
- [Connection lifecycle assessment]

### Subprocess Management
- [Cleanup assessment]

### Issues Found
| Resource Type | Location | Issue |
|---------------|----------|-------|
| file | importer.py:89 | open() without context manager |

## Type Safety

### Type Hint Coverage
- [Percentage/assessment of coverage]

### Mypy Results
- [Summary of mypy findings]

### Issues Found
- [Any types, missing None checks, etc.]

## Component Interactions

### Module Dependency Graph
```
[ASCII diagram or description]
```

### Clear Patterns
- [What's done well]

### Areas for Improvement
- [Unclear interfaces, tight coupling, etc.]

## Recommendations

### High Priority
1. [Most important fix]
2. [Second most important]

### Medium Priority
1. [...]

### Low Priority
1. [...]

## Appendix: Detailed Findings

[Any detailed notes, specific code snippets, etc.]
```

## Tips

- **Be thorough but practical** - Focus on issues that matter for a release
- **Prioritize findings** - Not all issues are equally important
- **Be specific** - Reference exact files, functions, line numbers
- **Suggest solutions** - Don't just identify problems, propose fixes
- **Note what's done well** - Acknowledge good patterns and coverage
- **Consider the project stage** - This is pre-release, so focus on stability

## Coverage Assessment Criteria

Use these criteria when assessing test coverage:

- **Good**: All public functions tested, error paths covered, edge cases handled
- **Adequate**: Main happy paths tested, some error handling
- **Partial**: Only basic tests, missing significant functionality
- **Poor**: Few or no tests, critical paths untested
- **None**: No test file exists

## Code Duplication Severity

- **High**: Same logic repeated 3+ times, maintenance burden
- **Medium**: 2 instances of duplication, some risk
- **Low**: Minor duplication, acceptable for clarity
