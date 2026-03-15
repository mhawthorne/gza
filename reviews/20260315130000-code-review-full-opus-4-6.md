# Gza Code Review - Pre-Release Assessment

Date: 2026-03-15
Reviewer: Claude (Opus 4.6)

## Executive Summary

Gza is a well-structured codebase with strong test coverage (1,297 unit tests, all passing) and clean type safety (mypy reports zero issues across 36 source files). The architecture follows a clean layered design with no circular dependencies. The main areas for improvement are: a few modules with poor/no test coverage (`_query.py`, `console.py`, `review_tasks.py`), broad exception catching in ~20 locations, and code duplication in provider credential handling and database migrations. Overall codebase health is **good** and suitable for release with the high-priority items addressed.

**Source code:** ~19,871 lines across 25 modules (including CLI and providers)
**Test code:** ~30,088 lines across 27 test files
**Test ratio:** 1.5x test code to source code (healthy)

## Test Coverage

### Unit Tests

All **1,297 tests pass** in 26.56 seconds.

| Module | Test File(s) | Coverage Assessment |
|--------|-------------|---------------------|
| db.py (2,620 lines) | test_db.py (134K) | **Good** - CRUD, queries, migrations, chaining, cycles |
| runner.py (1,955 lines) | test_runner.py (135K) | **Adequate** - Helpers tested, but main `run()` orchestration untested |
| git.py (520 lines) | test_git.py, test_git_is_merged.py, test_git_worktree.py | **Good** - Comprehensive with 3 specialized files |
| github.py (100 lines) | test_github.py (456 lines) | **Good** - All methods tested with edge cases |
| config.py (1,750 lines) | test_prompts.py, cli tests | **Adequate** - High-level tested, internal merge helpers untested |
| importer.py (270 lines) | test_importer.py (436 lines) | **Good** - All public functions tested |
| learnings.py (240 lines) | test_learnings.py (276 lines) | **Partial** - Entry points tested, 9 internal helpers untested |
| console.py (135 lines) | test_console.py (1 test) | **Poor** - Only 1 of 10 functions tested |
| branch_naming.py (108 lines) | test_branch_naming.py (239 lines) | **Good** - Both functions comprehensively tested |
| query.py (137 lines) | test_query.py (367 lines) | **Good** - All public functions tested |
| workers.py (209 lines) | test_workers.py (315 lines) | **Good** - All methods tested |
| review_tasks.py (64 lines) | None dedicated | **Poor** - Only tested indirectly through runner |
| review_verdict.py (25 lines) | test_review_verdict.py (42 lines) | **Good** - Single function well tested |
| task_slug.py (23 lines) | test_task_slug.py (20 lines) | **Good** - Both functions tested |
| skills_utils.py (217 lines) | test_claude_install_skills.py (353 lines) | **Adequate** - Main functions tested |
| _query.py (112 lines) | None | **None** - Zero test coverage |
| providers/*.py (2,400 lines) | test_providers.py (3,900+ lines) | **Good** - Comprehensive provider testing |
| CLI modules (8,886 lines) | tests/cli/*.py (extensive) | **Excellent** - All 30+ commands tested |

#### Well-Tested Areas
- CLI commands: All user-facing commands have comprehensive tests
- Database CRUD and schema migrations
- Git operations with multiple specialized test files
- Provider cost calculation, Docker config, error handling
- Task chaining and dependency resolution
- Branch naming strategies (164 test cases)

#### Under-Tested Areas
- **`_query.py`**: 7 public functions with zero test coverage (lineage building, slug extraction)
- **`console.py`**: 9 of 10 utility functions untested (`truncate()`, `format_duration()`, `task_header()`, etc.)
- **`review_tasks.py`**: No dedicated test file; `build_auto_review_prompt()` and `create_review_task()` only tested indirectly
- **`runner.py:run()`**: The main orchestration function (~990 lines in `_run_inner`) lacks direct testing
- **`learnings.py`**: Internal helpers (`_extract_learnings_from_output()`, `_normalize_learning()`, `_dedupe()`, etc.) untested
- **`config.py`**: `_deep_merge_dicts()`, `_read_yaml_dict()`, `_validate_local_override_data()` untested

### Functional Tests

| Workflow | Test Status | Notes |
|----------|-------------|-------|
| Task creation → queuing → status | Tested | Via CLI and DB tests |
| Task execution orchestration | Partial | Helper functions tested, but `run()` itself untested |
| PR creation workflow | Tested | CLI test coverage |
| Review workflow | Tested | CLI and runner tests |
| Improve workflow | Tested | CLI tests |
| Advance (auto-merge/review) | Tested | Comprehensive advance tests |
| Task import | Tested | Dedicated importer tests |
| Docker execution | Integration only | tests_integration/test_docker.py |
| Worktree management | Integration only | tests_integration/test_worktree_env.py |

### Type Safety

**mypy: 0 issues across 36 source files** - Excellent.

Type hint coverage is approximately 83%, with ~84 functions missing return type annotations (mostly CLI handlers and signal handlers). Some `Any` types could be more specific (64 instances).

## Code Duplication

### Issues Found

1. **Docker credential mounting pattern** (High) - `src/gza/providers/base.py`
   - The config directory mounting logic is duplicated between `build_docker_cmd()` (~line 222) and `verify_docker_credentials()` (~line 292)
   - Suggestion: Extract to a shared `_mount_config_dir(cmd, docker_config)` helper

2. **Provider credential checking** (Medium) - `src/gza/providers/claude.py`, `codex.py`, `gemini.py`
   - Each provider reimplements credential detection, OAuth directory checking, and DockerConfig building
   - Suggestion: Move common credential patterns to `Provider` base class

3. **Migration boilerplate in db.py** (Medium) - `src/gza/db.py:600-700`
   - Same pattern repeated 9+ times for each schema version migration (parse SQL, execute, catch OperationalError, update version)
   - Suggestion: Refactor to a loop over migration definitions

4. **Pricing prefix-matching logic** (Low) - Each provider implements identical model pricing lookup
   - Suggestion: Move to a shared utility in `providers/base.py`

### Single Code Path Violations
- No significant violations found. The codebase follows the single code path principle well.

## Error Handling

### Consistency Assessment
- SQL queries use parameterized statements consistently (no injection risk) - well done
- No bare `except:` clauses found in source code
- Custom exceptions are defined but underutilized (only 4: `GitError`, `GitHubError`, `ConfigError`, `DuplicateReviewError`)

### Issues Found

| Location | Issue | Severity | Suggestion |
|----------|-------|----------|------------|
| runner.py:38 | `write_log_entry` catches `Exception` silently | Medium | Catch specific I/O exceptions |
| cli/execution.py:206,360,515 | Multiple broad `except Exception` with print | Medium | Use logger + specific exceptions |
| cli/_common.py:202,247,331,405 | Background worker spawn exceptions caught broadly | Low | Expected - fire-and-forget pattern |
| cli/git_ops.py:703 | Silent exception swallowing | Medium | Add logging |
| db.py:607-609 | `except sqlite3.OperationalError: pass` in migrations | Low | Expected for idempotent migrations |
| db.py:1158 | Silent rollback on any exception | Medium | Log the error before rollback |

### Inconsistent Error Returns
- Database `get()`: returns `None` if not found
- Query module: raises `ValueError` if not found
- API module: raises `KeyError` if not found
- Suggestion: Standardize on one pattern per layer (returns for lookups, raises for required resources)

## API/Interface Consistency

### Naming Conventions
- Database functions consistently use `get_` prefix - good
- Git class methods omit `get_` (e.g., `current_branch()`, not `get_current_branch()`) - acceptable, idiomatic
- Minor inconsistency: providers use both `check_credentials()` and `verify_credentials()` for similar operations

### Signature Consistency
- Parameter ordering is mostly consistent within modules but varies between them
- `_merge_single_task()` in git_ops.py has 6 parameters with inconsistent ordering

### Module Exports
- `__all__` defined in `api/v0.py` and `providers/__init__.py`
- Missing from: `db.py`, `config.py`, `runner.py`, and most core modules
- Impact: Low for an internal tool, but would improve API clarity

## Configuration & Hardcoding

### Magic Values
Configuration defaults are **well-centralized** in `config.py:24-51`:
```python
DEFAULT_TIMEOUT_MINUTES = 10
DEFAULT_MAX_STEPS = 50
DEFAULT_MAX_TURNS = 50
DEFAULT_CLEANUP_DAYS = 30
DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD = 500
# etc.
```
This is well done - no scattered magic numbers.

### Path Handling
- Mostly uses `pathlib.Path` consistently
- Some necessary `str(path)` conversions for subprocess calls
- No unsafe string concatenation for paths

### Hardcoded Paths
- Provider-specific config dirs (`.claude`, `.codex`, `.gemini`) are hardcoded in each provider - acceptable since these are provider-specific conventions
- Worktree dir defaults to `/tmp/{APP_NAME}-worktrees` - configurable via config

## Logging & Observability

### Coverage Assessment
- 879 logging statements across 21 files - good density
- Operations can generally be traced through logs
- Log levels are mostly appropriate

### Issues Found
- CLI modules mix `print()` and `logger` for error messages - print for user-facing output is intentional and acceptable, but internal errors should consistently use logging
- Some exception handlers log warnings where errors would be more appropriate (runner.py:39)
- Database migrations have no per-step logging

### Sensitive Data
- **Well-protected**: `_format_command_for_log()` in `providers/base.py` redacts sensitive values
- API keys and credentials are handled securely
- No sensitive data exposure found in logs

## Resource Management

### File Handling
- Context managers (`with` statements) used consistently - good
- Database connections use proper `_connect()` context manager throughout (50+ instances)
- Temp file cleanup uses `finally` blocks appropriately

### Database Connections
- Isolation level properly configured
- Context managers ensure connection closure
- Rollback on error paths

### Subprocess Management
- `shell=True` used once in `git_ops.py:638` for pager invocation - this is the standard pattern (pager comes from `$PAGER`/`$GIT_PAGER`/git config, same as git itself does). Not a security issue.
- Processes properly waited on
- Stdout/stderr handles closed correctly
- Background processes properly detached with `start_new_session=True`

### Issues Found
| Resource Type | Location | Issue |
|---------------|----------|-------|
| Process pipes | git_ops.py:627-647 | Potential deadlock if stderr buffer fills before pager finishes (stderr.read() after wait) |

## Component Interactions

### Module Dependency Graph
```
config.py (foundation - no internal imports)
    ↑
db.py (imports config, task_slug)
    ↑
git.py (standalone)   github.py (standalone)
    ↑                     ↑
providers/*.py (import config, db)
    ↑
runner.py (imports all above - orchestration layer)
    ↑
cli/*.py (imports all above - user interface layer)
```

### Clear Patterns
- **No circular dependencies** - clean layered architecture
- CLI properly split into 7 modules by concern (execution, git_ops, query, config, log, main, _common)
- Provider abstraction is well-designed with proper base class
- Database is the single canonical data store

### Areas for Improvement
- **db.py (2,620 lines)**: Largest module; handles models, CRUD, migrations, interactive editing, and cycle management. Could benefit from splitting into schema/store/migrations
- **runner.py (1,955 lines)**: Second largest; handles orchestration, worktree setup, provider invocation, review creation, and learnings. Multiple responsibilities
- **cli/_common.py (969 lines)**: Growing large with mixed concerns

## Recommendations

### High Priority
1. **Add tests for `_query.py`** - Zero coverage on 7 public functions used for lineage building
2. **Add tests for `review_tasks.py`** - No dedicated test file for review task creation logic
3. **Extract duplicate Docker credential mounting** - Duplicated in `providers/base.py` between `build_docker_cmd()` and `verify_docker_credentials()`

### Medium Priority
4. **Improve `console.py` test coverage** - Only 1 of 10 functions tested
5. **Standardize error return patterns** - Choose between None-return and exception-raising for "not found" scenarios
6. **Add logging to broad exception handlers** - Replace ~20 silent `except Exception` blocks with specific exceptions and logging
7. **Refactor migration boilerplate in `db.py`** - Replace 9 identical migration blocks with a loop

### Low Priority
8. **Add `__all__` to core modules** - Clarify public API boundaries
9. **Split large modules** - db.py (2,620 lines) and runner.py (1,955 lines) handle multiple responsibilities
10. **Consolidate provider credential logic** - Move common patterns to Provider base class
11. **Add direct tests for `runner.run()`** - Main orchestration function lacks direct unit tests
