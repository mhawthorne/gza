# Branch Naming Strategies

## Overview

Allow users to configure branch naming conventions to support different repository structures and team workflows.

## Current Implementation

Branches are currently named as `{project_name}/{task_id}`:
- Example: `gza/20260107-add-user-auth`
- This pattern was designed for monorepos where the project name distinguishes branches

## Problem

For standalone repos (like gza's own GitHub repo), the `project_name/` prefix is redundant. Common conventions use prefixes like:
- `feature/add-user-auth`
- `fix/null-pointer-exception`
- `chore/update-dependencies`

## Proposed Solution

### Configuration

Add `branch_strategy` to `gza.yaml`:

```yaml
project_name: gza
branch_strategy:
  pattern: "{type}/{slug}"      # Template for branch names
  default_type: feature         # Used when type cannot be inferred
```

#### Pattern Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `{project}` | The `project_name` value | `gza` |
| `{task_id}` | Full task ID | `20260107-add-auth` |
| `{date}` | Date portion of task_id | `20260107` |
| `{slug}` | Slug portion of task_id | `add-auth` |
| `{type}` | Inferred or default type | `feature` |

#### Preset Strategies

For convenience, support named presets:

```yaml
branch_strategy: monorepo    # {project}/{task_id} - current behavior, default
branch_strategy: conventional  # {type}/{slug}
branch_strategy: simple       # {slug}
```

Or define a custom pattern:

```yaml
branch_strategy:
  pattern: "{project}/{type}/{slug}"
  default_type: feat
```

### Type Inference

The `{type}` variable is determined by:

1. **Explicit `--type` flag** (highest priority):
   ```bash
   gza add --type fix "Resolve null pointer in auth"
   ```

2. **Keyword detection in prompt** (if no explicit type):

   | Keywords | Type |
   |----------|------|
   | fix, bug, error, crash, broken, issue | `fix` |
   | feat, feature, add, implement, create, new | `feature` |
   | refactor, restructure, reorganize, clean | `refactor` |
   | doc, docs, document, readme | `docs` |
   | test, spec, coverage | `test` |
   | chore, update, upgrade, bump, deps | `chore` |
   | perf, performance, optimize, speed | `perf` |

3. **Default type** from config (fallback):
   ```yaml
   branch_strategy:
     default_type: feature  # Used when no keywords match
   ```

### CLI Changes

#### New `--type` flag for `add` command

```bash
gza add --type fix "Resolve authentication timeout"
# Creates branch: fix/resolve-authentication-timeout

gza add --type feature "Add dark mode support"
# Creates branch: feature/add-dark-mode-support

gza add "Improve query performance"
# Inferred from "improve" -> feature/improve-query-performance
```

#### Init command update

```bash
gza init
# Prompts: Branch strategy? [monorepo/conventional/simple/custom]
```

### Database Changes

Add `task_type_hint` column to tasks table (nullable):
- Stores explicit type when provided via `--type`
- Used for branch naming and potentially PR labels

### Migration Path

- Default `branch_strategy` to `monorepo` for backward compatibility
- Existing branches/tasks unaffected
- New tasks use configured strategy

## Examples

### Monorepo (current default)
```yaml
branch_strategy: monorepo
```
- `gza/20260107-add-user-auth`
- `gza/20260107-fix-login-bug`

### Conventional (GitHub-style)
```yaml
branch_strategy: conventional
```
- `feature/add-user-auth`
- `fix/login-bug`
- `docs/update-readme`

### Simple (minimal)
```yaml
branch_strategy: simple
```
- `add-user-auth`
- `fix-login-bug`

### Custom patterns
```yaml
branch_strategy:
  pattern: "{type}/{date}-{slug}"
  default_type: feat
```
- `feat/20260107-add-user-auth`
- `fix/20260107-login-bug`

```yaml
branch_strategy:
  pattern: "user/{project}-{slug}"
  default_type: feature
```
- `user/gza-add-user-auth`

## Implementation Tasks

1. Add `branch_strategy` to Config dataclass with validation
2. Implement pattern parsing and variable substitution
3. Add keyword-based type inference function
4. Add `--type` flag to `add` command
5. Add `task_type_hint` column to database schema
6. Update `generate_branch_name()` in runner.py
7. Update `gza init` to prompt for branch strategy
8. Add tests for each preset and custom patterns

## Design Decisions

### `branch_mode: single` is unaffected

`branch_strategy` only applies to `branch_mode: multi` (the default).

Single mode continues to use `{project}/gza-work` regardless of the configured strategy. Rationale: single mode is a "scratchpad" - the branch is deleted and recreated from main on each task, so the name is irrelevant.

### Light branch name validation

Git has specific rules for valid ref names (see [git-check-ref-format](https://git-scm.com/docs/git-check-ref-format)):

**Not allowed:**
- Two consecutive dots `..`
- ASCII control characters, space, `~`, `^`, `:`, `?`, `*`, `[`, `\`
- Start/end with `/`, or consecutive slashes
- End with `.lock`
- Start with a dot
- The single character `@`

**Practical limits:**
- ~255 characters per path component (filesystem limit)
- GitHub rejects 40-char hex strings (looks like SHA) and names starting with `refs/`

Since `slugify()` already sanitizes to `[a-z0-9-]`, the slug portion is safe. The risk is in custom patterns with invalid characters.

**Approach:** Validate the final generated branch name for obvious problems (spaces, `..`, consecutive slashes). Git will reject truly invalid names anyway - we just provide a friendlier error at config validation time if the pattern contains invalid characters.

## Alternatives Considered

### Alternative: Separate prefix and suffix config
```yaml
branch_prefix: "feature"
branch_suffix: "{slug}"
```
Rejected: Less flexible than pattern-based approach.

### Alternative: No type inference
Require explicit `--type` always for conventional branches.
Rejected: Adds friction for common cases where type is obvious from prompt.
