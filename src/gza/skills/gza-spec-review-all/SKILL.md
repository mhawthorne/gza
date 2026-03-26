---
name: gza-spec-review-all
description: Review all spec files for accuracy against the current implementation, flagging outdated vs aspirational content
allowed-tools: Read, Glob, Grep, Write, Bash(ls:*), Bash(git log:*), Bash(git blame:*), Bash(uv run *--help*), Bash(date +%Y%m%d%H%M%S)
version: 1.0.0
public: true
---

# Spec Review Skill

Review specification documents in `specs/` for accuracy against the current implementation.

## When to Use

- User asks to review specs
- User asks "are the specs accurate?" or "which specs are outdated?"
- Before a release to ensure specs reflect current state
- After a major refactor to find specs that reference old code

## Important: Aspirational vs Outdated

Specs can be **forward-looking** (describing planned features) or **outdated** (describing old behavior). Use this heuristic:

- **Aspirational (skip)**: Describes functionality that doesn't exist in code but sounds intentional/planned. Leave these alone.
- **Outdated (flag)**: Describes functionality that *used to* work differently, or references old file paths, old command names, or deprecated patterns.

When in doubt, flag it with a note that it "may be aspirational."

## Process

### Step 1: Discover specs

```bash
ls specs/
```

### Step 2: For each spec, verify against implementation

Read the spec file, then search the codebase to verify its claims.

**Check for concrete, verifiable claims:**

1. **File paths** — Does the spec reference files that exist?
   - Search with Glob for referenced paths
   - Flag paths that don't exist and aren't plausible future additions

2. **Command/option names** — Does the spec describe CLI commands or flags?
   - Compare against `uv run gza --help` and `uv run gza <command> --help`
   - Flag commands that were renamed or flags that changed

3. **Config fields** — Does the spec reference configuration options?
   - Search `src/gza/config.py` for referenced field names
   - Flag fields that were renamed or removed

4. **Code patterns** — Does the spec describe specific functions, classes, or modules?
   - Search with Grep for referenced identifiers
   - Flag identifiers that no longer exist

5. **Workflow steps** — Does the spec describe a multi-step process?
   - Verify each step is still how the feature works
   - Flag steps that have been simplified, reordered, or changed

### Step 3: Classify each spec

For each spec, assign one of:

- **Current** — Spec matches implementation. Note which key claims were verified.
- **Outdated** — Spec contains concrete claims that are wrong. List each issue.
- **Partially outdated** — Some claims are correct, others are wrong. List the wrong ones.
- **Aspirational** — Spec describes features not yet implemented. Note this is expected.
- **Partially aspirational** — Mix of implemented and not-yet-implemented features.
- **Superseded** — Feature was built differently than spec describes. Note the divergence.

### Step 4: Use git blame for context

When unsure if a spec is aspirational or outdated:

```bash
git log --oneline -1 specs/<file>.md
git blame specs/<file>.md | head -5
```

- **Recent specs** (last few weeks) are more likely aspirational
- **Old specs** with recent code changes around the feature are more likely outdated

### Step 5: Compile findings

Generate timestamp and write report:

```bash
date +%Y%m%d%H%M%S
```

Write to `reviews/<timestamp>-spec-review.md`.

## Output Format

```markdown
# Spec Review

## Summary
Reviewed N specs. X current, Y outdated, Z aspirational.

## Outdated Specs

### specs/example.md
**Status:** Outdated
**Issues:**
| Claim | Location in Spec | Actual State |
|-------|-----------------|--------------|
| References `src/gza/tasks.py` | Line 15 | File deleted; logic moved to `src/gza/db.py` |
| Says `--verbose` flag exists | Line 42 | Flag was removed in favor of `--log-level` |

### specs/another.md
**Status:** Partially outdated
**Issues:**
| Claim | Location in Spec | Actual State |
|-------|-----------------|--------------|
| Config field `worker_count` | Line 8 | Renamed to `work_count` |
**Correct claims:** Task chaining via `depends_on` works as described.

## Aspirational Specs (No Action Needed)

| Spec | Summary | Last Modified |
|------|---------|---------------|
| specs/beads-integration.md | Beads integration not yet implemented | 2026-01-15 |

## Current Specs

| Spec | Key Claims Verified |
|------|-------------------|
| specs/docker-testing.md | Dockerfile paths, Docker build flow, volume mounts |

## Recommendations
1. [Priority action 1]
2. [Priority action 2]
```

## Tips

- **Don't auto-update specs** — just flag issues for human review. Specs may be intentionally aspirational.
- **Focus on concrete claims** — file paths, command names, config fields, function signatures. These are objectively verifiable.
- **Skip vague/conceptual content** — prose descriptions of goals and motivations are hard to verify and rarely become "wrong."
- **Group related issues** — if a spec has 5 wrong file paths because of a directory rename, note the rename once rather than listing each path.
- **Note positive findings** — confirming a spec is current is valuable too, so teams know which specs they can trust.
- **Check if the feature exists at all** — before checking details, verify the core feature described by the spec is implemented. If not, it's likely aspirational.
