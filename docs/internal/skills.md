# Internal Skills

These skills are used within gza's own development workflow. They are not installed via `gza claude-install-skills` — they live in gza's own `.claude/skills/` directory and are available when working inside the gza repository.

---

## gza-test-and-fix

**Run mypy and pytest, fix any errors found in files changed on the current branch, then commit all fixes.**

Invoke with `/gza-test-and-fix` before declaring any task complete. Per AGENTS.md, this is required before marking a task done.

**Key behaviors:**
- Compares against `main` with `git diff --name-only main...HEAD` — only fixes errors in changed files, never touches unrelated files
- Runs mypy first, then pytest; up to 2 fix iterations per tool
- If errors remain after 2 rounds, reports them but continues
- Commits all fixes in a single commit at the end (not per-fix)
- Never runs `git push` — leaves that to you

**When to use:** Before completing any gza task. Run it last, after all code changes are done.

---

## gza-code-review-full

**Comprehensive pre-release code review assessing test coverage, code duplication, and component interactions.**

Use `/gza-code-review-full` before a release or when you want a full quality assessment of the gza codebase.

**Key behaviors:**
- Reviews 10 dimensions: unit test coverage, functional test coverage, code duplication, component interactions, error handling, API consistency, configuration/hardcoding, logging, resource management, and type safety
- Runs mypy and pytest as part of the review
- Writes findings to `reviews/<timestamp>-code-review-full.md` in the project root

**When to use:**
- Before cutting a release
- When the codebase has grown and you want a health check
- To identify areas needing more tests or refactoring

---

## gza-generate-commit-message

**Generate a concise commit message for staged git changes using Claude.**

Use `/gza-generate-commit-message` after staging changes with `git add` to get a well-formatted commit message without writing it manually.

**Key behaviors:**
- Checks for staged changes first; exits with a message if nothing is staged
- Reads `git diff --staged` and recent `git log` for style context
- Outputs the raw commit message only — no explanations or markdown fences
- Follows imperative mood convention ("Add", "Fix", "Update") and keeps the summary under 50 characters

**Output format:**

```
Add user authentication

- Add JWT token validation in src/api/auth.py
- Add login endpoint with rate limiting
```

---

## gza-docs-review

**Review documentation for accuracy, completeness, and missing information that users may need.**

Use `/gza-docs-review` to audit gza's documentation before a release or after adding new features.

**Key behaviors:**
- Discovers all docs under `docs/`, `README.md`, and related root files
- Verifies CLI docs against actual `--help` output to catch missing commands, wrong flags, or deprecated features
- Checks internal consistency: working links, consistent terminology, current examples
- Also reviews `specs/` directory for outdated or aspirational specs (flags issues without auto-updating)
- Writes findings to `reviews/<timestamp>-docs-review.md`

**Output sections:**
- Accuracy Issues (wrong syntax, missing options, deprecated features)
- Missing Information (undocumented commands, missing examples)
- Minor Issues (broken links, typos)
- Spec Review (outdated vs aspirational specs)
