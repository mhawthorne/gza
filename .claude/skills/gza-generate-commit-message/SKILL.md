---
name: gza-generate-commit-message
description: Generate a concise commit message for staged git changes using Claude
allowed-tools: Bash(git diff:*), Bash(git log:*)
version: 1.0.0
public: true
---

# Generate Commit Message

Generate a concise, well-formatted git commit message for the currently staged changes.

## Process

### Step 1: Check for staged changes

Run `git diff --staged --quiet` to check if there are staged changes. If the exit code is 0 (no staged changes), output:

```
No staged changes to commit.
```

And stop.

### Step 2: Gather context

Run both commands in parallel:

1. `git diff --staged` — get the full staged diff
2. `git log --oneline -5` — get recent commits for style reference

### Step 3: Generate the commit message

Using the staged diff and recent commits as context, generate a commit message following this format:

- **First line**: Short summary (50 chars or less, imperative mood, e.g. "Add user authentication")
- **Blank line**
- **Bullet points**: One bullet per key change (if there are multiple distinct changes)

Style guidance:
- Match the tone and style of recent commits when possible
- Use imperative mood ("Add", "Fix", "Update", not "Added", "Fixed", "Updated")
- Keep the summary line under 50 characters
- Be specific but concise in bullet points

### Step 4: Output

Output **only** the commit message — no explanations, no markdown code fences, no preamble. The raw commit message text only.
