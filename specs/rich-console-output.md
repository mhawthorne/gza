# Rich Console Output for `gza work`

## Overview

Add the `rich` library to beautify `gza work` output with colors, better formatting, and visual hierarchy.

## Dependencies

Add `rich>=13.0` to `pyproject.toml` dependencies.

## Implementation Approach

### 1. Create a Console Module

Create `src/gza/console.py` - a thin wrapper around rich.Console that provides:
- A shared `console` instance for all output
- Styled helper functions for common patterns
- Consistent color scheme

```python
# Key functions:
console = Console()  # shared instance

def task_header(prompt: str, task_id: str, task_type: str) -> None
def stats_line(stats: TaskStats, has_commits: bool | None = None) -> None
def success_message(title: str) -> None
def error_message(message: str) -> None
def warning_message(message: str) -> None
def next_steps(commands: list[tuple[str, str]]) -> None  # (command, comment)
def info_line(label: str, value: str) -> None
```

### 2. Color Scheme

| Element | Color/Style |
|---------|-------------|
| Task header | Bold cyan |
| Task type | Magenta |
| Task ID | Dim |
| Success header | Bold green |
| Error message | Bold red |
| Warning | Yellow |
| Stats labels | Dim |
| Stats values | White/bold |
| Branch name | Blue |
| Commands | Cyan |
| Comments | Dim |
| Checkmarks | Green |

### 3. Files to Modify

1. **pyproject.toml** - Add `rich>=13.0` to dependencies
2. **src/gza/console.py** (new) - Console helpers
3. **src/gza/runner.py** - Replace print() calls with console functions
   - Lines 29-41: `print_stats` -> use `stats_line`
   - Lines 401-410: Credential messages
   - Lines 470-495: Task header
   - Lines 541-560: Worktree creation messages
   - Lines 614-701: Success/failure output
   - Lines 823-901: Non-code task output
   - Lines 376-377: Auto-review message

### 4. Sample Output Transformation

**Before:**
```
=== Task: Add user authentication ===
    ID: 20260211-add-user-authentication
    Type: implement
Creating worktree: /tmp/gza-worktrees/...

=== Done ===
Stats: Runtime: 5m 23s | Turns: 45 | Cost: $0.8234 | Commits: yes
Task ID: 42
Branch: gza/20260211-add-user-authentication

Next steps:
  gza merge 42           # merge branch for task
  gza pr 42              # create a PR
```

**After:**
Same structure but with:
- Cyan task headers (`=== Task: ... ===`)
- Green success headers (`=== Done ===`)
- Red error messages
- Blue branch names
- Dimmed comments
- Colored/bold stats values

### 5. Scope

Focus on `gza work` output only (runner.py). Other CLI commands can be updated in a follow-up task if desired.

## Verification

1. Run `uv run pytest tests/ -v` to ensure no regressions
2. Run `gza work` on a test task to see styled output
3. Verify output looks correct on both light and dark terminals
4. Pipe output to file to verify no ANSI codes when not a TTY (rich handles this automatically)
