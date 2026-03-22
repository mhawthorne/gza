---
name: gza-log-insights
description: Analyze gza run logs to find recurring anti-patterns, wasted effort, and suggest AGENTS.md or workflow improvements
allowed-tools: Read, Bash(uv run python -c:*), Bash(wc:*), Bash(ls:*), Bash(head:*), Bash(tail:*)
version: 1.0.0
public: false
---

# Gza Log Insights

Analyze gza execution logs to find recurring anti-patterns, wasted compute, and actionable improvements. This skill scans the JSONL log files, aggregates patterns across many runs, and produces recommendations for AGENTS.md updates, prompt improvements, or workflow changes.

## Process

### Step 1: Locate and inventory logs

Find the log directory and count available logs:

```bash
uv run python -c "
from gza.config import load_config
cfg = load_config()
log_dir = cfg.get_log_dir()
print(str(log_dir))
"
```

Then list and count:
```bash
ls <log_dir> | wc -l
```

If no logs exist, report that and stop.

### Step 2: Run the analysis script

Run the following comprehensive analysis across all log files. This script extracts patterns from the JSONL log format (where each line is a JSON entry with types: system, assistant, user, result).

```bash
uv run python -c "
import json, os, re, sys
from collections import Counter, defaultdict
from pathlib import Path

from gza.config import load_config
cfg = load_config()
log_dir = cfg.get_log_dir()

log_files = sorted(log_dir.glob('*.log'))
if not log_files:
    print('No log files found.')
    sys.exit(0)

# --- Counters ---
bare_commands = Counter()          # commands missing 'uv run'
failed_bash = Counter()            # bash commands that failed
git_errors = Counter()             # git-specific errors
tool_distribution = Counter()      # overall tool usage
skill_errors = Counter()           # failed skill executions
import_errors = Counter()          # Python import errors
file_too_large = 0                 # Read tool file-too-large errors
no_module_pytest = 0               # 'No module named pytest'
sqlite_not_found = 0               # sqlite3 not available
worktree_git_errors = 0            # git fails in cleaned-up worktrees
test_runs_per_log = []             # (filename, count) for test-heavy logs
result_subtypes = Counter()        # success vs error_max_turns etc
costs = []                         # per-log costs
high_cost_logs = []                # logs with cost info
repeated_patterns = Counter()      # any command run 5+ times in a single log

BARE_PREFIXES = ['gza ', 'pytest', 'mypy ', 'python ']

for logfile in log_files:
    tool_uses_in_log = {}   # tool_use_id -> command
    test_runs = 0
    bash_cmds_in_log = Counter()
    fname = logfile.name

    with open(logfile) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue

            etype = entry.get('type', '')

            # --- System init: skip ---

            # --- Assistant messages: extract tool calls ---
            if etype == 'assistant':
                msg = entry.get('message', {})
                for c in msg.get('content', []):
                    if c.get('type') == 'tool_use':
                        tool = c.get('name', '')
                        inp = c.get('input', {})
                        tid = c.get('id', '')
                        tool_distribution[tool] += 1

                        if tool == 'Bash':
                            cmd = inp.get('command', '').strip()
                            tool_uses_in_log[tid] = cmd
                            bash_cmds_in_log[cmd[:80]] += 1

                            # Check bare commands
                            for prefix in BARE_PREFIXES:
                                if cmd.startswith(prefix) and not cmd.startswith('uv run'):
                                    bare_commands[cmd[:100]] += 1

                            # Count test/lint runs
                            if 'pytest' in cmd or 'mypy' in cmd:
                                test_runs += 1

            # --- User messages: extract tool results ---
            if etype == 'user':
                msg = entry.get('message', {})
                for c in msg.get('content', []):
                    if c.get('type') != 'tool_result':
                        continue
                    tid = c.get('tool_use_id', '')
                    is_err = c.get('is_error', False)
                    content = str(c.get('content', ''))

                    # Skill errors — 'Execute skill: X' with is_error=True is NORMAL
                    # in headless mode (skill loaded successfully). Only count as
                    # error if the content indicates a real failure (e.g. 'Unknown skill').
                    if is_err and 'skill' in content.lower():
                        if 'Execute skill' in content:
                            pass  # Normal headless behavior, not an error
                        elif 'Unknown skill' in content:
                            skill_name = content.split('Unknown skill:')[-1].strip()[:40] if 'Unknown skill:' in content else content[:60]
                            skill_errors[skill_name] += 1
                        else:
                            skill_errors[content[:60]] += 1

                    # File too large
                    if 'exceeds maximum allowed' in content:
                        file_too_large += 1

                    # Specific error categories
                    if 'not a git repository' in content:
                        worktree_git_errors += 1
                    if 'sqlite3: command not found' in content:
                        sqlite_not_found += 1
                    if 'No module named pytest' in content:
                        no_module_pytest += 1
                    if 'ImportError' in content:
                        idx = content.find('ImportError')
                        import_errors[content[idx:idx+80]] += 1

                    # Failed bash commands
                    if tid in tool_uses_in_log:
                        cmd = tool_uses_in_log[tid]
                        exit_match = re.search(r'Exit code (\d+)', content[:30])
                        if exit_match and exit_match.group(1) != '0':
                            short = cmd[:60]
                            failed_bash[short] += 1
                            if cmd.strip().startswith('git'):
                                git_errors[short] += 1

            # --- Result entry ---
            if etype == 'result':
                result_subtypes[entry.get('subtype', '?')] += 1
                cost = entry.get('total_cost_usd', 0)
                if cost:
                    costs.append(cost)
                    high_cost_logs.append((fname, cost, entry.get('num_turns', 0)))

    if test_runs > 0:
        test_runs_per_log.append((fname, test_runs))

    # Repeated commands in single log
    for cmd, count in bash_cmds_in_log.items():
        if count >= 5:
            repeated_patterns[cmd] += 1

# ========== OUTPUT ==========
print('=' * 70)
print('GZA LOG INSIGHTS REPORT')
print(f'Analyzed {len(log_files)} log files')
print('=' * 70)

# Section 1: Outcome summary
print('\n## Task Outcomes')
for st, count in result_subtypes.most_common():
    print(f'  {st}: {count}')
if costs:
    print(f'  Total spend: \${sum(costs):.2f} across {len(costs)} tasks')
    print(f'  Average cost: \${sum(costs)/len(costs):.2f}/task')

# Section 2: Bare commands
if bare_commands:
    print(f'\n## Bare Commands (missing uv run) — {sum(bare_commands.values())} total')
    print('These commands were invoked without \"uv run\" prefix, which may fail in')
    print('environments without the package installed globally.')
    for cmd, count in bare_commands.most_common(15):
        print(f'  {count}x: {cmd}')

# Section 3: Git errors
if worktree_git_errors or git_errors:
    print(f'\n## Git Errors — {worktree_git_errors} "not a git repository" + {sum(git_errors.values())} failed git commands')
    print('Includes stale worktrees, missing repos, and other git failures.')
    for cmd, count in git_errors.most_common(10):
        print(f'  {count}x: {cmd}')

# Section 4: Missing tools/modules
missing = []
if no_module_pytest:
    missing.append(f'\"No module named pytest\": {no_module_pytest} occurrences')
if sqlite_not_found:
    missing.append(f'\"sqlite3: command not found\": {sqlite_not_found} occurrences')
if file_too_large:
    missing.append(f'Read tool file-too-large errors: {file_too_large} occurrences')
if import_errors:
    for err, count in import_errors.most_common(5):
        missing.append(f'{err}: {count}x')
if missing:
    print(f'\n## Missing Dependencies / Environment Issues')
    for m in missing:
        print(f'  - {m}')

# Section 5: Skill resolution errors (not counting normal 'Execute skill' responses)
if skill_errors:
    print(f'\n## Skill Resolution Errors — {sum(skill_errors.values())} total')
    print('Note: \"Execute skill: X\" with is_error=True is normal in headless mode.')
    print('Only \"Unknown skill\" and other genuine failures are counted here.')
    for skill, count in skill_errors.most_common():
        print(f'  {count}x: {skill}')

# Section 6: Test-heavy logs (potential loops)
heavy = [(f, c) for f, c in test_runs_per_log if c >= 8]
if heavy:
    print(f'\n## Test-Heavy Runs (8+ test/lint invocations — possible loops)')
    for fname, count in sorted(heavy, key=lambda x: -x[1])[:10]:
        print(f'  {count} runs: {fname[:70]}')

# Section 7: Repeated commands within single logs
if repeated_patterns:
    print(f'\n## Repeated Commands (same command 5+ times in one session)')
    for cmd, num_logs in repeated_patterns.most_common(10):
        print(f'  in {num_logs} log(s): {cmd}')

# Section 8: Failed bash commands
if failed_bash:
    print(f'\n## Most Common Bash Failures — {sum(failed_bash.values())} total')
    for cmd, count in failed_bash.most_common(15):
        print(f'  {count}x: {cmd}')

# Section 9: Cost outliers
if high_cost_logs:
    expensive = sorted(high_cost_logs, key=lambda x: -x[1])[:5]
    print(f'\n## Most Expensive Runs')
    for fname, cost, turns in expensive:
        print(f'  \${cost:.2f} ({turns} turns): {fname[:60]}')

# Section 10: Tool distribution
print(f'\n## Tool Usage Distribution')
for tool, count in tool_distribution.most_common():
    print(f'  {tool}: {count}')

print()
"
```

### Step 3: Read AGENTS.md for current guidance

Read the project's AGENTS.md to understand what instructions agents already have. This helps identify gaps — patterns in the logs that aren't addressed by existing documentation.

### Step 4: Synthesize recommendations

Based on the analysis, produce actionable recommendations in these categories:

#### A. AGENTS.md Updates
For each anti-pattern found in logs, suggest a specific line to add to AGENTS.md that would prevent the issue. Examples:

- If bare `pytest`/`mypy`/`gza` commands are common:
  > Add to AGENTS.md: "Always use `uv run pytest`, `uv run mypy`, `uv run gza` — never bare commands. The project uses uv for dependency management."

- If git worktree errors are frequent:
  > Add to AGENTS.md: "When running in a worktree, verify git works before running git commands. If the worktree's .git file is stale, do not attempt git init or repair — report the issue instead."

- If `python -m pytest` is used instead of `uv run pytest`:
  > Add to AGENTS.md: "Use `uv run pytest` (not `python -m pytest` or bare `pytest`). The uv tool manages the virtual environment."

- If sqlite3 is used directly:
  > Add to AGENTS.md: "Do not use the `sqlite3` CLI — it may not be installed. Use `uv run python -c 'from gza.db import ...'` to query the database."

- If the Read tool hits file-too-large errors:
  > Add to AGENTS.md: "Large files: always use offset/limit parameters with the Read tool. Key large files: [list files that triggered errors]."

#### B. Skill Improvements
If skills are failing repeatedly, identify why and suggest fixes:
- Is the skill calling tools it doesn't have permission for?
- Is the skill assuming a tool/command exists that doesn't?
- Should the skill's `allowed-tools` be updated?

#### C. Subagent Usage Opportunities
Consider whether any anti-pattern could be solved or mitigated by using subagents (the Agent tool). Common cases:

- **File-too-large errors or long grep-then-read cycles**: Suggest delegating exploration to an Explore subagent, which can search and summarize without filling the main context window.
- **Repeated searches for the same information**: Suggest using an Explore agent upfront to gather context, rather than incrementally searching across many turns.
- **Multi-file research before making changes**: Suggest an Explore agent to map out the relevant code first, then act on the summary.
- **Expensive review/analysis tasks**: Suggest breaking the work into parallel subagents (e.g., one for test analysis, one for code review) to reduce wall-clock time.
- **Agent stuck trying to understand unfamiliar code**: Suggest adding an Explore step to the prompt/workflow that runs before implementation begins.

When recommending subagent use, be specific about which agent type (Explore, Plan, general-purpose) and what the prompt should focus on.

#### D. Prompt/Workflow Improvements
- If test loops are common (8+ test runs per session), suggest adding circuit-breaker guidance: "If the same test fails 3 times with the same error, stop and report the issue."
- If imports fail because the agent uses outdated API names, suggest adding API reference notes to AGENTS.md.
- If high-cost runs correlate with specific task types, flag those for prompt refinement.

#### E. Infrastructure/Environment Fixes
- Missing system tools (sqlite3, etc.)
- Docker environment gaps
- Dependency issues

### Step 5: Output the report

Present findings as:

```
## Log Analysis Summary
[Key metrics: total logs, success rate, total spend, avg cost]

## Top Anti-Patterns Found
1. **[Pattern name]** — [count] occurrences
   - Impact: [wasted turns/cost estimate]
   - Fix: [specific recommendation]

2. **[Pattern name]** — [count] occurrences
   ...

## Recommended AGENTS.md Additions
[Specific lines to add, with rationale]

## Skill Fixes Needed
[Specific skill changes, if any]

## Other Recommendations
[Infrastructure, workflow, or prompt changes]
```

Prioritize recommendations by **impact** (frequency x cost-per-occurrence). A pattern that wastes 2 turns across 100 logs is higher priority than one that wastes 10 turns in 2 logs.

### Step 6: Save the report

Write the full report to `.gza/log-insights/<date>.md` using `YYYYmmdd` format (e.g., `.gza/log-insights/20260319.md`). Create the directory if it doesn't exist. If a report for today already exists, overwrite it.

The saved report should be the same markdown you presented to the user — the complete analysis with all sections.

### Step 7: Offer to apply changes

Ask the user:

> Would you like me to apply any of these recommendations? I can:
> 1. Add the suggested lines to AGENTS.md
> 2. Fix the identified skill issues
> 3. Create a gza task to address infrastructure gaps

## Important notes

- **Log format**: Logs are JSONL with entry types: `system` (init), `assistant` (model responses with tool_use), `user` (tool_result responses), `result` (final summary with cost/turns/duration)
- **Two schemas**: Some logs use schema v1 (message-based) and newer ones use schema v2 (turn.started/turn.completed). The analysis script handles both.
- **Large log directories**: If there are 500+ logs, the script processes them all — this is fine, JSONL parsing is fast
- **Privacy**: Log content may contain file contents and code — focus on patterns, not specific code content
- **Relative paths**: Log dir comes from config, which is relative to project root. Use `load_config()` to resolve it properly.
- **Actionability**: Every finding should have a concrete recommendation. Don't just report problems — suggest the fix.
