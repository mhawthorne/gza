---
name: review-docs
description: Review documentation for accuracy, completeness, and missing information that users may need
allowed-tools: Read, Glob, Grep, Bash(ls:*), Bash(uv run *--help*)
---

# Documentation Review Skill

Evaluate project documentation for accuracy and identify gaps that potential users may encounter.

## When to Use

- User asks to review/evaluate documentation
- User asks "are the docs accurate?"
- User asks "what's missing from the docs?"
- Before a release to ensure docs match implementation

## Process

### Step 1: Discover documentation structure

1. **Find all documentation files:**
   ```bash
   ls docs/
   ```

2. **Check for README and other root docs:**
   - README.md
   - CONTRIBUTING.md
   - CHANGELOG.md

3. **Map the documentation structure** to understand what's documented.

### Step 2: Read the documentation

Read key documentation files:
- README.md (entry point)
- Quick start / getting started guide
- Configuration reference
- API/CLI reference
- Examples/tutorials

### Step 3: Verify against implementation

For CLI tools, compare docs against actual `--help` output:

```bash
uv run <tool> --help
uv run <tool> <command> --help
```

Check for:
- **Missing commands** - commands in CLI but not in docs
- **Missing options** - flags/options not documented
- **Incorrect syntax** - documented syntax doesn't match actual
- **Deprecated features** - docs mention features that no longer exist

For libraries/APIs:
- Compare documented functions/classes against actual code
- Check if examples still work
- Verify type signatures match

### Step 4: Identify information gaps

Look for missing information users commonly need:

**Installation & Setup:**
- [ ] Prerequisites clearly listed?
- [ ] Installation steps complete?
- [ ] Authentication/credentials setup?
- [ ] First-run experience documented?

**Core Concepts:**
- [ ] Key terms defined?
- [ ] Architecture/flow explained?
- [ ] Data model documented?

**Usage:**
- [ ] Common workflows covered?
- [ ] Examples for each major feature?
- [ ] Error messages explained?

**Troubleshooting:**
- [ ] Common errors documented?
- [ ] FAQ section?
- [ ] Debug/verbose mode explained?

**Reference:**
- [ ] All commands/functions documented?
- [ ] All options/parameters listed?
- [ ] Default values specified?
- [ ] Environment variables listed?

### Step 5: Check internal consistency

- Do links work (especially relative links)?
- Is terminology consistent across docs?
- Do examples use consistent patterns?
- Are version numbers/dates current?

### Step 6: Compile findings

Organize findings into categories:

#### Accuracy Issues
Things that are wrong or outdated:
- Incorrect command syntax
- Missing options/flags
- Deprecated features still documented
- Wrong default values

#### Missing Information
Things users may need but aren't documented:
- Undocumented commands/features
- Missing conceptual explanations
- No troubleshooting guidance
- Missing examples for common use cases

#### Minor Issues
Non-critical improvements:
- Broken links
- Typos
- Inconsistent formatting
- Outdated examples

## Output Format

Present findings as a structured report:

```markdown
## Documentation Assessment

### Overall Summary
[1-2 sentence summary]

### Accuracy Issues Found
| Issue | Location | Details |
|-------|----------|---------|
| Missing command X | config.md | CLI has `foo` but docs don't mention it |

### Missing Information
| Topic | Why Users Need It |
|-------|-------------------|
| Error handling | Users won't know how to recover from failures |

### Minor Issues
- [list of small fixes]

### Recommendations
1. [Priority fix 1]
2. [Priority fix 2]
```

## Tips

- **Prioritize user journey** - Focus on what a new user needs to get started
- **Think like a newcomer** - What would confuse someone who doesn't know the tool?
- **Check edge cases** - Error states, unusual configurations, advanced features
- **Verify examples** - Outdated examples are worse than no examples
- **Note positive findings too** - Call out what's done well

## Common Documentation Gaps

Based on patterns across projects, commonly missing items:

1. **Task/object lifecycle** - States and transitions
2. **Resume vs retry semantics** - When to use which
3. **Cost/resource expectations** - What will this cost me?
4. **Worktree/workspace concepts** - How parallel execution works
5. **Dependency resolution** - How ordering is determined
6. **Error recovery** - What to do when things fail
