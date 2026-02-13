---
name: docs-review
description: Review documentation for accuracy, completeness, and missing information that users may need
allowed-tools: Read, Glob, Grep, Write, Bash(ls:*), Bash(uv run *--help*), Bash(date +%Y%m%d%H%M%S)
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

Write findings to `reviews/<timestamp>-docs-review.md`.

1. **Generate timestamp:**
   ```bash
   date +%Y%m%d%H%M%S
   ```

2. **Write the report** to `reviews/<timestamp>-docs-review.md` with this structure:

```markdown
# Documentation Review

## Overall Summary
[1-2 sentence summary]

## Accuracy Issues Found
| Issue | Location | Details |
|-------|----------|---------|
| Missing command X | config.md | CLI has `foo` but docs don't mention it |

## Missing Information
| Topic | Why Users Need It |
|-------|-------------------|
| Error handling | Users won't know how to recover from failures |

## Minor Issues
- [list of small fixes]

## Spec Review

### Outdated Specs
| Spec | Issue | Details |
|------|-------|---------|

### Possibly Aspirational
| Spec | Notes |
|------|-------|

### Specs OK
- [list of specs that match implementation]

## Recommendations
1. [Priority fix 1]
2. [Priority fix 2]
```

3. **Tell the user** the path to the review file so they can open it.

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

---

## Part 2: Spec Review

Review specification documents in `specs/` for accuracy against the current implementation.

### Important: Aspirational vs Outdated

Specs can be **forward-looking** (describing planned features) or **outdated** (describing old behavior). Use this heuristic:

- **Aspirational (skip)**: Describes functionality that doesn't exist in code but sounds intentional/planned. Leave these alone.
- **Outdated (flag)**: Describes functionality that *used to* work differently, or references old file paths, old command names, or deprecated patterns.

When in doubt, flag it with a note that it "may be aspirational."

### Step 1: Discover specs

```bash
ls specs/
```

### Step 2: Review each spec

For each spec file:

1. **Read the spec** to understand what it describes
2. **Check if the feature exists** - search for relevant code, commands, or config
3. **Compare behavior** - does the implementation match the spec?

Look for:
- **File paths that don't exist** - spec references `src/foo/bar.py` but file is gone or moved
- **Command/option names that changed** - spec says `--old-flag` but CLI uses `--new-flag`
- **Workflow steps that no longer apply** - spec describes a process that's been simplified or changed
- **Config fields that were renamed or removed**

### Step 3: Compile spec findings

Add a "Spec Review" section to your report:

```markdown
### Spec Review

#### Outdated Specs
| Spec | Issue | Details |
|------|-------|---------|
| task-resume.md | Wrong file path | References `src/gza/resume.py` but logic is now in `runner.py` |

#### Possibly Aspirational (needs human review)
| Spec | Notes |
|------|-------|
| beads-integration.md | Describes beads integration but no beads code found - may be planned |

#### Specs OK
- task-chaining.md - matches implementation
- docker-testing.md - matches implementation
```

### Tips for spec review

- **Don't auto-update specs** - just flag issues for human review
- **Check git blame** if unsure - recent specs are more likely aspirational
- **Focus on concrete claims** - file paths, command names, config fields
- **Skip vague/conceptual content** - prose descriptions of goals are hard to verify
