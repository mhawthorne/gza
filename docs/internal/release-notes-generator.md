# Release Notes Generator

## Overview

The `generate-release-notes.sh` script automatically generates human-readable release notes using Claude AI based on git commits between two tags.

## Location

`bin/generate-release-notes.sh`

## Requirements

- Git repository with tags
- Claude CLI installed (`npm install -g @anthropic-ai/claude-code`)
- Bash shell

## Usage

```bash
./bin/generate-release-notes.sh <from_tag> <to_tag>
```

### Examples

```bash
# Generate release notes between v1.0.0 and v1.1.0
./bin/generate-release-notes.sh v1.0.0 v1.1.0

# Generate release notes for the latest release
./bin/generate-release-notes.sh v1.9.0 v2.0.0
```

## Output Format

The script generates release notes in markdown format with:

1. A header showing the version range
2. A brief summary of overall changes
3. Changes grouped by category:
   - Features
   - Bug Fixes
   - Improvements
   - Documentation
   - Breaking Changes (if any)
4. Bullet points for each change
5. Proper markdown formatting

## How It Works

1. Validates that both tags exist in the repository
2. Extracts all commits between the two tags using `git log`
3. Sends the commit list to Claude with a structured prompt
4. Claude analyzes the commits and generates organized release notes
5. Outputs the formatted markdown to stdout

## Error Handling

The script will exit with an error if:
- Arguments are missing or invalid
- Git tags don't exist
- No commits found between tags
- Claude CLI is not installed

## Tips

- Use semantic versioning tags (e.g., v1.0.0, v2.1.3)
- Ensure commits have descriptive messages for better release notes
- Redirect output to a file: `./bin/generate-release-notes.sh v1.0.0 v2.0.0 > RELEASE_NOTES.md`
- Review and edit the generated notes before publishing
