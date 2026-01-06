#!/usr/bin/env bash
# Generate a commit message using Claude for staged changes
# Usage: ./scripts/generate-commit-msg.sh

set -e

# Check if there are staged changes
if git diff --staged --quiet; then
    echo "No staged changes to commit."
    exit 1
fi

# Gather context
STAGED_DIFF=$(git diff --staged)
RECENT_COMMITS=$(git log --oneline -5 2>/dev/null || echo "No previous commits")

# Create the prompt
PROMPT="Generate a concise git commit message for these staged changes.

Follow this format:
- First line: short summary (50 chars or less, imperative mood)
- Blank line
- Bullet points for key changes (if multiple changes)

Recent commits for style reference:
$RECENT_COMMITS

Staged diff:
$STAGED_DIFF

Output ONLY the commit message, no explanations or markdown formatting."

# Use Claude to generate the message
echo "Generating commit message..."
echo ""

if command -v claude &> /dev/null; then
    echo "$PROMPT" | claude --print
else
    echo "Error: 'claude' CLI not found. Install it with: npm install -g @anthropic-ai/claude-code"
    exit 1
fi
