#!/bin/bash
# Rebase current branch on main, using Claude Code or Codex to resolve conflicts
#
# Usage: bin/rebase-on-main.sh [claude|codex]

set -e

MAIN_BRANCH="main"
REMOTE="origin"
DEFAULT_AGENT="claude"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 [claude|codex]"
    echo ""
    echo "Rebases the current branch onto main and invokes the selected AI agent"
    echo "to resolve conflicts if the rebase stops."
    echo ""
    echo "Default agent: $DEFAULT_AGENT"
}

SELECTED_AGENT="${1:-$DEFAULT_AGENT}"

case "$SELECTED_AGENT" in
    claude|codex)
        ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        echo -e "${RED}Error: Invalid agent '$SELECTED_AGENT'. Use 'claude' or 'codex'.${NC}"
        echo ""
        usage
        exit 1
        ;;
esac

if ! command -v "$SELECTED_AGENT" >/dev/null 2>&1; then
    echo -e "${RED}Error: '$SELECTED_AGENT' CLI not found on PATH.${NC}"
    exit 1
fi

CURRENT_BRANCH=$(git branch --show-current)

# Check for uncommitted changes before attempting rebase
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo -e "${RED}Error: You have uncommitted changes.${NC}"
    echo "Please commit or stash them before rebasing."
    echo ""
    echo "Unstaged changes:"
    git diff --name-only
    echo ""
    echo "Staged changes:"
    git diff --cached --name-only
    exit 1
fi

# Ask user which base to rebase against
echo ""
echo "Rebase $CURRENT_BRANCH onto:"
echo "  1) $MAIN_BRANCH (local - default)"
echo "  2) $REMOTE/$MAIN_BRANCH (remote)"
echo ""
read -p "Choose [1-2] (default: 1): " CHOICE

case "$CHOICE" in
    1|"")
        REBASE_TARGET="$MAIN_BRANCH"
        echo "Using local $MAIN_BRANCH"
        ;;
    2)
        REBASE_TARGET="$REMOTE/$MAIN_BRANCH"
        echo "Fetching latest from $REMOTE..."
        git fetch $REMOTE $MAIN_BRANCH
        ;;
    *)
        echo -e "${RED}Invalid choice. Exiting.${NC}"
        exit 1
        ;;
esac

echo "Rebasing $CURRENT_BRANCH onto $REBASE_TARGET..."

# Attempt rebase
if git rebase $REBASE_TARGET; then
    echo -e "${GREEN}Rebase completed successfully!${NC}"
    echo "Rebased $CURRENT_BRANCH onto $REBASE_TARGET"
    echo ""
    read -p "Push with --force-with-lease? [y/N]: " PUSH_CHOICE
    if [[ "$PUSH_CHOICE" =~ ^[Yy]$ ]]; then
        git push --force-with-lease
        echo -e "${GREEN}Pushed successfully!${NC}"
    fi
    exit 0
fi

# Rebase failed - use selected agent to resolve conflicts
echo ""
echo -e "${YELLOW}=== Merge conflicts detected ===${NC}"
echo ""
echo "Conflicted files:"
git diff --name-only --diff-filter=U
echo ""

RESOLUTION_PROMPT="Resolve the merge conflicts. For each conflicted file:
1. Read the file to see the conflict markers
2. Understand what both sides are trying to add
3. Combine both changes appropriately (usually keeping both additions)
4. Remove the conflict markers
5. Verify Python syntax with: uv run python -m py_compile <file>
6. Stage the resolved file with: git add <file>

After resolving all conflicts, run: git rebase --continue"

echo "Invoking $SELECTED_AGENT to resolve conflicts..."

if [[ "$SELECTED_AGENT" == "claude" ]]; then
    claude "$RESOLUTION_PROMPT" \
        --allowedTools 'Bash(git add:*)' \
        --allowedTools 'Bash(git rebase --continue:*)' \
        --allowedTools 'Bash(uv run python -m py_compile:*)' \
        --allowedTools 'Edit' \
        --allowedTools 'Read' \
        --allowedTools 'Glob' \
        --allowedTools 'Grep'
else
    codex -C "$(pwd)" "$RESOLUTION_PROMPT"
fi

echo ""
echo -e "${YELLOW}Review the changes, then:${NC}"
echo "  git rebase --continue"
echo "  git push --force-with-lease"
echo ""
echo -e "${RED}To abort:${NC}"
echo "  git rebase --abort"
