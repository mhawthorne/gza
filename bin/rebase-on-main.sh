#!/bin/bash
# Rebase current branch on main, using Claude Code to resolve conflicts
#
# Usage: bin/rebase-on-main.sh

set -e

MAIN_BRANCH="main"
REMOTE="origin"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
echo "  1) $REMOTE/$MAIN_BRANCH (remote - default)"
echo "  2) $MAIN_BRANCH (local)"
echo ""
read -p "Choose [1-2] (default: 1): " CHOICE

case "$CHOICE" in
    2)
        REBASE_TARGET="$MAIN_BRANCH"
        echo "Using local $MAIN_BRANCH"
        ;;
    1|"")
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
    echo "To push the rebased branch:"
    echo "  git push --force-with-lease"
    exit 0
fi

# Rebase failed - use Claude Code to resolve conflicts
echo ""
echo -e "${YELLOW}=== Merge conflicts detected ===${NC}"
echo ""
echo "Conflicted files:"
git diff --name-only --diff-filter=U
echo ""

echo "Invoking Claude Code to resolve conflicts..."
claude "Resolve the merge conflicts. For each conflicted file:
1. Read the file to see the conflict markers
2. Understand what both sides are trying to add
3. Combine both changes appropriately (usually keeping both additions)
4. Remove the conflict markers
5. Verify Python syntax with: uv run python -m py_compile <file>
6. Stage the resolved file with: git add <file>

After resolving all conflicts, run: git rebase --continue" \
    --allowedTools 'Bash(git add:*)' \
    --allowedTools 'Bash(git rebase --continue:*)' \
    --allowedTools 'Bash(uv run python -m py_compile:*)' \
    --allowedTools 'Edit' \
    --allowedTools 'Read' \
    --allowedTools 'Glob' \
    --allowedTools 'Grep'

echo ""
echo -e "${YELLOW}Review the changes, then:${NC}"
echo "  git rebase --continue"
echo "  git push --force-with-lease"
echo ""
echo -e "${RED}To abort:${NC}"
echo "  git rebase --abort"
