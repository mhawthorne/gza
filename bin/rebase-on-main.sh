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

echo "Fetching latest from $REMOTE..."
git fetch $REMOTE $MAIN_BRANCH

echo "Rebasing $CURRENT_BRANCH onto $REMOTE/$MAIN_BRANCH..."

# Attempt rebase
if git rebase $REMOTE/$MAIN_BRANCH; then
    echo -e "${GREEN}Rebase completed successfully!${NC}"
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
