#!/bin/bash
# Merge a branch into main with a diff review and confirmation prompt.
#
# Usage: bin/merge-to-main.sh [branch-name]
#
# If no branch is given, uses the current branch.
# Can be run from a worktree — it operates on the main repo's main branch.

set -e

MAIN_BRANCH="main"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Determine branch to merge
if [[ -n "$1" ]]; then
    BRANCH="$1"
else
    BRANCH=$(git branch --show-current)
fi

if [[ "$BRANCH" == "$MAIN_BRANCH" ]]; then
    echo -e "${RED}Error: Already on $MAIN_BRANCH, nothing to merge.${NC}"
    exit 1
fi

# Make sure the branch exists
if ! git rev-parse --verify "$BRANCH" &>/dev/null; then
    echo -e "${RED}Error: Branch '$BRANCH' not found.${NC}"
    exit 1
fi

# Find the repo root (not the worktree root)
REPO_ROOT=$(git rev-parse --path-format=absolute --git-common-dir | sed 's|/.git$||')

echo -e "${CYAN}=== Merging ${BRANCH} into ${MAIN_BRANCH} ===${NC}"
echo ""

# Show commit log
COMMIT_COUNT=$(git rev-list --count "$MAIN_BRANCH".."$BRANCH")
echo -e "${YELLOW}Commits ($COMMIT_COUNT):${NC}"
git log --oneline "$MAIN_BRANCH".."$BRANCH"
echo ""

# Show diffstat
echo -e "${YELLOW}Files changed:${NC}"
git diff "$MAIN_BRANCH"..."$BRANCH" --stat
echo ""

# Show full diff
echo -e "${YELLOW}Full diff:${NC}"
git diff "$MAIN_BRANCH"..."$BRANCH"
echo ""

# Confirm
read -p "Merge $BRANCH into $MAIN_BRANCH? [y/N] " confirm
if [[ "$confirm" != [yY] ]]; then
    echo "Aborted."
    exit 0
fi

# Perform the merge from the main repo root so we can checkout main
cd "$REPO_ROOT"

ORIGINAL_BRANCH=$(git branch --show-current)

git checkout "$MAIN_BRANCH"
if git merge --no-ff "$BRANCH" -m "Merge branch '$BRANCH'"; then
    echo ""
    echo -e "${GREEN}Merged $BRANCH into $MAIN_BRANCH.${NC}"
else
    echo ""
    echo -e "${RED}Merge failed. Resolve conflicts, then commit.${NC}"
    exit 1
fi

# Switch back if we were on a different branch
if [[ -n "$ORIGINAL_BRANCH" && "$ORIGINAL_BRANCH" != "$MAIN_BRANCH" ]]; then
    git checkout "$ORIGINAL_BRANCH"
fi

echo ""
echo -e "${YELLOW}To push:${NC}  git push origin $MAIN_BRANCH"
