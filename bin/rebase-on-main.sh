#!/bin/bash
# Rebase current branch on main, using Claude Code or Codex to resolve conflicts
#
# Usage: bin/rebase-on-main.sh [claude|codex]

set -euo pipefail

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
    echo "If conflicts occur, the agent resolves and stages the current conflict set,"
    echo "then this script continues the rebase and offers the force-push prompt."
    echo ""
    echo "Default agent: $DEFAULT_AGENT"
}

git_path() {
    git rev-parse --git-path "$1"
}

rebase_in_progress() {
    [[ -d "$(git_path rebase-merge)" || -d "$(git_path rebase-apply)" ]]
}

list_conflicted_files() {
    git diff --name-only --diff-filter=U
}

show_conflicted_files() {
    local conflicted_files

    conflicted_files="$(list_conflicted_files)"
    if [[ -z "$conflicted_files" ]]; then
        echo "No conflicted files reported."
        return
    fi

    echo "Conflicted files:"
    printf '%s\n' "$conflicted_files"
}

build_resolution_prompt() {
    cat <<EOF
A git rebase is currently stopped on merge conflicts while rebasing branch '$CURRENT_BRANCH' onto '$REBASE_TARGET'.

Resolve only the currently conflicted files reported by:
  git diff --name-only --diff-filter=U

For each conflicted file:
1. Read the file to see the conflict markers.
2. Understand what both sides are trying to add or change.
3. Combine both changes appropriately (usually keeping both additions).
4. Remove the conflict markers.
5. Verify Python syntax with: uv run python -m py_compile <file>
6. Stage the resolved file with: git add <file>

Important:
- Do not run git rebase --continue.
- Do not run git push.
- Do not start a new rebase.
- Do not abort the current rebase.
- Stop and exit after the current conflicted files are resolved and staged so the calling script can continue the rebase.
EOF
}

invoke_agent_for_conflicts() {
    local resolution_prompt

    resolution_prompt="$(build_resolution_prompt)"

    echo "Invoking $SELECTED_AGENT to resolve and stage current conflicts..."

    if [[ "$SELECTED_AGENT" == "claude" ]]; then
        printf '%s\n' "$resolution_prompt" | claude \
            -p - \
            --allowedTools 'Bash(git add:*)' \
            --allowedTools 'Bash(git diff:*)' \
            --allowedTools 'Bash(uv run python -m py_compile:*)' \
            --allowedTools 'Edit' \
            --allowedTools 'Read' \
            --allowedTools 'Glob' \
            --allowedTools 'Grep'
    else
        printf '%s\n' "$resolution_prompt" | codex \
            -c check_for_update_on_startup=false \
            exec \
            --json \
            --dangerously-bypass-approvals-and-sandbox \
            --skip-git-repo-check \
            -C "$(pwd)" \
            -
    fi
}

finish_rebase_success() {
    local post_rebase_head

    post_rebase_head=$(git rev-parse HEAD)

    echo -e "${GREEN}Rebase completed successfully!${NC}"
    if [[ "$PRE_REBASE_HEAD" == "$post_rebase_head" ]]; then
        echo -e "${YELLOW}$CURRENT_BRANCH is already up to date with $REBASE_TARGET.${NC}"
        echo "Rebase made no changes, so there is nothing new to push from this run."
        exit 0
    fi

    echo "Rebased $CURRENT_BRANCH onto $REBASE_TARGET"
    echo ""
    read -p "Push with --force-with-lease? [y/N]: " PUSH_CHOICE
    if [[ "$PUSH_CHOICE" =~ ^[Yy]$ ]]; then
        git push --force-with-lease
        echo -e "${GREEN}Pushed successfully!${NC}"
    fi
}

report_rebase_state_cleared_without_completion() {
    local post_agent_head

    post_agent_head=$(git rev-parse HEAD)

    echo ""
    echo -e "${RED}$SELECTED_AGENT exited and the rebase is no longer in progress, but this script did not complete it.${NC}"
    if [[ "$PRE_REBASE_HEAD" == "$post_agent_head" ]]; then
        echo "HEAD is unchanged from before the rebase attempt."
    fi
    echo "The rebase may have been aborted or altered manually."
    echo "Inspect the branch state and rerun the rebase if needed."
    echo "To abort any partial state manually: git rebase --abort"
}

resolve_rebase_conflicts() {
    local agent_exit_status=0
    local rebase_completed_by_script=0

    while rebase_in_progress; do
        echo ""
        echo -e "${YELLOW}=== Merge conflicts detected ===${NC}"
        echo ""
        show_conflicted_files
        echo ""

        if invoke_agent_for_conflicts; then
            agent_exit_status=0
        else
            agent_exit_status=$?
        fi

        if ! rebase_in_progress; then
            if [[ "$rebase_completed_by_script" -eq 1 ]]; then
                return 0
            fi

            report_rebase_state_cleared_without_completion
            return 1
        fi

        if [[ -n "$(list_conflicted_files)" ]]; then
            echo ""
            if [[ "$agent_exit_status" -ne 0 ]]; then
                echo -e "${RED}$SELECTED_AGENT exited with status $agent_exit_status, and conflicts are still present.${NC}"
            else
                echo -e "${RED}$SELECTED_AGENT exited, but conflicts are still present.${NC}"
            fi
            echo "Resolve the remaining conflicts manually or abort with: git rebase --abort"
            return 1
        fi

        if [[ "$agent_exit_status" -ne 0 ]]; then
            echo ""
            echo -e "${YELLOW}$SELECTED_AGENT exited with status $agent_exit_status after resolving the current conflict set.${NC}"
            echo "Attempting scripted git rebase --continue anyway."
        fi

        echo ""
        echo "Continuing rebase..."
        if git -c core.editor=true rebase --continue; then
            if ! rebase_in_progress; then
                rebase_completed_by_script=1
            fi
            continue
        fi

        if [[ -n "$(list_conflicted_files)" ]]; then
            continue
        fi

        echo ""
        echo -e "${RED}git rebase --continue failed without leaving conflicted files.${NC}"
        echo "Inspect the rebase state manually. To abort: git rebase --abort"
        return 1
    done
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
        git fetch "$REMOTE" "$MAIN_BRANCH"
        ;;
    *)
        echo -e "${RED}Invalid choice. Exiting.${NC}"
        exit 1
        ;;
esac

echo "Rebasing $CURRENT_BRANCH onto $REBASE_TARGET..."

PRE_REBASE_HEAD=$(git rev-parse HEAD)

# Attempt rebase
if git rebase "$REBASE_TARGET"; then
    finish_rebase_success
    exit 0
fi

if ! rebase_in_progress; then
    echo ""
    echo -e "${RED}Rebase failed before entering conflict resolution.${NC}"
    exit 1
fi

resolve_rebase_conflicts
finish_rebase_success
