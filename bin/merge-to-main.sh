#!/bin/bash
# Merge a branch into main with a diff review and confirmation prompt.
#
# Usage: bin/merge-to-main.sh [branch-name] [claude|codex]
#
# If no branch is given, uses the current branch.
# Can be run from a worktree — it operates on the main repo's main branch.

set -e

MAIN_BRANCH="main"
DEFAULT_AGENT="claude"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

usage() {
    echo "Usage: $0 [branch-name] [claude|codex]"
    echo ""
    echo "Merges a branch into $MAIN_BRANCH after showing commits and diff output."
    echo "If conflicts occur, the selected AI agent resolves and stages files"
    echo "before this script continues the merge."
    echo ""
    echo "If only one argument is provided and it is 'claude' or 'codex',"
    echo "the current branch is merged using that agent."
    echo ""
    echo "Default agent: $DEFAULT_AGENT"
}

build_agent_command() {
    local agent="$1"
    local project_root="$2"
    local work_dir="$3"
    local output_file="$4"

    PYTHONPATH="$project_root/src${PYTHONPATH:+:$PYTHONPATH}" \
        uv run python - "$agent" "$project_root" "$work_dir" >"$output_file" <<'PY'
import sys
from dataclasses import replace
from pathlib import Path

from gza.config import Config
from gza.providers.claude import ClaudeProvider
from gza.providers.codex import CodexProvider

provider_name = sys.argv[1]
project_root = Path(sys.argv[2])
work_dir = Path(sys.argv[3])

config = Config.load(project_root)
max_steps = config.get_max_steps_for_task("implement", provider_name)
resolved_config = replace(
    config,
    use_docker=False,
    provider=provider_name,
    model=config.get_model_for_task("implement", provider_name) or "",
    reasoning_effort=config.get_reasoning_effort_for_task("implement", provider_name) or "",
    max_steps=max_steps,
    max_turns=max_steps,
)

if provider_name == "claude":
    cmd = ClaudeProvider.build_noninteractive_command(resolved_config, work_dir)
elif provider_name == "codex":
    cmd = CodexProvider.build_noninteractive_command(resolved_config, work_dir)
else:
    raise SystemExit(f"Unsupported agent: {provider_name}")

for arg in cmd:
    sys.stdout.buffer.write(arg.encode("utf-8"))
    sys.stdout.buffer.write(b"\0")
PY
}

LAST_AGENT_INVOCATION_PHASE=""
LAST_AGENT_LAUNCHER_COMMAND=""

command_exists() {
    local command_name="$1"

    if [[ "$command_name" == */* ]]; then
        [[ -x "$command_name" ]]
    else
        command -v "$command_name" >/dev/null 2>&1
    fi
}

invoke_conflict_agent() {
    local agent="$1"
    local project_root="$2"
    local work_dir="$3"
    local prompt="$4"

    LAST_AGENT_INVOCATION_PHASE=""
    LAST_AGENT_LAUNCHER_COMMAND=""

    local prompt_file
    prompt_file=$(mktemp)
    printf '%s\n' "$prompt" > "$prompt_file"

    local cmd_file
    cmd_file=$(mktemp)

    if ! build_agent_command "$agent" "$project_root" "$work_dir" "$cmd_file"; then
        LAST_AGENT_INVOCATION_PHASE="bootstrap"
        rm -f "$prompt_file" "$cmd_file"
        return 1
    fi

    local -a cmd=()
    mapfile -d '' -t cmd < "$cmd_file"
    rm -f "$cmd_file"
    if [[ "${#cmd[@]}" -eq 0 || -z "${cmd[0]}" ]]; then
        echo "Error: Failed to build $agent command for conflict resolution." >&2
        LAST_AGENT_INVOCATION_PHASE="bootstrap"
        rm -f "$prompt_file"
        return 1
    fi

    LAST_AGENT_LAUNCHER_COMMAND="${cmd[0]}"
    if ! command_exists "${cmd[0]}"; then
        echo "Error: Launcher command '${cmd[0]}' for $agent conflict resolution is not available." >&2
        LAST_AGENT_INVOCATION_PHASE="launcher"
        rm -f "$prompt_file"
        return 1
    fi

    local agent_status=0
    if "${cmd[@]}" < "$prompt_file"; then
        :
    else
        agent_status=$?
        LAST_AGENT_INVOCATION_PHASE="agent"
    fi

    rm -f "$prompt_file"
    return "$agent_status"
}

case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
esac

SELECTED_AGENT="$DEFAULT_AGENT"
if [[ $# -eq 0 ]]; then
    BRANCH=$(git branch --show-current)
elif [[ $# -eq 1 ]]; then
    if [[ "$1" == "claude" || "$1" == "codex" ]]; then
        BRANCH=$(git branch --show-current)
        SELECTED_AGENT="$1"
    else
        BRANCH="$1"
    fi
elif [[ $# -eq 2 ]]; then
    BRANCH="$1"
    SELECTED_AGENT="$2"
else
    echo -e "${RED}Error: Too many arguments.${NC}"
    echo ""
    usage
    exit 1
fi

case "$SELECTED_AGENT" in
    claude|codex)
        ;;
    *)
        echo -e "${RED}Error: Invalid agent '$SELECTED_AGENT'. Use 'claude' or 'codex'.${NC}"
        echo ""
        usage
        exit 1
        ;;
esac

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

# Check if there are any changes to merge
COMMIT_COUNT=$(git rev-list --count "$MAIN_BRANCH".."$BRANCH")
if [[ "$COMMIT_COUNT" -eq 0 ]]; then
    echo -e "${YELLOW}No new commits on ${BRANCH} relative to ${MAIN_BRANCH}. Nothing to merge.${NC}"
    exit 0
fi

# Show commit log
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
    if ! git diff --name-only --diff-filter=U | grep -q .; then
        echo ""
        echo -e "${RED}Merge failed before conflict resolution could start.${NC}"
        exit 1
    fi

    if ! command -v "$SELECTED_AGENT" >/dev/null 2>&1; then
        echo ""
        echo -e "${RED}Error: '$SELECTED_AGENT' CLI not found on PATH.${NC}"
        echo "Install it or resolve the merge conflicts manually."
        exit 1
    fi

    echo ""
    echo -e "${YELLOW}=== Merge conflicts detected ===${NC}"
    echo ""
    echo "Conflicted files:"
    git diff --name-only --diff-filter=U
    echo ""

    RESOLUTION_PROMPT="Resolve the current merge conflicts from merging branch '$BRANCH' into '$MAIN_BRANCH'.

For each conflicted file:
1. Read the file and inspect the conflict markers
2. Understand what both sides are changing
3. Resolve the conflict carefully, preserving intended changes from both branches when possible
4. Remove all conflict markers
5. For Python files, run: uv run python -m py_compile <file>
6. Stage each resolved file with: git add <file>

Do not run git merge --continue or git commit. Stop after every conflicted file is resolved and staged so the calling script can continue the merge."

    echo "Invoking $SELECTED_AGENT to resolve conflicts..."

    AGENT_STATUS=0
    if invoke_conflict_agent "$SELECTED_AGENT" "$REPO_ROOT" "$REPO_ROOT" "$RESOLUTION_PROMPT"; then
        :
    else
        AGENT_STATUS=$?
    fi

    if [[ "$LAST_AGENT_INVOCATION_PHASE" == "bootstrap" ]]; then
        echo ""
        echo -e "${RED}Failed to build the $SELECTED_AGENT command for conflict resolution.${NC}"
        echo "Resolve the conflicts manually, then run: git merge --continue"
        exit 1
    fi

    if [[ "$LAST_AGENT_INVOCATION_PHASE" == "launcher" ]]; then
        echo ""
        echo -e "${RED}Failed to launch $SELECTED_AGENT conflict resolution because '${LAST_AGENT_LAUNCHER_COMMAND}' is unavailable.${NC}"
        echo "Install the missing launcher or resolve the conflicts manually, then run: git merge --continue"
        exit 1
    fi

    if [[ "$AGENT_STATUS" -ne 0 ]]; then
        echo ""
        echo -e "${RED}$SELECTED_AGENT exited with status $AGENT_STATUS while resolving conflicts.${NC}"
        echo "Resolve the conflicts manually, then run: git merge --continue"
        exit 1
    fi

    if git diff --name-only --diff-filter=U | grep -q .; then
        echo ""
        echo -e "${RED}Conflicts remain after $SELECTED_AGENT finished.${NC}"
        echo "Resolve the remaining conflicts, stage the files, and run: git merge --continue"
        exit 1
    fi

    if GIT_EDITOR=true git merge --continue; then
        echo ""
        echo -e "${GREEN}Merged $BRANCH into $MAIN_BRANCH.${NC}"
    else
        echo ""
        echo -e "${RED}git merge --continue failed after conflict resolution.${NC}"
        echo "Review the index and run: git merge --continue"
        exit 1
    fi
fi

# Switch back if we were on a different branch
if [[ -n "$ORIGINAL_BRANCH" && "$ORIGINAL_BRANCH" != "$MAIN_BRANCH" ]]; then
    git checkout "$ORIGINAL_BRANCH"
fi

echo ""
echo -e "${YELLOW}To push:${NC}  git push origin $MAIN_BRANCH"
