#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MERGE_SCRIPT="$ROOT_DIR/bin/merge-to-main.sh"

ORIGINAL_PATH="${PATH}"
CASE_DIRS=()

cleanup() {
    local dir
    for dir in "${CASE_DIRS[@]:-}"; do
        rm -rf "$dir"
    done
}
trap cleanup EXIT

fail() {
    echo "FAIL: $1" >&2
    exit 1
}

assert_contains() {
    local haystack="$1"
    local needle="$2"
    local description="$3"
    if [[ "$haystack" != *"$needle"* ]]; then
        fail "$description
Expected to find: $needle
Actual output:
$haystack"
    fi
}

assert_not_contains() {
    local haystack="$1"
    local needle="$2"
    local description="$3"
    if [[ "$haystack" == *"$needle"* ]]; then
        fail "$description
Did not expect to find: $needle
Actual output:
$haystack"
    fi
}

assert_file_contains() {
    local path="$1"
    local needle="$2"
    local description="$3"
    if ! grep -Fq "$needle" "$path"; then
        fail "$description
Expected to find: $needle
In file: $path
Actual content:
$(cat "$path")"
    fi
}

assert_file_not_contains() {
    local path="$1"
    local needle="$2"
    local description="$3"
    if grep -Fq "$needle" "$path"; then
        fail "$description
Did not expect to find: $needle
In file: $path
Actual content:
$(cat "$path")"
    fi
}

setup_case() {
    CASE_DIR="$(mktemp -d)"
    CASE_DIRS+=("$CASE_DIR")
    export CASE_DIR
    export PATH="$CASE_DIR/bin:$ORIGINAL_PATH"
    export FAKE_REPO_ROOT="$CASE_DIR/repo"
    export FAKE_GIT_LOG="$CASE_DIR/git.log"
    export FAKE_UV_LOG="$CASE_DIR/uv.log"
    export FAKE_AGENT_LOG="$CASE_DIR/agent.log"
    export FAKE_UNMERGED_STATE="$CASE_DIR/unmerged.txt"
    mkdir -p "$CASE_DIR/bin" "$FAKE_REPO_ROOT/.git"
    : >"$FAKE_GIT_LOG"
    : >"$FAKE_UV_LOG"
    : >"$FAKE_AGENT_LOG"
    rm -f "$FAKE_UNMERGED_STATE"
    unset FAKE_GIT_UNMERGED_INITIAL
    unset FAKE_GIT_MERGE_CONTINUE_STATUS

    cat >"$CASE_DIR/bin/git" <<'EOF'
#!/bin/bash
set -euo pipefail

printf 'git %s\n' "$*" >>"$FAKE_GIT_LOG"

cmd="${1:-}"
shift || true

case "$cmd" in
    branch)
        if [[ "${1:-}" == "--show-current" ]]; then
            printf '%s\n' "${FAKE_GIT_CURRENT_BRANCH:-feature/current}"
            exit 0
        fi
        ;;
    rev-parse)
        if [[ "${1:-}" == "--verify" ]]; then
            branch="${2:-}"
            if [[ -n "${FAKE_GIT_VALID_BRANCHES:-}" ]]; then
                case " ${FAKE_GIT_VALID_BRANCHES} " in
                    *" $branch "*) exit 0 ;;
                    *) exit 1 ;;
                esac
            fi
            exit 0
        fi
        if [[ "${1:-}" == "--path-format=absolute" && "${2:-}" == "--git-common-dir" ]]; then
            printf '%s\n' "$FAKE_REPO_ROOT/.git"
            exit 0
        fi
        ;;
    rev-list)
        if [[ "${1:-}" == "--count" ]]; then
            printf '%s\n' "${FAKE_GIT_COMMIT_COUNT:-1}"
            exit 0
        fi
        ;;
    log)
        printf 'abc123 demo commit\n'
        exit 0
        ;;
    diff)
        if [[ "${1:-}" == "--name-only" && "${2:-}" == "--diff-filter=U" ]]; then
            if [[ -f "$FAKE_UNMERGED_STATE" ]]; then
                cat "$FAKE_UNMERGED_STATE"
            fi
            exit 0
        fi
        if [[ "${*: -1}" == "--stat" ]]; then
            printf ' file.txt | 1 +\n'
            exit 0
        fi
        printf 'diff --git a/file.txt b/file.txt\n'
        exit 0
        ;;
    checkout)
        printf 'checkout %s\n' "${1:-}" >>"$FAKE_GIT_LOG"
        exit 0
        ;;
    merge)
        if [[ "${1:-}" == "--no-ff" ]]; then
            branch="${2:-}"
            printf 'merge-start %s\n' "$branch" >>"$FAKE_GIT_LOG"
            if [[ "${FAKE_GIT_MERGE_STATUS:-0}" -ne 0 ]]; then
                printf '%b' "${FAKE_GIT_UNMERGED_INITIAL:-conflicted.txt\n}" >"$FAKE_UNMERGED_STATE"
                exit "${FAKE_GIT_MERGE_STATUS}"
            fi
            exit 0
        fi
        if [[ "${1:-}" == "--continue" ]]; then
            printf 'merge-continue\n' >>"$FAKE_GIT_LOG"
            exit "${FAKE_GIT_MERGE_CONTINUE_STATUS:-0}"
        fi
        ;;
esac

echo "Unsupported fake git invocation: git $cmd $*" >&2
exit 2
EOF
    chmod +x "$CASE_DIR/bin/git"

    cat >"$CASE_DIR/bin/uv" <<'EOF'
#!/bin/bash
set -euo pipefail

printf 'uv %s\n' "$*" >>"$FAKE_UV_LOG"

if [[ "${1:-}" != "run" || "${2:-}" != "python" ]]; then
    echo "Unsupported fake uv invocation: uv $*" >&2
    exit 2
fi

case "${FAKE_UV_MODE:-claude_cmd}" in
    bootstrap_fail)
        echo "builder exploded" >&2
        exit 42
        ;;
    missing_launcher_cmd)
        printf 'missing-launcher\0--wrapper-flag\0claude\0--headless\0'
        ;;
    claude_cmd)
        printf 'claude\0--headless\0'
        ;;
    codex_cmd)
        printf 'codex\0exec\0'
        ;;
    empty_cmd)
        exit 0
        ;;
    *)
        echo "Unsupported FAKE_UV_MODE=${FAKE_UV_MODE}" >&2
        exit 2
        ;;
esac
EOF
    chmod +x "$CASE_DIR/bin/uv"

    cat >"$CASE_DIR/bin/claude" <<'EOF'
#!/bin/bash
set -euo pipefail

printf 'claude %s\n' "$*" >>"$FAKE_AGENT_LOG"
cat >/dev/null
if [[ "${FAKE_AGENT_CLEAR_CONFLICTS:-0}" == "1" ]]; then
    : >"$FAKE_UNMERGED_STATE"
fi
exit "${FAKE_AGENT_STATUS:-0}"
EOF
    chmod +x "$CASE_DIR/bin/claude"

    cat >"$CASE_DIR/bin/codex" <<'EOF'
#!/bin/bash
set -euo pipefail

printf 'codex %s\n' "$*" >>"$FAKE_AGENT_LOG"
cat >/dev/null
if [[ "${FAKE_AGENT_CLEAR_CONFLICTS:-0}" == "1" ]]; then
    : >"$FAKE_UNMERGED_STATE"
fi
exit "${FAKE_AGENT_STATUS:-0}"
EOF
    chmod +x "$CASE_DIR/bin/codex"
}

run_merge() {
    RUN_OUTPUT_FILE="$CASE_DIR/output.txt"
    set +e
    printf 'y\n' | bash "$MERGE_SCRIPT" "$@" >"$RUN_OUTPUT_FILE" 2>&1
    RUN_STATUS=$?
    set -e
    RUN_OUTPUT="$(cat "$RUN_OUTPUT_FILE")"
}

test_bootstrap_failure_reports_builder_error() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/bootstrap"
    export FAKE_GIT_VALID_BRANCHES="feature/bootstrap"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="bootstrap_fail"
    export FAKE_AGENT_STATUS=0
    export FAKE_AGENT_CLEAR_CONFLICTS=0

    run_merge claude

    [[ "$RUN_STATUS" -eq 1 ]] || fail "bootstrap failure should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "builder exploded" "bootstrap stderr should be preserved"
    assert_contains "$RUN_OUTPUT" "Failed to build the claude command for conflict resolution." "bootstrap failure should be reported distinctly"
    assert_not_contains "$RUN_OUTPUT" "claude exited with status 127" "bootstrap failure must not be mislabeled as agent exit 127"
    assert_file_not_contains "$FAKE_GIT_LOG" "merge-continue" "bootstrap failure must not continue the merge"
    [[ ! -s "$FAKE_AGENT_LOG" ]] || fail "bootstrap failure should not launch the agent"
}

test_missing_launcher_reports_distinct_error() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/launcher"
    export FAKE_GIT_VALID_BRANCHES="feature/launcher"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="missing_launcher_cmd"
    export FAKE_AGENT_STATUS=0
    export FAKE_AGENT_CLEAR_CONFLICTS=0

    run_merge claude

    [[ "$RUN_STATUS" -eq 1 ]] || fail "missing launcher should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Launcher command 'missing-launcher' for claude conflict resolution is not available." "missing launcher should be identified before agent launch"
    assert_contains "$RUN_OUTPUT" "Failed to launch claude conflict resolution because 'missing-launcher' is unavailable." "launcher failure should be reported distinctly"
    assert_not_contains "$RUN_OUTPUT" "claude exited with status 127" "missing launcher must not be mislabeled as an agent exit"
    assert_file_not_contains "$FAKE_GIT_LOG" "merge-continue" "missing launcher must not continue the merge"
    [[ ! -s "$FAKE_AGENT_LOG" ]] || fail "missing launcher should not launch the agent"
}

test_single_argument_agent_uses_current_branch() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/current"
    export FAKE_GIT_VALID_BRANCHES="feature/current"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="codex_cmd"
    export FAKE_AGENT_STATUS=0
    export FAKE_AGENT_CLEAR_CONFLICTS=1

    run_merge codex

    [[ "$RUN_STATUS" -eq 0 ]] || fail "single-argument codex flow should succeed, got $RUN_STATUS"
    assert_file_contains "$FAKE_GIT_LOG" "merge-start feature/current" "single-argument agent form should merge the current branch"
    assert_file_contains "$FAKE_AGENT_LOG" "codex exec" "single-argument agent form should invoke codex"
    assert_file_contains "$FAKE_GIT_LOG" "merge-continue" "resolved conflicts should continue the merge"
}

test_two_argument_branch_and_agent_parse_correctly() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="worktree/topic"
    export FAKE_GIT_VALID_BRANCHES="topic-branch"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="claude_cmd"
    export FAKE_AGENT_STATUS=0
    export FAKE_AGENT_CLEAR_CONFLICTS=1

    run_merge topic-branch claude

    [[ "$RUN_STATUS" -eq 0 ]] || fail "two-argument branch+agent flow should succeed, got $RUN_STATUS"
    assert_file_contains "$FAKE_GIT_LOG" "merge-start topic-branch" "two-argument form should merge the explicit branch"
    assert_file_contains "$FAKE_AGENT_LOG" "claude --headless" "two-argument form should invoke claude"
    assert_file_contains "$FAKE_GIT_LOG" "merge-continue" "resolved conflicts should continue the merge"
}

test_remaining_conflicts_emit_recovery_guidance() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/conflicts"
    export FAKE_GIT_VALID_BRANCHES="feature/conflicts"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="codex_cmd"
    export FAKE_AGENT_STATUS=0
    export FAKE_AGENT_CLEAR_CONFLICTS=0
    export FAKE_GIT_UNMERGED_INITIAL=$'left.py\nright.py\n'

    run_merge codex

    [[ "$RUN_STATUS" -eq 1 ]] || fail "remaining conflicts should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Conflicts remain after codex finished." "remaining conflicts should surface clear guidance"
    assert_contains "$RUN_OUTPUT" "Resolve the remaining conflicts, stage the files, and run: git merge --continue" "remaining conflicts should tell the operator how to recover"
    assert_file_not_contains "$FAKE_GIT_LOG" "merge-continue" "merge should not continue while conflicts remain"
}

test_agent_failure_surfaces_exit_status() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/failure"
    export FAKE_GIT_VALID_BRANCHES="feature/failure"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_UV_MODE="claude_cmd"
    export FAKE_AGENT_STATUS=23
    export FAKE_AGENT_CLEAR_CONFLICTS=0

    run_merge claude

    [[ "$RUN_STATUS" -eq 1 ]] || fail "agent failure should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "claude exited with status 23 while resolving conflicts." "real agent failures should preserve the real exit status"
    assert_contains "$RUN_OUTPUT" "Resolve the conflicts manually, then run: git merge --continue" "agent failure should print recovery guidance"
    assert_file_not_contains "$FAKE_GIT_LOG" "merge-continue" "failed agents must not continue the merge"
}

test_non_conflict_merge_failure_does_not_require_agent_cli() {
    setup_case
    export FAKE_GIT_CURRENT_BRANCH="feature/preconflict-failure"
    export FAKE_GIT_VALID_BRANCHES="feature/preconflict-failure"
    export FAKE_GIT_MERGE_STATUS=1
    export FAKE_GIT_UNMERGED_INITIAL=$'\n'
    export PATH="$CASE_DIR/bin-no-agent:$ORIGINAL_PATH"
    mkdir -p "$CASE_DIR/bin-no-agent"
    ln -s "$CASE_DIR/bin/git" "$CASE_DIR/bin-no-agent/git"
    ln -s "$CASE_DIR/bin/uv" "$CASE_DIR/bin-no-agent/uv"

    run_merge codex

    [[ "$RUN_STATUS" -eq 1 ]] || fail "pre-conflict merge failure should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Merge failed before conflict resolution could start." "pre-conflict merge failure should report the generic merge failure"
    assert_not_contains "$RUN_OUTPUT" "CLI not found on PATH" "pre-conflict merge failure must not be mislabeled as a missing agent"
    assert_file_not_contains "$FAKE_GIT_LOG" "merge-continue" "pre-conflict merge failure must not continue the merge"
    [[ ! -s "$FAKE_AGENT_LOG" ]] || fail "pre-conflict merge failure should not launch an agent"
}

test_bootstrap_failure_reports_builder_error
test_missing_launcher_reports_distinct_error
test_single_argument_agent_uses_current_branch
test_two_argument_branch_and_agent_parse_correctly
test_remaining_conflicts_emit_recovery_guidance
test_agent_failure_surfaces_exit_status
test_non_conflict_merge_failure_does_not_require_agent_cli

echo "merge_to_main_harness: ok"
