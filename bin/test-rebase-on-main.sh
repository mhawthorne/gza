#!/bin/bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT_UNDER_TEST="$REPO_ROOT/bin/rebase-on-main.sh"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rebase-on-main-test.XXXXXX")

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local actual="$1"
    local expected="$2"
    local message="$3"

    if [[ "$actual" != "$expected" ]]; then
        fail "$message (expected '$expected', got '$actual')"
    fi
}

assert_file_contains() {
    local file="$1"
    local expected="$2"
    local message="$3"

    if ! grep -Fq -- "$expected" "$file"; then
        fail "$message"
    fi
}

create_mock_tools() {
    local case_dir="$1"
    local mock_bin="$case_dir/mock-bin"

    mkdir -p "$mock_bin"

    cat <<'EOF' > "$mock_bin/git"
#!/bin/bash
set -euo pipefail

STATE_DIR="${FAKE_GIT_STATE_DIR:?}"
printf '%s\n' "$*" >> "$STATE_DIR/git.log"

while [[ "${1:-}" == "-c" ]]; do
    shift 2
done

cmd="${1:-}"
if [[ -z "$cmd" ]]; then
    echo "missing git subcommand" >&2
    exit 1
fi
shift || true

case "$cmd" in
    branch)
        if [[ "${1:-}" == "--show-current" ]]; then
            cat "$STATE_DIR/current_branch"
            exit 0
        fi
        ;;
    diff)
        has_cached=0
        has_quiet=0
        has_name_only=0
        diff_filter=""
        for arg in "$@"; do
            case "$arg" in
                --cached)
                    has_cached=1
                    ;;
                --quiet)
                    has_quiet=1
                    ;;
                --name-only)
                    has_name_only=1
                    ;;
                --diff-filter=*)
                    diff_filter="${arg#--diff-filter=}"
                    ;;
            esac
        done

        if [[ "$has_quiet" -eq 1 ]]; then
            if [[ "$has_cached" -eq 1 ]]; then
                [[ -f "$STATE_DIR/cached_dirty" ]] && exit 1
                exit 0
            fi
            [[ -f "$STATE_DIR/dirty" ]] && exit 1
            exit 0
        fi

        if [[ "$has_name_only" -eq 1 && "$diff_filter" == "U" ]]; then
            [[ -f "$STATE_DIR/conflicts" ]] && cat "$STATE_DIR/conflicts"
            exit 0
        fi

        if [[ "$has_name_only" -eq 1 && "$has_cached" -eq 1 ]]; then
            [[ -f "$STATE_DIR/cached_names" ]] && cat "$STATE_DIR/cached_names"
            exit 0
        fi

        if [[ "$has_name_only" -eq 1 ]]; then
            [[ -f "$STATE_DIR/dirty_names" ]] && cat "$STATE_DIR/dirty_names"
            exit 0
        fi
        ;;
    rev-parse)
        if [[ "${1:-}" == "HEAD" ]]; then
            cat "$STATE_DIR/head"
            exit 0
        fi
        if [[ "${1:-}" == "--git-path" ]]; then
            case "${2:-}" in
                rebase-merge)
                    printf '%s\n' "$STATE_DIR/rebase-merge"
                    exit 0
                    ;;
                rebase-apply)
                    printf '%s\n' "$STATE_DIR/rebase-apply"
                    exit 0
                    ;;
            esac
        fi
        ;;
    fetch)
        exit 0
        ;;
    rebase)
        target="${1:-}"
        scenario=$(cat "$STATE_DIR/scenario")

        if [[ "$target" == "--continue" ]]; then
            continue_index=$(cat "$STATE_DIR/continue_index")
            if [[ "$scenario" != "conflict_loop" ]]; then
                echo "unexpected rebase --continue" >&2
                exit 1
            fi
            if [[ "$continue_index" == "0" ]]; then
                printf '%s\n' "src/second.py" > "$STATE_DIR/conflicts"
                printf '1' > "$STATE_DIR/continue_index"
                exit 1
            fi
            rm -f "$STATE_DIR/conflicts"
            rm -rf "$STATE_DIR/rebase-merge" "$STATE_DIR/rebase-apply"
            printf '%s\n' "head-after" > "$STATE_DIR/head"
            printf '2' > "$STATE_DIR/continue_index"
            exit 0
        fi

        if [[ "$scenario" == "no_conflict" ]]; then
            printf '%s\n' "head-after" > "$STATE_DIR/head"
            exit 0
        fi

        mkdir -p "$STATE_DIR/rebase-merge"
        printf '%s\n' "src/first.py" > "$STATE_DIR/conflicts"
        printf '0' > "$STATE_DIR/continue_index"
        exit 1
        ;;
    push)
        push_count=$(cat "$STATE_DIR/push_count")
        printf '%s' "$((push_count + 1))" > "$STATE_DIR/push_count"
        exit 0
        ;;
esac

echo "unsupported git invocation: $cmd $*" >&2
exit 1
EOF

    cat <<'EOF' > "$mock_bin/claude"
#!/bin/bash
set -euo pipefail

STATE_DIR="${FAKE_GIT_STATE_DIR:?}"
count=$(cat "$STATE_DIR/claude_count")
count=$((count + 1))
printf '%s' "$count" > "$STATE_DIR/claude_count"
printf '%s\n' "$*" > "$STATE_DIR/claude_args_${count}.txt"
cat > "$STATE_DIR/claude_prompt_${count}.txt"

case " $* " in
    *" -p - "*) ;;
    *)
        echo "claude must run in one-shot -p mode" >&2
        exit 1
        ;;
esac

case " $* " in
    *" --resume "*) 
        echo "claude should not resume an interactive session" >&2
        exit 1
        ;;
esac

rm -f "$STATE_DIR/conflicts"
EOF

    cat <<'EOF' > "$mock_bin/codex"
#!/bin/bash
set -euo pipefail

STATE_DIR="${FAKE_GIT_STATE_DIR:?}"
count=$(cat "$STATE_DIR/codex_count")
count=$((count + 1))
printf '%s' "$count" > "$STATE_DIR/codex_count"
printf '%s\n' "$*" > "$STATE_DIR/codex_args_${count}.txt"
cat > "$STATE_DIR/codex_prompt_${count}.txt"

case " $* " in
    *" exec "*) ;;
    *)
        echo "codex must run via codex exec" >&2
        exit 1
        ;;
esac

case " $* " in
    *" --dangerously-bypass-approvals-and-sandbox "*) ;;
    *)
        echo "codex must run in non-interactive auto mode" >&2
        exit 1
        ;;
esac

rm -f "$STATE_DIR/conflicts"
EOF

    chmod +x "$mock_bin/git" "$mock_bin/claude" "$mock_bin/codex"
}

initialize_state() {
    local state_dir="$1"
    local scenario="$2"

    mkdir -p "$state_dir"
    printf '%s\n' "feature/test-branch" > "$state_dir/current_branch"
    printf '%s\n' "head-before" > "$state_dir/head"
    printf '%s\n' "$scenario" > "$state_dir/scenario"
    printf '0' > "$state_dir/push_count"
    printf '0' > "$state_dir/continue_index"
    printf '0' > "$state_dir/claude_count"
    printf '0' > "$state_dir/codex_count"
    : > "$state_dir/git.log"
}

run_script_case() {
    local scenario="$1"
    local agent="$2"
    local case_dir="$TMP_DIR/${scenario}-${agent:-default}"
    local output_file="$case_dir/output.txt"

    mkdir -p "$case_dir"
    initialize_state "$case_dir/state" "$scenario"
    create_mock_tools "$case_dir"

    if [[ -n "$agent" ]]; then
        printf '\nY\n' | env \
            PATH="$case_dir/mock-bin:$PATH" \
            FAKE_GIT_STATE_DIR="$case_dir/state" \
            bash "$SCRIPT_UNDER_TEST" "$agent" > "$output_file" 2>&1
    else
        printf '\nY\n' | env \
            PATH="$case_dir/mock-bin:$PATH" \
            FAKE_GIT_STATE_DIR="$case_dir/state" \
            bash "$SCRIPT_UNDER_TEST" > "$output_file" 2>&1
    fi

    printf '%s\n' "$case_dir"
}

test_no_conflict_path() {
    local case_dir
    case_dir=$(run_script_case "no_conflict" "")

    assert_eq "$(cat "$case_dir/state/push_count")" "1" "no-conflict path should offer and run push"
    assert_eq "$(cat "$case_dir/state/claude_count")" "0" "no-conflict path should not invoke claude"
    assert_file_contains "$case_dir/output.txt" "Rebase completed successfully!" "no-conflict path should report success"
}

test_conflict_loop_with_claude() {
    local case_dir
    case_dir=$(run_script_case "conflict_loop" "claude")

    assert_eq "$(cat "$case_dir/state/claude_count")" "2" "claude should resolve each conflict round"
    assert_eq "$(cat "$case_dir/state/push_count")" "1" "claude conflict flow should return to scripted push"
    assert_eq "$(cat "$case_dir/state/continue_index")" "2" "script should own rebase --continue across rounds"
    assert_file_contains "$case_dir/state/git.log" "-c core.editor=true rebase --continue" "script should continue the rebase itself"
    assert_file_contains "$case_dir/state/claude_prompt_1.txt" "Do not run git rebase --continue." "claude prompt should keep rebase continuation in the shell script"
    assert_file_contains "$case_dir/output.txt" "Continuing rebase..." "conflict flow should report scripted continue step"
}

test_conflict_loop_with_codex() {
    local case_dir
    case_dir=$(run_script_case "conflict_loop" "codex")

    assert_eq "$(cat "$case_dir/state/codex_count")" "2" "codex should resolve each conflict round"
    assert_eq "$(cat "$case_dir/state/push_count")" "1" "codex conflict flow should return to scripted push"
    assert_eq "$(cat "$case_dir/state/continue_index")" "2" "script should continue rebases after codex exits"
    assert_file_contains "$case_dir/state/codex_prompt_1.txt" "Do not run git push." "codex prompt should keep push in the shell script"
    assert_file_contains "$case_dir/state/codex_args_1.txt" "exec" "codex should run in one-shot exec mode"
}

test_no_conflict_path
test_conflict_loop_with_claude
test_conflict_loop_with_codex

echo "bin/rebase-on-main.sh targeted checks passed"
