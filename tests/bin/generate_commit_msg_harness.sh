#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT_DIR/bin/generate-commit-msg.sh"

ORIGINAL_PATH="$PATH"
CASE_DIRS=()
LOCAL_CONFIG_PATH="$ROOT_DIR/gza.local.yaml"
LOCAL_CONFIG_BACKUP=""
if [[ -f "$LOCAL_CONFIG_PATH" ]]; then
    LOCAL_CONFIG_BACKUP="$(mktemp)"
    cp "$LOCAL_CONFIG_PATH" "$LOCAL_CONFIG_BACKUP"
fi

cleanup() {
    local dir
    for dir in "${CASE_DIRS[@]:-}"; do
        rm -rf "$dir"
    done
    if [[ -n "$LOCAL_CONFIG_BACKUP" && -f "$LOCAL_CONFIG_BACKUP" ]]; then
        mv "$LOCAL_CONFIG_BACKUP" "$LOCAL_CONFIG_PATH"
    else
        rm -f "$LOCAL_CONFIG_PATH"
    fi
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
    if ! grep -Fq -- "$needle" "$path"; then
        fail "$description
Expected to find: $needle
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
    export FAKE_GIT_LOG="$CASE_DIR/git.log"
    export FAKE_TIMEOUT_LOG="$CASE_DIR/timeout.log"
    export FAKE_CODEX_LOG="$CASE_DIR/codex.log"
    export FAKE_CLAUDE_LOG="$CASE_DIR/claude.log"
    export FAKE_STAGED_DIFF=$'diff --git a/demo.txt b/demo.txt\n+new line\n'
    export FAKE_RECENT_COMMITS=$'abc123 prior commit\n'
    export FAKE_HAS_STAGED_CHANGES=1
    mkdir -p "$CASE_DIR/bin"
    : >"$FAKE_GIT_LOG"
    : >"$FAKE_TIMEOUT_LOG"
    : >"$FAKE_CODEX_LOG"
    : >"$FAKE_CLAUDE_LOG"
    cat >"$LOCAL_CONFIG_PATH" <<'EOF'
timeout_minutes: 7
providers:
  codex:
    model: gpt-5.4
    reasoning_effort: high
EOF

    cat >"$CASE_DIR/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'git %s\n' "$*" >>"$FAKE_GIT_LOG"

if [[ "${1:-}" == "diff" && "${2:-}" == "--staged" && "${3:-}" == "--quiet" ]]; then
    if [[ "${FAKE_HAS_STAGED_CHANGES:-1}" == "1" ]]; then
        exit 1
    fi
    exit 0
fi

if [[ "${1:-}" == "diff" && "${2:-}" == "--staged" ]]; then
    printf '%s' "$FAKE_STAGED_DIFF"
    exit 0
fi

if [[ "${1:-}" == "log" && "${2:-}" == "--oneline" && "${3:-}" == "-5" ]]; then
    printf '%s' "$FAKE_RECENT_COMMITS"
    exit 0
fi

echo "Unsupported fake git invocation: git $*" >&2
exit 2
EOF
    chmod +x "$CASE_DIR/bin/git"

    cat >"$CASE_DIR/bin/timeout" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'timeout %s\n' "$*" >>"$FAKE_TIMEOUT_LOG"
duration="$1"
shift
"$@"
EOF
    chmod +x "$CASE_DIR/bin/timeout"

    cat >"$CASE_DIR/bin/codex" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'codex %s\n' "$*" >>"$FAKE_CODEX_LOG"

output_file=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    if [[ "${args[$i]}" == "--output-last-message" ]]; then
        output_file="${args[$((i + 1))]}"
        break
    fi
done

cat >/dev/null
printf 'ignored stdout\n'

if [[ -z "$output_file" ]]; then
    echo "codex did not receive --output-last-message" >&2
    exit 2
fi

printf '%s\n' "${FAKE_CODEX_RESPONSE:-Generated with Codex}" >"$output_file"
EOF
    chmod +x "$CASE_DIR/bin/codex"

    cat >"$CASE_DIR/bin/claude" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf 'claude %s\n' "$*" >>"$FAKE_CLAUDE_LOG"
cat >/dev/null
printf '%s\n' "${FAKE_CLAUDE_RESPONSE:-Generated with Claude}"
EOF
    chmod +x "$CASE_DIR/bin/claude"
}

run_script() {
    RUN_OUTPUT_FILE="$CASE_DIR/output.txt"
    set +e
    bash "$SCRIPT" "$@" >"$RUN_OUTPUT_FILE" 2>&1
    RUN_STATUS=$?
    set -e
    RUN_OUTPUT="$(cat "$RUN_OUTPUT_FILE")"
}

test_default_provider_uses_codex() {
    setup_case
    export FAKE_CODEX_RESPONSE="Codex summary"

    run_script

    [[ "$RUN_STATUS" -eq 0 ]] || fail "default codex flow should succeed, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Generating commit message..." "script should announce generation"
    assert_contains "$RUN_OUTPUT" "Codex summary" "default provider should print Codex output"
    assert_not_contains "$RUN_OUTPUT" "ignored stdout" "script should suppress Codex stdout noise"
    assert_file_contains "$FAKE_TIMEOUT_LOG" "timeout 7m codex" "codex flow should use the shared timeout-wrapped launcher"
    assert_file_contains "$FAKE_CODEX_LOG" "check_for_update_on_startup=false" "codex flow should disable startup update checks in non-interactive mode"
    assert_file_contains "$FAKE_CODEX_LOG" "exec --json" "codex flow should use the shared headless exec mode"
    assert_file_contains "$FAKE_CODEX_LOG" "--dangerously-bypass-approvals-and-sandbox" "codex flow should include headless safety bypass flags"
    assert_file_contains "$FAKE_CODEX_LOG" "--skip-git-repo-check" "codex flow should skip git repo checks in detached contexts"
    assert_file_contains "$FAKE_CODEX_LOG" "-C /workspace" "default provider should invoke codex from the current workspace"
    assert_file_contains "$FAKE_CODEX_LOG" "-m gpt-5.4" "codex flow should honor config-driven model overrides"
    assert_file_contains "$FAKE_CODEX_LOG" "model_reasoning_effort=high" "codex flow should honor config-driven reasoning overrides"
    assert_file_contains "$FAKE_CODEX_LOG" "--output-last-message" "codex flow should capture the final message explicitly"
}

test_explicit_claude_provider_uses_legacy_path() {
    setup_case
    export FAKE_CLAUDE_RESPONSE="Claude summary"

    run_script --claude

    [[ "$RUN_STATUS" -eq 0 ]] || fail "explicit claude flow should succeed, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Claude summary" "claude provider should print Claude output"
    assert_file_contains "$FAKE_CLAUDE_LOG" "--model haiku --print" "claude flow should preserve the existing Claude invocation"
}

test_explicit_codex_provider_works() {
    setup_case
    export FAKE_CODEX_RESPONSE="Explicit Codex summary"

    run_script --codex

    [[ "$RUN_STATUS" -eq 0 ]] || fail "explicit codex flow should succeed, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Explicit Codex summary" "explicit codex provider should print Codex output"
}

test_conflicting_provider_flags_fail() {
    setup_case

    run_script --codex --claude

    [[ "$RUN_STATUS" -eq 2 ]] || fail "conflicting providers should exit 2, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Error: Choose only one provider option." "conflicting providers should report a clear error"
}

test_no_staged_changes_preserved() {
    setup_case
    export FAKE_HAS_STAGED_CHANGES=0

    run_script

    [[ "$RUN_STATUS" -eq 1 ]] || fail "no staged changes should exit 1, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "No staged changes to commit." "no staged changes message should be preserved"
}

test_help_mentions_codex_default() {
    setup_case

    run_script --help

    [[ "$RUN_STATUS" -eq 0 ]] || fail "help should exit 0, got $RUN_STATUS"
    assert_contains "$RUN_OUTPUT" "Usage:" "help should include usage text"
    assert_contains "$RUN_OUTPUT" "--codex   Use Codex (default)" "help should document the new default provider"
    assert_contains "$RUN_OUTPUT" "--claude  Use Claude" "help should document the Claude override"
}

test_default_provider_uses_codex
test_explicit_claude_provider_uses_legacy_path
test_explicit_codex_provider_works
test_conflicting_provider_flags_fail
test_no_staged_changes_preserved
test_help_mentions_codex_default

echo "generate_commit_msg_harness: ok"
