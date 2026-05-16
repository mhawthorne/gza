#!/usr/bin/env bash
# Generate a commit message for staged changes using Codex or Claude.
# Usage: ./bin/generate-commit-msg.sh [--codex|--claude]

set -euo pipefail

DEFAULT_PROVIDER="claude"
CLAUDE_MODEL="haiku"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
    cat <<EOF
Usage: $0 [--codex|--claude]

Generate a concise git commit message for the currently staged changes.

Options:
  --codex   Use Codex (default)
  --claude  Use Claude
  -h, --help  Show this help text
EOF
}

SELECTED_PROVIDER="$DEFAULT_PROVIDER"
PROVIDER_FLAG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --codex)
            if [[ -n "$PROVIDER_FLAG" && "$PROVIDER_FLAG" != "codex" ]]; then
                echo "Error: Choose only one provider option." >&2
                usage >&2
                exit 2
            fi
            SELECTED_PROVIDER="codex"
            PROVIDER_FLAG="codex"
            shift
            ;;
        --claude)
            if [[ -n "$PROVIDER_FLAG" && "$PROVIDER_FLAG" != "claude" ]]; then
                echo "Error: Choose only one provider option." >&2
                usage >&2
                exit 2
            fi
            SELECTED_PROVIDER="claude"
            PROVIDER_FLAG="claude"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if git diff --staged --quiet; then
    echo "No staged changes to commit."
    exit 1
fi

STAGED_DIFF=$(git diff --staged)
RECENT_COMMITS=$(git log --oneline -5 2>/dev/null || echo "No previous commits")

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

run_claude() {
    if ! command -v claude >/dev/null 2>&1; then
        echo "Error: 'claude' CLI not found. Install it with: npm install -g @anthropic-ai/claude-code" >&2
        exit 1
    fi

    printf '%s\n' "$PROMPT" | claude --model "$CLAUDE_MODEL" --print
}

LAST_CODEX_INVOCATION_PHASE=""

command_exists() {
    local command_name="$1"

    if [[ "$command_name" == */* ]]; then
        [[ -x "$command_name" ]]
    else
        command -v "$command_name" >/dev/null 2>&1
    fi
}

build_codex_command() {
    local project_root="$1"
    local work_dir="$2"
    local output_file="$3"

    PYTHONPATH="$project_root/src${PYTHONPATH:+:$PYTHONPATH}" \
        uv run python - "$project_root" "$work_dir" "$output_file" <<'PY'
import sys
from dataclasses import replace
from pathlib import Path

from gza.config import Config
from gza.providers.codex import CodexProvider

project_root = Path(sys.argv[1])
work_dir = Path(sys.argv[2])
output_file = Path(sys.argv[3])

config = Config.load(project_root)
provider_name = "codex"
task_type = "implement"
max_steps = config.get_max_steps_for_task(task_type, provider_name)
resolved_config = replace(
    config,
    use_docker=False,
    provider=provider_name,
    model=config.get_model_for_task(task_type, provider_name) or "",
    reasoning_effort=config.get_reasoning_effort_for_task(task_type, provider_name) or "",
    max_steps=max_steps,
    max_turns=max_steps,
)

cmd = CodexProvider.build_noninteractive_command(resolved_config, work_dir)

for idx, arg in enumerate(cmd):
    if arg == "-":
        cmd[idx:idx] = ["--output-last-message", str(output_file)]
        break
else:
    raise SystemExit("Codex non-interactive command is missing stdin placeholder '-'")

for arg in cmd:
    sys.stdout.buffer.write(arg.encode("utf-8"))
    sys.stdout.buffer.write(b"\0")
PY
}

run_codex() {
    local output_file
    local cmd_file
    output_file=$(mktemp)
    cmd_file=$(mktemp)
    trap 'rm -f "$output_file" "$cmd_file"' RETURN

    local -a cmd=()
    LAST_CODEX_INVOCATION_PHASE=""
    if ! build_codex_command "$PROJECT_ROOT" "$(pwd)" "$output_file" >"$cmd_file"; then
        LAST_CODEX_INVOCATION_PHASE="bootstrap"
    elif ! mapfile -d '' -t cmd <"$cmd_file"; then
        LAST_CODEX_INVOCATION_PHASE="bootstrap"
    fi

    if [[ "${#cmd[@]}" -eq 0 || -z "${cmd[0]}" ]]; then
        echo "Error: Failed to build Codex command." >&2
        LAST_CODEX_INVOCATION_PHASE="bootstrap"
    fi

    if [[ "$LAST_CODEX_INVOCATION_PHASE" == "bootstrap" ]]; then
        echo "Failed to build the Codex command." >&2
        exit 1
    fi

    if ! command_exists "${cmd[0]}"; then
        echo "Error: Launcher command '${cmd[0]}' for Codex commit message generation is not available." >&2
        echo "Failed to launch Codex because '${cmd[0]}' is unavailable." >&2
        exit 1
    fi

    printf '%s\n' "$PROMPT" | "${cmd[@]}" >/dev/null
    cat "$output_file"
}

echo "Generating commit message..."
echo ""

case "$SELECTED_PROVIDER" in
    claude)
        run_claude
        ;;
    codex)
        run_codex
        ;;
    *)
        echo "Error: Unsupported provider '$SELECTED_PROVIDER'." >&2
        exit 1
        ;;
esac
echo
