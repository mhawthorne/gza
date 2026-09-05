#!/usr/bin/env bash
# Generate release notes from git commits between two tags, using the
# default agent/model harness (see bin/agent-defaults.sh).
# Usage: ./bin/generate-release-notes.sh <from_tag> <to_tag>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/agent-defaults.sh"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Check if two arguments are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <from_tag> <to_tag>"
    echo "Example: $0 v1.0.0 v1.1.0"
    exit 1
fi

FROM_TAG=$1
TO_TAG=$2

# Verify the tags exist
if ! git rev-parse "$FROM_TAG" >/dev/null 2>&1; then
    echo "Error: Tag '$FROM_TAG' not found"
    exit 1
fi

if ! git rev-parse "$TO_TAG" >/dev/null 2>&1; then
    echo "Error: Tag '$TO_TAG' not found"
    exit 1
fi

# Get the list of commits between the tags
COMMITS=$(git log --pretty=format:"%h - %s (%an, %ar)" "$FROM_TAG".."$TO_TAG")

# Check if there are any commits
if [ -z "$COMMITS" ]; then
    echo "No commits found between $FROM_TAG and $TO_TAG"
    exit 1
fi

# Get detailed commit information for better context
DETAILED_COMMITS=$(git log --pretty=format:"%h - %s%n%b%n---" "$FROM_TAG".."$TO_TAG")

PROMPT="Generate release notes in markdown format for the changes between $FROM_TAG and $TO_TAG.

Here are the commits:

$DETAILED_COMMITS

Please create well-structured release notes that:
1. Start with a header: # Release Notes: $FROM_TAG → $TO_TAG
2. Include a summary section with bullet points highlighting the changes users would care about most (new commands, breaking changes, major improvements)
3. Group changes into categories (e.g., Features, Bug Fixes, Improvements, Documentation, etc.)
4. Use bullet points for each change
5. Prioritize user-facing changes (CLI commands, options, arguments, log files, etc.)
6. Be concise but informative
7. Highlight breaking changes if any are evident
8. Use proper markdown formatting

Output ONLY the markdown release notes, no additional commentary."

command_exists() {
    local command_name="$1"

    if [[ "$command_name" == */* ]]; then
        [[ -x "$command_name" ]]
    else
        command -v "$command_name" >/dev/null 2>&1
    fi
}

build_agent_command() {
    local agent="$1"
    local project_root="$2"
    local work_dir="$3"
    local output_file="$4"

    PYTHONPATH="$project_root/src${PYTHONPATH:+:$PYTHONPATH}" \
        uv run python - "$agent" "$project_root" "$work_dir" "$output_file" <<'PY'
import sys
from dataclasses import replace
from pathlib import Path

from gza.config import Config
from gza.providers.claude import ClaudeProvider
from gza.providers.codex import CodexProvider

provider_name = sys.argv[1]
project_root = Path(sys.argv[2])
work_dir = Path(sys.argv[3])
output_file = Path(sys.argv[4])

config = Config.load(project_root)
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

if provider_name == "claude":
    cmd = ClaudeProvider.build_noninteractive_command(resolved_config, work_dir)
elif provider_name == "codex":
    cmd = CodexProvider.build_noninteractive_command(resolved_config, work_dir)
    for idx, arg in enumerate(cmd):
        if arg == "-":
            cmd[idx:idx] = ["--output-last-message", str(output_file)]
            break
    else:
        raise SystemExit("Codex non-interactive command is missing stdin placeholder '-'")
else:
    raise SystemExit(f"Unsupported agent: {provider_name}")

for arg in cmd:
    sys.stdout.buffer.write(arg.encode("utf-8"))
    sys.stdout.buffer.write(b"\0")
PY
}

run_agent() {
    local agent="$1"
    local output_file
    local cmd_file
    output_file=$(mktemp)
    cmd_file=$(mktemp)
    trap 'rm -f "$output_file" "$cmd_file"' RETURN

    local -a cmd=()
    if ! build_agent_command "$agent" "$PROJECT_ROOT" "$(pwd)" "$output_file" >"$cmd_file"; then
        echo "Error: Failed to build $agent command." >&2
        exit 1
    fi
    if ! mapfile -d '' -t cmd <"$cmd_file"; then
        echo "Error: Failed to build $agent command." >&2
        exit 1
    fi

    if [[ "${#cmd[@]}" -eq 0 || -z "${cmd[0]}" ]]; then
        echo "Error: Failed to build $agent command." >&2
        exit 1
    fi

    if ! command_exists "${cmd[0]}"; then
        echo "Error: Launcher command '${cmd[0]}' for $agent is not available." >&2
        exit 1
    fi

    if [[ "$agent" == "claude" ]]; then
        printf '%s\n' "$PROMPT" | "${cmd[@]}"
    else
        printf '%s\n' "$PROMPT" | "${cmd[@]}" >/dev/null
        cat "$output_file"
    fi
}

echo "Generating release notes from $FROM_TAG to $TO_TAG using $DEFAULT_AGENT..."
echo ""

OUTPUT_DIR="docs/release-notes"
mkdir -p "$OUTPUT_DIR"

# Sanitize the tag name for use as a filename (replace / with -)
OUTPUT_FILE="$OUTPUT_DIR/${TO_TAG//\//-}.md"

run_agent "$DEFAULT_AGENT" > "$OUTPUT_FILE"
echo "Release notes written to: $OUTPUT_FILE"
