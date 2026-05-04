#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE_NAME="gza-gza-codex:latest"
DOCKERFILE="etc/Dockerfile.codex"

usage() {
  cat <<'EOF'
Usage:
  bin/codex-model-check.sh --model <model> [--build] [--image <name>]

Smoke-tests whether the codex CLI in a Docker image accepts a given model
by running a single trivial prompt against the API. Useful for verifying
that a Dockerfile version pin actually unlocks a new model id.

Options:
  --model <model>   Model id to test (e.g. gpt-5.5). Required.
  --build           Rebuild the image with --no-cache before running.
                    Omit to test against the existing local image.
  --image <name>    Image tag to test (default: gza-gza-codex:latest).
  -h, --help        Show this help.

Auth: mounts $HOME/.codex into the container so OAuth credentials are
reused. If you use CODEX_API_KEY / OPENAI_API_KEY instead, export it
before running and the script will forward it.

Exit code: 0 if codex emits a non-error response, 1 otherwise.
EOF
  exit 1
}

MODEL=""
BUILD=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model) MODEL="${2:-}"; shift 2 ;;
    --build) BUILD=1; shift ;;
    --image) IMAGE_NAME="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "Unknown argument: $1" >&2; usage ;;
  esac
done

if [[ -z "$MODEL" ]]; then
  echo "Error: --model is required" >&2
  usage
fi

if [[ "$BUILD" == "1" ]]; then
  echo "Building $IMAGE_NAME from $DOCKERFILE (--no-cache)..."
  docker build --no-cache -f "$DOCKERFILE" -t "$IMAGE_NAME" etc/
fi

echo "Codex CLI version in image:"
docker run --rm "$IMAGE_NAME" codex --version

DOCKER_ARGS=(
  "run" "--rm" "-i"
  "-v" "$HOME/.codex:/home/gza/.codex"
)

if [[ -n "${CODEX_API_KEY:-}" ]]; then
  DOCKER_ARGS+=("-e" "CODEX_API_KEY")
fi
if [[ -n "${OPENAI_API_KEY:-}" ]]; then
  DOCKER_ARGS+=("-e" "OPENAI_API_KEY")
fi

echo
echo "Running codex with model=$MODEL..."
echo "----------------------------------------"

OUTPUT_FILE="$(mktemp)"
trap 'rm -f "$OUTPUT_FILE"' EXIT

set +e
docker "${DOCKER_ARGS[@]}" "$IMAGE_NAME" \
  codex -c check_for_update_on_startup=false \
        exec --json --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check \
        -m "$MODEL" \
        - <<<"reply with the single word: ok" \
  | tee "$OUTPUT_FILE"
DOCKER_EXIT=$?
set -e

echo "----------------------------------------"
echo

if grep -q '"type":"error"' "$OUTPUT_FILE"; then
  echo "RESULT: codex emitted an error event. Model '$MODEL' was not accepted." >&2
  exit 1
fi

if [[ $DOCKER_EXIT -ne 0 ]]; then
  echo "RESULT: codex exited non-zero ($DOCKER_EXIT) with no error event." >&2
  exit 1
fi

echo "RESULT: codex accepted model '$MODEL' and completed without error events."
