#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IMAGE_NAME="gza-test"
DOCKERFILE="etc/Dockerfile.test"
MARKER_FILE=".docker-build-marker"

usage() {
  cat <<'EOF'
Usage:
  bin/docker-test.sh <command>

Commands:
  sanity        Build image, install gza-agent from PyPI, run gza --help
  shell         Build image and open an interactive shell
  shell-install Build image, install gza-agent, then open an interactive shell

The image is rebuilt automatically if etc/Dockerfile.test has changed since the last build.
EOF
  exit 1
}

needs_rebuild() {
  [[ ! -f "$MARKER_FILE" ]] && return 0
  [[ "$DOCKERFILE" -nt "$MARKER_FILE" ]] && return 0
  return 1
}

build_image() {
  if needs_rebuild; then
    echo "Building Docker image '$IMAGE_NAME'..."
    docker build -t "$IMAGE_NAME" -f "$DOCKERFILE" .
    touch "$MARKER_FILE"
  else
    echo "Docker image '$IMAGE_NAME' is up to date."
  fi
}

run_sanity() {
  build_image
  echo "Running sanity check..."
  docker run --rm "$IMAGE_NAME" sh -c 'uv pip install --system gza-agent && gza --help'
}

run_shell() {
  build_image
  echo "Opening shell..."
  docker run -it --rm -e ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" "$IMAGE_NAME" /bin/bash
}

run_shell_install() {
  build_image
  echo "Installing gza-agent and opening shell..."
  docker run -it --rm -e ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" "$IMAGE_NAME" sh -c 'uv pip install --system gza-agent && exec /bin/bash'
}

[[ $# -lt 1 ]] && usage

case "$1" in
  sanity)
    run_sanity
    ;;
  shell)
    run_shell
    ;;
  shell-install)
    run_shell_install
    ;;
  *)
    echo "Unknown command: $1"
    usage
    ;;
esac
