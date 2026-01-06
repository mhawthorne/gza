#!/usr/bin/env bash
set -euo pipefail

# Security scan wrapper.
#
# Pre-commit mode (default): run a fast scan appropriate for commits (staged content).
# Full mode: scan the working tree (and optionally history) for a "pre-publish" check.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MODE="pre-commit"   # pre-commit | full
HISTORY=0           # 0 | 1 (only meaningful for MODE=full)
REDACT=1            # 0 | 1

usage() {
  cat <<'EOF'
Usage:
  bin/security_scan.sh [--mode pre-commit|full] [--history] [--no-redact]

Modes:
  --mode pre-commit   Fast scan intended for Git commits (default).
  --mode full         Full scan intended before publishing.

Options:
  --history     In full mode, also scan git history (can be slower/noisier).
  --no-redact   Show full secret matches in output (default redacts).

Examples:
  # Install hooks then commit as usual
  pre-commit install

  # Run the same check manually
  bin/security_scan.sh --mode pre-commit

  # Pre-publish sweep (working tree only)
  bin/security_scan.sh --mode full

  # Pre-publish sweep including history
  bin/security_scan.sh --mode full --history
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --history)
      HISTORY=1
      shift
      ;;
    --no-redact)
      REDACT=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown arg: $1" >&2
      echo >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! command -v gitleaks >/dev/null 2>&1; then
  cat >&2 <<'EOF'
ERROR: gitleaks is not available on PATH.

If you're running via pre-commit, run:
  pre-commit install
  pre-commit run gitleaks --all-files

Or install gitleaks locally (e.g. Homebrew) and retry:
  brew install gitleaks
EOF
  exit 127
fi

ARGS=()
if [[ "$REDACT" == "1" ]]; then
  ARGS+=(--redact)
fi

case "$MODE" in
  pre-commit)
    # Fast and commit-focused. "protect" is designed for pre-commit style usage.
    gitleaks protect --staged "${ARGS[@]}"
    ;;
  full)
    if [[ "$HISTORY" == "1" ]]; then
      # Default (git-aware) scan; includes history.
      gitleaks detect --source . "${ARGS[@]}"
    else
      # Working tree only (no history) to keep it quick and avoid scanning past commits.
      gitleaks detect --source . --no-git "${ARGS[@]}"
    fi
    ;;
  *)
    echo "ERROR: --mode must be 'pre-commit' or 'full' (got: $MODE)" >&2
    exit 2
    ;;
esac


