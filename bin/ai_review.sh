#!/usr/bin/env bash
set -uo pipefail

# Generate a standardized LLM review prompt + git diff for this repo.
# Optionally runs an installed LLM CLI if detected (claude / gemini).
#
# Examples:
#   bash bin/ai_review.sh --staged
#   bash bin/ai_review.sh --base origin/main
#   bash bin/ai_review.sh --staged --run

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASE_REF="origin/main"
MODE="staged" # staged | base
DO_RUN=0
PROVIDER="auto" # auto | claude | gemini | print
OUT_FILE=""
DO_COPY=0
USE_PAGER="auto" # auto | 1 | 0
SAVE_RUN_OUTPUT="auto" # auto | 1 | 0
REVIEWS_DIR="$ROOT/reviews"
FILES_MODE=0
FILES=()

usage() {
  cat <<'EOF'
Usage:
  bash bin/ai_review.sh [--staged] [--base <ref>] [--run] [--provider auto|claude|gemini|print]
  bash bin/ai_review.sh [--staged] [--base <ref>] --claude --run
  bash bin/ai_review.sh --files <path> [<path> ...] [--run] [--provider ...]

Modes:
  --staged        Review staged changes (default)
  --base <ref>    Review diff vs a base ref (default base: origin/main)
  --files <paths> Review specific files/paths (no git diff required)

Options:
  --run           If an LLM CLI is installed, run it (otherwise prints prompt+diff)
  --provider      Choose which CLI to run when using --run (default: auto)
  --claude        Shortcut for --provider claude
  --out <file>    Write prompt+diff to a file instead of printing
  --copy          Copy prompt+diff to clipboard (macOS: pbcopy) instead of printing
  --pager         Force paging when printing to a TTY (default: auto)
  --no-pager      Disable paging
  --save          Save --run output to reviews/YYYYmmddHHMMSS-$provider.txt (default: on for --run)
  --no-save       Do not save --run output

Notes:
  - This script prints a review prompt followed by the diff, suitable for pasting
    into Cursor / Claude Code / Gemini.
  - If you pass --run with provider=auto, it will try: `claude` then `gemini`.
EOF
}

timestamp() {
  date +"%Y%m%d%H%M%S"
}

review_out_path() {
  local provider="$1"
  local ts="$2"
  echo "$REVIEWS_DIR/${ts}-${provider}.txt"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --staged)
      MODE="staged"
      shift
      ;;
    --base)
      MODE="base"
      BASE_REF="${2:-}"
      if [[ -z "$BASE_REF" ]]; then
        echo "ERROR: --base requires a ref" >&2
        exit 2
      fi
      shift 2
      ;;
    --run)
      DO_RUN=1
      shift
      ;;
    --files)
      FILES_MODE=1
      MODE="files"
      shift
      if [[ $# -eq 0 ]]; then
        echo "ERROR: --files requires at least one path" >&2
        exit 2
      fi
      while [[ $# -gt 0 ]]; do
        case "$1" in
          --*)
            break
            ;;
          *)
            FILES+=("$1")
            shift
            ;;
        esac
      done
      if [[ "${#FILES[@]}" -eq 0 ]]; then
        echo "ERROR: --files requires at least one path" >&2
        exit 2
      fi
      ;;
    --provider)
      PROVIDER="${2:-}"
      if [[ -z "$PROVIDER" ]]; then
        echo "ERROR: --provider requires a value" >&2
        exit 2
      fi
      shift 2
      ;;
    --claude)
      PROVIDER="claude"
      shift
      ;;
    --out)
      OUT_FILE="${2:-}"
      if [[ -z "$OUT_FILE" ]]; then
        echo "ERROR: --out requires a file path" >&2
        exit 2
      fi
      shift 2
      ;;
    --copy)
      DO_COPY=1
      shift
      ;;
    --pager)
      USE_PAGER="1"
      shift
      ;;
    --no-pager)
      USE_PAGER="0"
      shift
      ;;
    --save)
      SAVE_RUN_OUTPUT="1"
      shift
      ;;
    --no-save)
      SAVE_RUN_OUTPUT="0"
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

if ! command -v git >/dev/null 2>&1; then
  echo "ERROR: git not found on PATH." >&2
  exit 127
fi

default_base_ref() {
  # Prefer the remote HEAD if it exists (e.g. origin/main), otherwise fall back.
  local ref=""
  ref="$(git symbolic-ref -q --short refs/remotes/origin/HEAD 2>/dev/null || true)"
  if [[ -n "$ref" ]]; then
    echo "$ref"
    return 0
  fi
  for candidate in origin/main origin/master main master; do
    if git rev-parse --verify -q "$candidate" >/dev/null 2>&1; then
      echo "$candidate"
      return 0
    fi
  done
  echo ""
}

if [[ "$MODE" == "base" && "$BASE_REF" == "origin/main" ]]; then
  # If user didn't explicitly set --base, try to infer a sensible default.
  inferred="$(default_base_ref)"
  if [[ -n "$inferred" ]]; then
    BASE_REF="$inferred"
  fi
fi

if [[ "$MODE" == "base" ]]; then
  if ! git rev-parse --verify -q "$BASE_REF" >/dev/null 2>&1; then
    echo "ERROR: base ref '$BASE_REF' not found." >&2
    echo "Tip: use '--staged' or pass an existing ref via '--base <ref>'." >&2
    exit 2
  fi
fi

FILES_BLOCK=""
if [[ "$FILES_MODE" == "1" ]]; then
  for p in "${FILES[@]}"; do
    if [[ ! -e "$p" ]]; then
      echo "ERROR: path not found: $p" >&2
      exit 2
    fi
  done

  for p in "${FILES[@]}"; do
    if [[ -d "$p" ]]; then
      while IFS= read -r f; do
        FILES_BLOCK+=$'\n'"===== FILE: $f ====="$'\n'
        FILES_BLOCK+="$(cat "$f")"$'\n'
      done < <(find "$p" -type f -print | LC_ALL=C sort)
    else
      FILES_BLOCK+=$'\n'"===== FILE: $p ====="$'\n'
      FILES_BLOCK+="$(cat "$p")"$'\n'
    fi
  done
else
  DIFF_CMD=(git diff --no-color)
  if [[ "$MODE" == "staged" ]]; then
    DIFF_CMD+=(--cached)
  else
    DIFF_CMD+=("$BASE_REF"...HEAD)
  fi
fi

DIFF=""
if [[ "$FILES_MODE" != "1" ]]; then
  DIFF="$("${DIFF_CMD[@]}" || true)"
  if [[ -z "$DIFF" ]]; then
    echo "No diff found for mode='$MODE' (nothing to review)." >&2
    exit 0
  fi
fi

read -r -d '' PROMPT <<'EOF' || true
You are an automated code reviewer for this repository.

Repo goal: repeatable micro-/meso-benchmarks comparing Python + NumPy performance across architectures (arm64 vs x86_64), producing interpretable pyperf JSON and simple plots.

Repo instructions (canonical): see AGENTS.md.

Review priorities:
1) Correctness & determinism
   - deterministic inputs (seed RNGs), stable sizes
   - avoid measuring allocations you don’t care about (prefer preallocation / out= where applicable)
   - appropriate pyperf usage (bench_func, inner_loops where relevant)
2) Reproducibility across machines/architectures
   - sufficient metadata (arch, python/numpy versions, numpy config, thread env vars)
   - call out BLAS/threading/Rosetta/thermal/power gotchas
3) Interpretability of outputs (pyperf JSON and plots)
4) Safety (no secrets; don’t commit huge results unintentionally)

Output format:
- Summary (1-3 bullets)
- Must-fix issues
- Suggestions
- Questions/assumptions

Now review the following content:
EOF

if [[ "$FILES_MODE" == "1" ]]; then
  OUTPUT="$(printf "%s\n\n%s\n" "$PROMPT" "$FILES_BLOCK")"
else
  OUTPUT="$(printf "%s\n\n%s\n" "$PROMPT" "$DIFF")"
fi

if [[ "$DO_RUN" == "1" ]]; then
  if [[ "$SAVE_RUN_OUTPUT" == "auto" ]]; then
    SAVE_RUN_OUTPUT="1"
  fi
  TS="$(timestamp)"

  case "$PROVIDER" in
    print)
      if [[ "$SAVE_RUN_OUTPUT" == "1" ]]; then
        mkdir -p "$REVIEWS_DIR"
        REVIEW_FILE="$(review_out_path "print" "$TS")"
        printf "%s" "$OUTPUT" >"$REVIEW_FILE"
        echo "Saved output to: $REVIEW_FILE" >&2
      fi
      printf "%s" "$OUTPUT"
      exit 0
      ;;
    auto|claude)
      if command -v claude >/dev/null 2>&1; then
        # IMPORTANT: `claude` defaults to an interactive Ink UI which requires a TTY.
        # When we pipe prompt+diff in, stdin is not a TTY; use --print for non-interactive mode.
        if [[ "$SAVE_RUN_OUTPUT" == "1" ]]; then
          mkdir -p "$REVIEWS_DIR"
          REVIEW_FILE="$(review_out_path "claude" "$TS")"
          printf "%s" "$OUTPUT" | claude --print | tee "$REVIEW_FILE"
          echo "Saved output to: $REVIEW_FILE" >&2
        else
          printf "%s" "$OUTPUT" | claude --print
        fi
        exit 0
      fi
      if [[ "$PROVIDER" == "claude" ]]; then
        echo "ERROR: provider=claude requested but 'claude' was not found on PATH." >&2
        exit 127
      fi
      ;;
    gemini)
      if command -v gemini >/dev/null 2>&1; then
        if [[ "$SAVE_RUN_OUTPUT" == "1" ]]; then
          mkdir -p "$REVIEWS_DIR"
          REVIEW_FILE="$(review_out_path "gemini" "$TS")"
          printf "%s" "$OUTPUT" | gemini | tee "$REVIEW_FILE"
          echo "Saved output to: $REVIEW_FILE" >&2
        else
          printf "%s" "$OUTPUT" | gemini
        fi
        exit 0
      fi
      echo "ERROR: provider=gemini requested but 'gemini' was not found on PATH." >&2
      exit 127
      ;;
    *)
      echo "ERROR: unknown provider: $PROVIDER (expected auto|claude|gemini|print)" >&2
      exit 2
      ;;
  esac

  # Auto fallback: try gemini if claude wasn't available.
  if command -v gemini >/dev/null 2>&1; then
    if [[ "$SAVE_RUN_OUTPUT" == "1" ]]; then
      mkdir -p "$REVIEWS_DIR"
      REVIEW_FILE="$(review_out_path "gemini" "$TS")"
      printf "%s" "$OUTPUT" | gemini | tee "$REVIEW_FILE"
      echo "Saved output to: $REVIEW_FILE" >&2
    else
      printf "%s" "$OUTPUT" | gemini
    fi
    exit 0
  fi

  echo "ERROR: --run requested but no supported CLI found (claude/gemini)." >&2
  exit 127
fi

if [[ -n "$OUT_FILE" ]]; then
  mkdir -p "$(dirname "$OUT_FILE")"
  printf "%s" "$OUTPUT" >"$OUT_FILE"
  echo "Wrote prompt+diff to: $OUT_FILE" >&2
  exit 0
fi

if [[ "$DO_COPY" == "1" ]]; then
  if command -v pbcopy >/dev/null 2>&1; then
    printf "%s" "$OUTPUT" | pbcopy
    echo "Copied prompt+diff to clipboard (pbcopy)." >&2
    exit 0
  fi
  echo "ERROR: --copy requested but 'pbcopy' not found on PATH." >&2
  exit 127
fi

if [[ "$USE_PAGER" == "1" || ( "$USE_PAGER" == "auto" && -t 1 ) ]]; then
  if command -v less >/dev/null 2>&1; then
    # -F: quit if one screen; -R: keep color codes if present; -S: no wrap; -X: don't clear screen on exit
    printf "%s" "$OUTPUT" | less -FRSX
    exit 0
  fi
fi

printf "%s" "$OUTPUT"


