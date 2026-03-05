#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: git-worktree-create <worktree-dir>" >&2
    exit 1
fi

worktree_dir="$1"
branch="$(basename "$worktree_dir")"
main_checkout="$(git worktree list --porcelain | head -1 | sed 's/^worktree //')"

git worktree add -b "$branch" "$worktree_dir"

(
cd "$worktree_dir" || exit 2
uv sync --dev
gza skills-install --dev
ln -s "$main_checkout/.gza" "$worktree_dir/.gza"
)

echo
echo "worktree ready at $worktree_dir"
