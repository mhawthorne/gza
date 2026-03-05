#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: git-worktree-create <worktree-dir>" >&2
    exit 1
fi

worktree_dir="$1"
branch="$(basename "$worktree_dir")"
main_checkout="$(git rev-parse --show-toplevel)"

git worktree add -b "$branch" "$worktree_dir"

ln -s "$main_checkout/.gza" "$worktree_dir/.gza"

(cd "$worktree_dir" && gza skills-install)

echo ""
echo "Worktree ready:"
echo "  cd $worktree_dir"
