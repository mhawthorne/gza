#!/bin/bash

set -euo pipefail
trap 'echo "Error on line $LINENO" >&2' ERR

if [[ $# -ne 1 ]]; then
    echo "Usage: git-worktree-create <worktree-dir>" >&2
    exit 1
fi

case "$1" in
    /*) ;;
    *) echo "Error: worktree-dir must be an absolute path" >&2; exit 1 ;;
esac

worktree_dir="$1"
echo "worktree_dir:$worktree_dir"

branch="$(basename "$worktree_dir")"
echo "branch:$branch"

#git worktree list --porcelain | head -1 | sed 's/^worktree //'

main_checkout="$(git worktree list --porcelain | head -1 | sed 's/^worktree //')"
echo "main_checkout:$main_checkout"

echo "adding worktree"
git worktree add -b "$branch" "$worktree_dir"

(
cd "$worktree_dir" || exit 2
uv sync --dev
gza skills-install --dev
ln -s "$main_checkout/.gza" "$worktree_dir/.gza"
)

echo
echo "worktree ready at $worktree_dir"
