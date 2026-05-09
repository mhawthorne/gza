#!/bin/bash
# Link this worktree's .gza/ to the main checkout's .gza/ so gza CLI commands
# share the task DB, workers, and logs across worktrees. Idempotent.

set -euo pipefail
trap 'echo "Error on line $LINENO" >&2' ERR

worktree_dir="$(git rev-parse --show-toplevel)"
main_checkout="$(git worktree list --porcelain | sed -n '1s/^worktree //p')"

if [[ "$worktree_dir" == "$main_checkout" ]]; then
    echo "Refusing to link: current directory is the main checkout ($main_checkout)" >&2
    exit 1
fi

target="$main_checkout/.gza"
link="$worktree_dir/.gza"

if [[ ! -d "$target" ]]; then
    echo "Main checkout has no .gza directory at $target" >&2
    exit 1
fi

if [[ -L "$link" ]]; then
    current="$(readlink "$link")"
    if [[ "$current" == "$target" ]]; then
        echo "Already linked: $link -> $target"
        exit 0
    fi
    echo "Refusing to link: $link is a symlink to $current (expected $target)" >&2
    exit 1
fi

if [[ -e "$link" ]]; then
    # Only remove if it contains no files — empty subdirs (like .gza/workers/)
    # are scaffolding gza creates on first run and are safe to drop.
    if find "$link" -type f -o -type l | read; then
        echo "Refusing to link: $link exists and contains files. Move or delete it manually." >&2
        exit 1
    fi
    rm -rf "$link"
fi

ln -s "$target" "$link"
echo "linked $link -> $target"
