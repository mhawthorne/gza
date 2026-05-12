#!/bin/bash
#
# Repair a git worktree whose parent-repo registration was deleted.
#
# Typical symptom: `git -C <worktree> status` fails with
#     fatal: not a git repository: <parent>/.git/worktrees/<name>
# even though the working directory and its .git file are intact.
#
# Cause: `git worktree prune --expire now` swept the registration out from
# under the working dir. The official `git worktree repair` cannot recover
# this state because both endpoints are pointing at missing locations.
#
# This script rebuilds the missing registration by hand and rebuilds the
# index from HEAD. No working-tree files are touched.

set -euo pipefail
trap 'echo "Error on line $LINENO" >&2' ERR

usage() {
    cat <<EOF >&2
Usage: repair-worktree <worktree-dir> <branch>

  <worktree-dir>  Absolute path to the orphaned worktree.
  <branch>        Branch name the worktree should be checked out on.
                  The branch must already exist in the parent repo.

The script will:
  1. Verify the worktree's .git file references a missing registration.
  2. Recreate the parent-repo registration (.git/worktrees/<name>/).
  3. Run \`git read-tree HEAD\` so \`git status\` shows the correct diff.

Nothing in the worktree's working directory is modified.
EOF
    exit 1
}

[[ $# -eq 2 ]] || usage

worktree_dir="$1"
branch="$2"

case "$worktree_dir" in
    /*) ;;
    *) echo "Error: worktree-dir must be an absolute path" >&2; exit 1 ;;
esac

if [[ ! -d "$worktree_dir" ]]; then
    echo "Error: worktree-dir does not exist: $worktree_dir" >&2
    exit 1
fi

dotgit="$worktree_dir/.git"
if [[ ! -f "$dotgit" ]]; then
    echo "Error: $dotgit is not a regular file (expected a 'gitdir: ...' pointer)" >&2
    echo "       This script is for worktrees, not for ordinary repositories." >&2
    exit 1
fi

# Parse the expected registration path out of the worktree's .git file.
gitdir_line="$(head -1 "$dotgit")"
case "$gitdir_line" in
    gitdir:\ *) reg_dir="${gitdir_line#gitdir: }" ;;
    *) echo "Error: $dotgit does not start with 'gitdir: '" >&2; exit 1 ;;
esac
echo "worktree_dir: $worktree_dir"
echo "branch:       $branch"
echo "registration: $reg_dir"

if [[ -d "$reg_dir" && -f "$reg_dir/HEAD" ]]; then
    echo
    echo "Registration already exists. Trying \`git worktree repair\`..."
    parent_git="$(dirname "$(dirname "$reg_dir")")"  # .../worktrees/<name> → .../.git
    parent_repo="$(dirname "$parent_git")"
    git -C "$parent_repo" worktree repair "$worktree_dir"
    echo "Repair complete (registration was already present)."
    exit 0
fi

# Locate the parent repo from the expected registration path.
# Registration paths follow the layout <parent>/.git/worktrees/<name>.
case "$reg_dir" in
    */.git/worktrees/*) ;;
    *) echo "Error: registration path is not under <repo>/.git/worktrees/: $reg_dir" >&2; exit 1 ;;
esac
parent_git="${reg_dir%/worktrees/*}"   # .../.git
parent_repo="${parent_git%/.git}"      # ...
echo "parent_repo:  $parent_repo"

if [[ ! -d "$parent_git" ]]; then
    echo "Error: parent .git not found: $parent_git" >&2
    exit 1
fi

# Verify the branch exists in the parent repo before we write anything.
if ! git -C "$parent_repo" show-ref --verify --quiet "refs/heads/$branch"; then
    echo "Error: branch '$branch' does not exist in $parent_repo" >&2
    echo "       Create it first or pass an existing branch name." >&2
    exit 1
fi

# Refuse to overwrite a registration directory that contains files but is
# corrupted — surface to the user rather than guessing.
if [[ -e "$reg_dir" ]] && [[ -n "$(ls -A "$reg_dir" 2>/dev/null || true)" ]]; then
    echo "Error: $reg_dir already exists and is non-empty. Inspect it manually before re-running." >&2
    exit 1
fi

echo
echo "Reconstructing registration..."
mkdir -p "$reg_dir"
printf 'ref: refs/heads/%s\n' "$branch" > "$reg_dir/HEAD"
printf '../..\n' > "$reg_dir/commondir"
printf '%s\n' "$dotgit" > "$reg_dir/gitdir"

echo "  wrote $reg_dir/HEAD       (ref: refs/heads/$branch)"
echo "  wrote $reg_dir/commondir  (../..)"
echo "  wrote $reg_dir/gitdir     ($dotgit)"

echo
echo "Rebuilding index from HEAD..."
git -C "$worktree_dir" read-tree HEAD

echo
echo "Verifying..."
current_branch="$(git -C "$worktree_dir" symbolic-ref --quiet --short HEAD || echo '<detached>')"
echo "  current branch: $current_branch"
echo "  status (first 5 lines):"
git -C "$worktree_dir" status --short | head -5 | sed 's/^/    /'

echo
echo "Repair complete. Verify the status above matches what you expected."
echo "If files appear as modified or untracked that should be tracked, check"
echo "whether the working directory had uncommitted changes before the wipe."
