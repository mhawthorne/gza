"""Git operations for Gza."""

import re
import subprocess
from pathlib import Path


class GitError(Exception):
    """Git operation failed."""
    pass


def parse_diff_numstat(numstat_output: str) -> tuple[int, int, int]:
    """Parse --numstat output into (files_changed, lines_added, lines_removed).

    Args:
        numstat_output: Output from 'git diff --numstat'

    Returns:
        Tuple of (files_changed, lines_added, lines_removed)
    """
    files, added, removed = 0, 0, 0
    for line in numstat_output.splitlines():
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        if parts[0] == "-":  # binary file
            continue
        try:
            added += int(parts[0])
            removed += int(parts[1])
            files += 1
        except ValueError:
            continue
    return files, added, removed


class Git:
    """Git operations wrapper."""

    def __init__(self, repo_dir: Path):
        self.repo_dir = repo_dir

    def _run(self, *args: str, check: bool = True, stdin: bytes | None = None) -> subprocess.CompletedProcess:
        """Run a git command.

        Args:
            *args: Git command arguments
            check: If True, raise GitError on non-zero exit code
            stdin: Optional bytes to pass as stdin to the command

        Returns:
            CompletedProcess result
        """
        result = subprocess.run(
            ["git", *args],
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
            input=stdin.decode() if stdin else None,
        )
        if check and result.returncode != 0:
            error_output = result.stderr or result.stdout
            raise GitError(f"git {' '.join(args)} failed:\n{error_output}")
        return result

    def current_branch(self) -> str:
        """Get current branch name."""
        result = self._run("rev-parse", "--abbrev-ref", "HEAD")
        return result.stdout.strip()

    def default_branch(self) -> str:
        """Detect the default branch (main or master)."""
        # Try to get from origin HEAD
        result = self._run("symbolic-ref", "refs/remotes/origin/HEAD", check=False)
        if result.returncode == 0:
            return result.stdout.strip().replace("refs/remotes/origin/", "")

        # Fallback: check which exists locally
        for branch in ["main", "master"]:
            result = self._run("show-ref", "--verify", "--quiet", f"refs/heads/{branch}", check=False)
            if result.returncode == 0:
                return branch

        return "master"

    def checkout(self, branch: str) -> None:
        """Checkout a branch."""
        self._run("checkout", branch)

    def pull(self) -> bool:
        """Pull latest changes. Returns True if successful."""
        result = self._run("pull", "--ff-only", check=False)
        return result.returncode == 0

    def fetch(self, remote: str = "origin") -> None:
        """Fetch latest changes from remote.

        Args:
            remote: The remote to fetch from (default: origin)

        Raises:
            GitError: If the fetch fails
        """
        self._run("fetch", remote)

    def create_branch(self, branch: str, force: bool = False) -> None:
        """Create and checkout a new branch."""
        if force:
            self._run("branch", "-D", branch, check=False)
        self._run("checkout", "-b", branch)

    def has_changes(self, path: str = ".", include_untracked: bool = True) -> bool:
        """Check if there are uncommitted changes or untracked files.

        Args:
            path: Path to check for changes (default: ".")
            include_untracked: Whether to consider untracked files as changes (default: True)

        Returns:
            True if there are staged, unstaged, or (optionally) untracked changes
        """
        staged = self._run("diff", "--cached", "--quiet", "--", path, check=False)
        unstaged = self._run("diff", "--quiet", "--", path, check=False)

        has_tracked_changes = staged.returncode != 0 or unstaged.returncode != 0

        if not include_untracked:
            return has_tracked_changes

        untracked = self._run("ls-files", "--others", "--exclude-standard", "--", path, check=False)
        has_untracked = bool(untracked.stdout.strip())
        return has_tracked_changes or has_untracked

    def add(self, path: str = ".") -> None:
        """Stage changes."""
        self._run("add", path)

    def commit(self, message: str) -> None:
        """Create a commit."""
        self._run("commit", "-m", message)

    def amend(self) -> None:
        """Amend the last commit with staged changes."""
        self._run("commit", "--amend", "--no-edit")

    def branch_exists(self, branch: str) -> bool:
        """Check if a branch exists locally."""
        result = self._run("show-ref", "--verify", "--quiet", f"refs/heads/{branch}", check=False)
        return result.returncode == 0

    def worktree_add(self, path: Path, branch: str, base_branch: str | None = None) -> Path:
        """Create a new worktree with a new branch.

        Args:
            path: Directory where worktree will be created
            branch: Name of the new branch to create
            base_branch: Branch to base the new branch on (defaults to HEAD)

        Returns:
            The path to the created worktree
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        # Remove existing worktree if it exists (handles stale worktrees)
        if path.exists():
            self.worktree_remove(path, force=True)

        # Create worktree with new branch
        args = ["worktree", "add", "-b", branch, str(path)]
        if base_branch:
            args.append(base_branch)
        self._run(*args)

        # Push the new branch to origin with upstream tracking
        # This ensures git push works without errors later
        worktree_git = Git(path)
        try:
            worktree_git.push_branch(branch, remote="origin", set_upstream=True)
        except GitError:
            # If push fails (e.g., no network, no remote configured), continue
            # The branch is still created locally and the task can proceed
            pass

        return path

    def worktree_remove(self, path: Path, force: bool = False) -> None:
        """Remove a worktree.

        Args:
            path: Path to the worktree to remove
            force: Force removal even if worktree is dirty
        """
        args = ["worktree", "remove"]
        if force:
            args.append("--force")
        args.append(str(path))
        self._run(*args, check=False)

    def worktree_list(self) -> list[dict]:
        """List all worktrees.

        Returns:
            List of dicts with 'path', 'head', 'branch' keys
        """
        result = self._run("worktree", "list", "--porcelain")
        worktrees = []
        current: dict[str, str] = {}
        for line in result.stdout.strip().split("\n"):
            if not line:
                if current:
                    worktrees.append(current)
                    current = {}
            elif line.startswith("worktree "):
                current["path"] = line[9:]
            elif line.startswith("HEAD "):
                current["head"] = line[5:]
            elif line.startswith("branch "):
                current["branch"] = line[7:]
        if current:
            worktrees.append(current)
        return worktrees

    def remote_branch_exists(self, branch: str, remote: str = "origin") -> bool:
        """Check if a branch exists on the remote.

        Args:
            branch: The branch name to check
            remote: The remote name (default: origin)

        Returns:
            True if the branch exists on the remote
        """
        result = self._run("ls-remote", "--heads", remote, branch, check=False)
        return bool(result.stdout.strip())

    def needs_push(self, branch: str, remote: str = "origin") -> bool:
        """Check if a local branch has commits that need to be pushed.

        Args:
            branch: The branch name to check
            remote: The remote name (default: origin)

        Returns:
            True if local branch is ahead of remote (or remote doesn't exist)
        """
        # Check if remote branch exists
        if not self.remote_branch_exists(branch, remote):
            return True

        # Compare local and remote commits
        result = self._run(
            "rev-list", "--count", f"{remote}/{branch}..{branch}", check=False
        )
        if result.returncode != 0:
            # If comparison fails, assume we need to push
            return True

        count = int(result.stdout.strip())
        return count > 0

    def push_branch(self, branch: str, remote: str = "origin", set_upstream: bool = True) -> None:
        """Push a branch to the remote.

        Args:
            branch: The branch to push
            remote: The remote name (default: origin)
            set_upstream: Whether to set upstream tracking (default: True)
        """
        args = ["push"]
        if set_upstream:
            args.append("-u")
        args.extend([remote, branch])
        self._run(*args)

    def push_force_with_lease(self, branch: str, remote: str = "origin") -> None:
        """Force push a branch with lease protection.

        Args:
            branch: The branch to push
            remote: The remote name (default: origin)

        Raises:
            GitError: If the force push fails
        """
        self._run("push", "--force-with-lease", remote, branch)

    def get_log(self, revision_range: str, oneline: bool = True) -> str:
        """Get git log output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main..feature")
            oneline: Use --oneline format (default: True)

        Returns:
            The log output as a string
        """
        args = ["log"]
        if oneline:
            args.append("--oneline")
        args.append(revision_range)
        result = self._run(*args, check=False)
        return result.stdout.strip()

    def get_diff_numstat(self, revision_range: str) -> str:
        """Get diff --numstat output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            The diff --numstat output as a string (machine-readable)
        """
        result = self._run("diff", "--numstat", revision_range, check=False)
        return result.stdout.strip()

    def get_diff_stat(self, revision_range: str) -> str:
        """Get diff --stat output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            The diff stat output as a string
        """
        result = self._run("diff", "--stat", revision_range, check=False)
        return result.stdout.strip()

    def get_diff_stat_parsed(self, revision_range: str) -> tuple[int, int, int]:
        """Get parsed diff statistics for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            Tuple of (files_changed, insertions, deletions)
        """
        stat_output = self.get_diff_stat(revision_range)
        if not stat_output:
            return (0, 0, 0)

        lines = stat_output.strip().split("\n")
        summary = lines[-1].strip()

        files = 0
        insertions = 0
        deletions = 0

        m = re.search(r"(\d+) files? changed", summary)
        if m:
            files = int(m.group(1))
        m = re.search(r"(\d+) insertions?\(\+\)", summary)
        if m:
            insertions = int(m.group(1))
        m = re.search(r"(\d+) deletions?\(-\)", summary)
        if m:
            deletions = int(m.group(1))

        return (files, insertions, deletions)

    def get_diff(self, revision_range: str) -> str:
        """Get full diff output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            The full diff output as a string
        """
        result = self._run("diff", revision_range, check=False)
        return result.stdout.strip()

    def is_merged(self, branch: str, into: str | None = None, use_cherry: bool = False) -> bool:
        """Check if a branch has been merged into another branch.

        By default, uses git merge-tree to simulate a merge and compare the
        resulting tree with the target branch's tree. If they're identical,
        merging the branch would be a no-op, meaning all changes are already
        in the target. This correctly handles squash merges, rebases, and
        branches that diverged but have equivalent content.

        Args:
            branch: The branch to check
            into: The target branch (defaults to default branch)
            use_cherry: Use git cherry instead of merge-tree (legacy method)

        Returns:
            True if the branch has been merged into the target
        """
        if into is None:
            into = self.default_branch()

        # Check if branch exists
        if not self.branch_exists(branch):
            return True  # Branch deleted, assume merged

        if use_cherry:
            # Legacy method: Use git cherry to detect if commits have been applied
            # git cherry shows - for commits already in target, + for commits not in target
            result = self._run("cherry", into, branch, check=False)
            if result.returncode != 0:
                return False

            # If all lines start with -, all commits have been merged
            # If there's no output, the branches are identical (also merged)
            lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
            return all(line.startswith("-") for line in lines)
        else:
            # Merge-tree method: Simulate a merge and compare trees
            # If the merged tree equals the target's tree, the branch adds nothing
            result = self._run("merge-tree", "--write-tree", into, branch, check=False)
            if result.returncode != 0:
                # merge-tree failed (likely conflicts), branch is not cleanly merged
                return False

            merged_tree = result.stdout.strip()
            # Get the target branch's tree
            target_tree_result = self._run("rev-parse", f"{into}^{{tree}}", check=False)
            if target_tree_result.returncode != 0:
                return False

            target_tree = target_tree_result.stdout.strip()
            # If trees match, merging would be a no-op - branch is effectively merged
            return merged_tree == target_tree

    def can_merge(self, branch: str, into: str | None = None) -> bool:
        """Check if a branch can be merged cleanly (no conflicts).

        Uses git merge-tree to simulate a merge without touching the worktree.

        Args:
            branch: The branch to check
            into: The target branch (defaults to default branch)

        Returns:
            True if the branch can be merged without conflicts
        """
        if into is None:
            into = self.default_branch()

        if not self.branch_exists(branch):
            return False

        # git merge-tree returns 0 for clean merge, 1 for conflicts
        result = self._run("merge-tree", "--write-tree", into, branch, check=False)
        return result.returncode == 0

    def merge(self, branch: str, squash: bool = False, commit_message: str | None = None) -> None:
        """Merge a branch into the current branch.

        Args:
            branch: The branch to merge
            squash: Use squash merge (default: False, uses --no-ff)
            commit_message: Commit message for squash merge (required if squash=True)

        Raises:
            GitError: If the merge fails
        """
        args = ["merge"]
        if squash:
            args.append("--squash")
        else:
            args.append("--no-ff")
        args.append(branch)
        self._run(*args)

        # Auto-commit after squash merge
        if squash:
            if not commit_message:
                raise ValueError("commit_message is required for squash merge")
            self.commit(commit_message)

    def merge_abort(self) -> None:
        """Abort a merge in progress and restore clean state.

        This is called after a failed merge to clean up the working directory
        and return to the state before the merge was attempted.

        Raises:
            GitError: If aborting the merge fails
        """
        self._run("merge", "--abort")

    def rebase(self, branch: str) -> None:
        """Rebase the current branch onto another branch.

        Args:
            branch: The branch to rebase onto

        Raises:
            GitError: If the rebase fails
        """
        self._run("rebase", branch)

    def rebase_abort(self) -> None:
        """Abort a rebase in progress and restore clean state.

        This is called after a failed rebase to clean up the working directory
        and return to the state before the rebase was attempted.

        Raises:
            GitError: If aborting the rebase fails
        """
        self._run("rebase", "--abort")

    def delete_branch(self, branch: str, force: bool = False) -> None:
        """Delete a local branch.

        Args:
            branch: The branch to delete
            force: Force deletion even if not fully merged (default: False)

        Raises:
            GitError: If the deletion fails
        """
        args = ["branch"]
        if force:
            args.append("-D")
        else:
            args.append("-d")
        args.append(branch)
        self._run(*args)

    def count_commits_ahead(self, branch: str, base: str) -> int:
        """Count how many commits a branch is ahead of base.

        Args:
            branch: The branch to check
            base: The base branch to compare against

        Returns:
            Number of commits that branch is ahead of base
        """
        result = self._run("rev-list", "--count", f"{base}..{branch}", check=False)
        if result.returncode != 0:
            return 0
        return int(result.stdout.strip())


def cleanup_worktree_for_branch(git: "Git", branch: str, force: bool = False) -> Path | None:
    """Clean up worktree if branch is checked out in one.

    Args:
        git: Git instance for the main repository
        branch: Branch name to check for worktree
        force: If True, remove worktree even with uncommitted changes

    Returns:
        Path to cleaned worktree, or None if no worktree found

    Raises:
        ValueError: If worktree has uncommitted changes and force=False
    """
    worktrees = git.worktree_list()
    worktree_path = None
    for wt in worktrees:
        wt_branch = wt.get("branch", "")
        # Branch is stored as refs/heads/branch-name
        if wt_branch == f"refs/heads/{branch}" or wt_branch == branch:
            worktree_path = Path(wt["path"])
            break

    if worktree_path:
        # Check if worktree has uncommitted changes
        worktree_git = Git(worktree_path)
        if worktree_git.has_changes(include_untracked=True) and not force:
            raise ValueError(
                f"Worktree at {worktree_path} has uncommitted changes.\n"
                f"\nOptions:\n"
                f"  1. cd {worktree_path} and commit or discard changes\n"
                f"  2. Use --force to remove the worktree anyway (loses changes)"
            )

        # Remove the worktree
        git.worktree_remove(worktree_path, force=force)
        return worktree_path

    return None
