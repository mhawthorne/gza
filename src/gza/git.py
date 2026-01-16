"""Git operations for Gza."""

import subprocess
from pathlib import Path


class GitError(Exception):
    """Git operation failed."""
    pass


class Git:
    """Git operations wrapper."""

    def __init__(self, repo_dir: Path):
        self.repo_dir = repo_dir

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a git command."""
        result = subprocess.run(
            ["git", *args],
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            raise GitError(f"git {' '.join(args)} failed: {result.stderr}")
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
        current = {}
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

    def get_diff_stat(self, revision_range: str) -> str:
        """Get diff --stat output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            The diff stat output as a string
        """
        result = self._run("diff", "--stat", revision_range, check=False)
        return result.stdout.strip()

    def is_merged(self, branch: str, into: str | None = None) -> bool:
        """Check if a branch has been merged into another branch.

        Uses git cherry to detect if the branch's changes have been applied,
        which works correctly for squash merges (where commit SHAs differ but
        the patch content is the same).

        Args:
            branch: The branch to check
            into: The target branch (defaults to default branch)

        Returns:
            True if the branch has been merged into the target
        """
        if into is None:
            into = self.default_branch()

        # Check if branch exists
        if not self.branch_exists(branch):
            return True  # Branch deleted, assume merged

        # Use git cherry to detect if commits have been applied (works with squash merges)
        # git cherry shows - for commits already in target, + for commits not in target
        result = self._run("cherry", into, branch, check=False)
        if result.returncode != 0:
            return False

        # If all lines start with -, all commits have been merged
        # If there's no output, the branches are identical (also merged)
        lines = result.stdout.strip().split("\n") if result.stdout.strip() else []
        return all(line.startswith("-") for line in lines)

    def merge(self, branch: str, squash: bool = False) -> None:
        """Merge a branch into the current branch.

        Args:
            branch: The branch to merge
            squash: Use squash merge (default: False, uses --no-ff)

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
