"""Git operations for Gza."""

import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitError(Exception):
    """Git operation failed."""
    pass


@dataclass(frozen=True)
class GitApplyResult:
    """Outcome of ``git apply --3way`` without forcing exception control flow."""

    returncode: int
    stdout: str
    stderr: str

    @property
    def error_output(self) -> str:
        """Best-effort human-readable command output for failures."""
        return self.stderr or self.stdout


@dataclass(frozen=True)
class ResolvedGitRef:
    """Best-effort ref resolution outcome for callers with different warning policy."""

    sha: str | None
    warning: str | None = None


def _unquote_c_style_path(path: str) -> str:
    """Decode git C-style quoted paths from porcelain output."""
    if not (len(path) >= 2 and path[0] == '"' and path[-1] == '"'):
        return path

    escaped = path[1:-1]
    out = bytearray()
    i = 0
    while i < len(escaped):
        ch = escaped[i]
        if ch != "\\":
            out.extend(ch.encode("utf-8"))
            i += 1
            continue

        if i + 1 >= len(escaped):
            out.append(ord("\\"))
            break

        nxt = escaped[i + 1]
        if nxt in "01234567":
            j = i + 1
            while j < len(escaped) and (j - (i + 1)) < 3 and escaped[j] in "01234567":
                j += 1
            out.append(int(escaped[i + 1:j], 8))
            i = j
            continue

        simple_escapes = {
            '"': ord('"'),
            "\\": ord("\\"),
            "a": 7,
            "b": 8,
            "f": 12,
            "n": 10,
            "r": 13,
            "t": 9,
            "v": 11,
        }
        if nxt in simple_escapes:
            out.append(simple_escapes[nxt])
        else:
            out.extend(nxt.encode("utf-8"))
        i += 2

    return out.decode("utf-8", errors="surrogateescape")


def _split_rename_paths(pathspec: str) -> tuple[str, str] | None:
    """Split ``old -> new`` pathspec while respecting quoted segments."""
    in_quotes = False
    escaped = False

    for i, ch in enumerate(pathspec):
        if escaped:
            escaped = False
            continue

        if in_quotes and ch == "\\":
            escaped = True
            continue

        if ch == '"':
            in_quotes = not in_quotes
            continue

        if not in_quotes and pathspec.startswith(" -> ", i):
            return pathspec[:i], pathspec[i + 4:]

    return None


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


def resolve_ref_if_possible(git: "Git", ref: str | None) -> ResolvedGitRef:
    """Resolve ``ref`` when available without forcing a caller-wide failure."""
    if not ref:
        return ResolvedGitRef(None)

    def _normalize(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    rev_parse_if_exists = getattr(git, "rev_parse_if_exists", None)
    if callable(rev_parse_if_exists):
        try:
            return ResolvedGitRef(_normalize(rev_parse_if_exists(ref)))
        except GitError:
            return ResolvedGitRef(None)
        except Exception as exc:
            return ResolvedGitRef(None, f"unexpected error resolving ref '{ref}': {exc}")

    rev_parse = getattr(git, "rev_parse", None)
    if callable(rev_parse):
        try:
            return ResolvedGitRef(_normalize(rev_parse(ref)))
        except GitError:
            return ResolvedGitRef(None)
        except Exception as exc:
            return ResolvedGitRef(None, f"unexpected error resolving ref '{ref}': {exc}")

    return ResolvedGitRef(None)


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

    def remote_exists(self, remote: str = "origin") -> bool:
        """Return True when a named git remote is configured."""
        result = self._run("remote", "get-url", remote, check=False)
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

    def status_porcelain(self) -> set[tuple[str, str]]:
        """Return set of (status, filepath) tuples from git status --porcelain.

        Each entry is a tuple like ('M', 'src/foo.py') or ('??', 'new_file.txt').
        The status codes follow git's porcelain format (M, A, D, ??, etc.).
        """
        result = self._run("status", "--porcelain", check=False)
        entries: set[tuple[str, str]] = set()
        for line in result.stdout.splitlines():
            if not line:
                continue
            # Porcelain format: XY filename (or XY orig -> renamed)
            status = line[:2].strip()
            filepath = line[3:]
            rename_paths = _split_rename_paths(filepath)
            if rename_paths is not None:
                _src_path, dst_path = rename_paths
                filepath = dst_path
            filepath = _unquote_c_style_path(filepath)
            entries.add((status, filepath))
        return entries

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

    def worktree_remove(self, path: Path, force: bool = False) -> subprocess.CompletedProcess:
        """Remove a worktree.

        Args:
            path: Path to the worktree to remove
            force: Force removal even if worktree is dirty

        Returns:
            CompletedProcess result from ``git worktree remove``.
        """
        args = ["worktree", "remove"]
        if force:
            args.append("--force")
        args.append(str(path))
        return self._run(*args, check=False)

    def worktree_list(self) -> list[dict]:
        """List all worktrees.

        Returns:
            List of dicts parsed from ``git worktree list --porcelain``.

            Known keys include ``path``, ``head``, ``branch``, and optional
            state flags like ``prunable``.
        """
        result = self._run("worktree", "list", "--porcelain")
        worktrees = []
        current: dict[str, str | bool] = {}
        for line in result.stdout.splitlines():
            if not line:
                if current:
                    worktrees.append(current)
                    current = {}
                continue

            key, sep, value = line.partition(" ")
            if key == "worktree" and sep:
                current["path"] = value
            elif key == "HEAD" and sep:
                current["head"] = value
            elif key == "branch" and sep:
                current["branch"] = value
            elif key == "prunable":
                # ``prunable`` can be a bare flag or include a reason.
                current["prunable"] = value if sep else True
            elif key in {"bare", "detached"}:
                current[key] = True
            elif key == "locked":
                current["locked"] = value if sep else True
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

    def get_diff_numstat(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
    ) -> str:
        """Get diff --numstat output for a revision range.

        Args:
            revision_range: The revision range (e.g., "main...feature")

        Returns:
            The diff --numstat output as a string (machine-readable)
        """
        args = ["diff", "--numstat", "--find-renames", "--find-copies", "--find-copies-harder", revision_range]
        if paths:
            args.append("--")
            args.extend(paths)
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

    def ref_exists(self, ref: str) -> bool:
        """Return whether a ref resolves to a commit."""
        result = self._run("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
        return result.returncode == 0

    def resolve_merge_source_ref(self, branch: str, *, remote: str = "origin") -> str | None:
        """Return an existing ref that can prove merge truth for a branch.

        Prefer the local branch when it survives. Fall back to ``<remote>/<branch>``
        when only the remote-tracking ref remains. If neither exists, return
        ``None`` so callers do not treat a missing source ref as merge proof.
        """
        if self.branch_exists(branch):
            return branch

        remote_ref = f"{remote}/{branch}"
        if self.ref_exists(remote_ref):
            return remote_ref

        return None

    def rev_parse(self, ref: str) -> str:
        """Resolve a ref to its commit SHA."""
        result = self._run("rev-parse", "--verify", f"{ref}^{{commit}}")
        return result.stdout.strip()

    def rev_parse_if_exists(self, ref: str) -> str | None:
        """Resolve a ref to its commit SHA when it exists."""
        result = self._run("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False)
        if result.returncode != 0:
            return None
        return result.stdout.strip()

    def get_commit_subject(self, commit_ref: str) -> str:
        """Get the subject line for a single committed revision."""
        result = self._run("show", "-s", "--format=%s", commit_ref, check=False)
        return result.stdout.strip()

    def update_ref(self, ref: str, new_oid: str, old_oid: str | None = None) -> None:
        """Update a ref, optionally requiring the current value to match ``old_oid``."""
        args = ["update-ref", ref, new_oid]
        if old_oid is not None:
            args.append(old_oid)
        self._run(*args)

    def get_diff_name_status(self, revision_range: str, paths: tuple[str, ...] | list[str] = ()) -> str:
        """Get machine-readable name/status output for a revision range and optional paths."""
        args = ["diff", "--name-status", "--find-renames", "--find-copies", "--find-copies-harder", revision_range]
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        return result.stdout.strip()

    def get_commit_name_status(self, commit_ref: str, paths: tuple[str, ...] | list[str] = ()) -> str:
        """Get machine-readable name/status output for a single committed revision."""
        args = [
            "show",
            "--format=",
            "--name-status",
            "--find-renames",
            "--find-copies",
            "--find-copies-harder",
            commit_ref,
        ]
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        return result.stdout.strip()

    def get_diff_patch_for_paths(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str],
        *,
        binary: bool = False,
    ) -> str:
        """Get patch text for a revision range scoped to specific paths."""
        args = ["diff", "--find-renames", "--find-copies", "--find-copies-harder"]
        if binary:
            args.append("--binary")
        args.append(revision_range)
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        return result.stdout

    def get_commit_numstat(
        self,
        commit_ref: str,
        paths: tuple[str, ...] | list[str] = (),
    ) -> str:
        """Get machine-readable numstat output for a single committed revision."""
        args = [
            "show",
            "--format=",
            "--numstat",
            "--find-renames",
            "--find-copies",
            "--find-copies-harder",
            commit_ref,
        ]
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        return result.stdout.strip()

    def get_commit_patch_for_paths(
        self,
        commit_ref: str,
        paths: tuple[str, ...] | list[str],
        *,
        binary: bool = False,
    ) -> str:
        """Get patch text for a single committed revision."""
        args = [
            "show",
            "--format=",
            "--find-renames",
            "--find-copies",
            "--find-copies-harder",
        ]
        if binary:
            args.append("--binary")
        args.append(commit_ref)
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        return result.stdout

    def apply_patch_file_result(self, patch_file: Path) -> GitApplyResult:
        """Run ``git apply --3way`` and return the raw result."""
        result = self._run("apply", "--3way", str(patch_file), check=False)
        return GitApplyResult(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def reverse_check_patch_file_result(self, patch_file: Path) -> GitApplyResult:
        """Check whether a patch is already present by reverse-applying it in check mode."""
        result = self._run("apply", "--check", "--reverse", str(patch_file), check=False)
        return GitApplyResult(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    def apply_patch_file(self, patch_file: Path) -> None:
        """Apply a patch file with ``git apply --3way``."""
        result = self.apply_patch_file_result(patch_file)
        if result.returncode != 0:
            error_output = result.error_output
            raise GitError(
                f"git apply --3way {patch_file} failed:\n{error_output}"
            )

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

        # Accept either a local branch name or any other resolvable ref
        # (for example ``origin/feature`` during canonical sync reconciliation).
        if not self.branch_exists(branch) and not self.ref_exists(branch):
            return True  # Ref deleted, assume merged

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
        Accepts either a local branch name or any other resolvable commit ref,
        such as ``origin/<branch>`` when advance planning is reconciling across
        worktrees.

        Args:
            branch: The branch to check
            into: The target branch (defaults to default branch)

        Returns:
            True if the branch can be merged without conflicts
        """
        if into is None:
            into = self.default_branch()

        if not self.branch_exists(branch) and not self.ref_exists(branch):
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

    def reset_hard_head(self) -> None:
        """Reset tracked files to HEAD, discarding local tracked changes.

        Used as a fallback cleanup path when merge abort is unavailable
        (for example, failed squash merges without MERGE_HEAD).
        """
        self.reset_hard("HEAD")

    def reset_hard(self, ref: str) -> None:
        """Reset tracked files to the given ref, discarding local tracked changes."""
        self._run("reset", "--hard", ref)

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
    worktree_path = active_worktree_path_for_branch(git, branch)

    remove_succeeded = False
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
        remove_result = git.worktree_remove(worktree_path, force=force)
        if remove_result.returncode != 0:
            error_output = remove_result.stderr or remove_result.stdout
            raise GitError(
                f"git worktree remove failed for branch '{branch}' at '{worktree_path}':\n{error_output}"
            )
        remove_succeeded = True

    registration_dir = _worktree_registration_dir_for_branch(git, branch)
    should_remove_registration = worktree_path is None or remove_succeeded
    if should_remove_registration and registration_dir is not None:
        try:
            shutil.rmtree(registration_dir)
        except OSError as exc:
            raise GitError(
                f"failed to remove stale worktree registration for branch '{branch}' at '{registration_dir}': {exc}"
            ) from exc

    still_registered = _registered_worktree_path_for_branch(git.worktree_list(), branch)
    if still_registered is not None:
        raise GitError(
            f"worktree for branch '{branch}' is still registered at '{still_registered}' after cleanup"
        )

    return worktree_path


def _branch_matches_worktree_ref(worktree_branch: str, branch: str) -> bool:
    """Return whether a worktree branch value matches a local branch name."""
    if not worktree_branch:
        return False
    return worktree_branch == branch or worktree_branch == f"refs/heads/{branch}"


def _registered_worktree_path_for_branch(worktrees: list[dict], branch: str) -> Path | None:
    """Return first registered worktree path for branch, including prunable entries."""
    for wt in worktrees:
        wt_branch = wt.get("branch", "")
        if isinstance(wt_branch, str) and _branch_matches_worktree_ref(wt_branch, branch):
            wt_path = wt.get("path")
            if isinstance(wt_path, str) and wt_path:
                return Path(wt_path)
    return None


def _worktree_registration_dir_for_branch(git: "Git", branch: str) -> Path | None:
    """Return the registration directory for a branch under ``.git/worktrees``."""
    worktrees_dir = _git_common_dir(git) / "worktrees"
    if not worktrees_dir.is_dir():
        return None

    for registration_dir in worktrees_dir.iterdir():
        if not registration_dir.is_dir():
            continue
        registration_branch = _worktree_registration_branch(registration_dir)
        if registration_branch and _branch_matches_worktree_ref(registration_branch, branch):
            return registration_dir
    return None


def _git_common_dir(git: "Git") -> Path:
    """Return the repository's common git directory."""
    result = git._run("rev-parse", "--git-common-dir")
    stdout = result.stdout if isinstance(result.stdout, str) else ""
    common_dir = stdout.strip()
    if not common_dir:
        repo_dir = git.repo_dir if isinstance(git.repo_dir, Path) else None
        if repo_dir is not None:
            return (repo_dir / ".git").resolve()
        return Path(".git").resolve()
    path = Path(common_dir)
    if path.is_absolute():
        return path
    return (git.repo_dir / path).resolve()


def _worktree_registration_branch(registration_dir: Path) -> str | None:
    """Return the branch ref recorded in a worktree registration directory."""
    head_path = registration_dir / "HEAD"
    try:
        head_content = head_path.read_text().strip()
    except OSError:
        return None

    ref_prefix = "ref: "
    if not head_content.startswith(ref_prefix):
        return None
    return head_content[len(ref_prefix):]


def active_worktree_path_for_branch(git: "Git", branch: str) -> Path | None:
    """Return active (non-prunable) worktree path for a branch, if any."""
    for wt in git.worktree_list():
        wt_branch = wt.get("branch", "")
        if not isinstance(wt_branch, str) or not _branch_matches_worktree_ref(wt_branch, branch):
            continue
        if wt.get("prunable"):
            continue
        wt_path = wt.get("path")
        if isinstance(wt_path, str) and wt_path:
            return Path(wt_path)
    return None
