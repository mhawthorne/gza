"""Git operations for Gza."""

import logging
import os
import re
import shutil
import subprocess
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class GitError(Exception):
    """Git operation failed."""
    pass


class GitStatusError(GitError):
    """`git status --porcelain` failed, so tree cleanliness is unknown."""


def git_error_indicates_containerized_worktree_metadata_failure(error: BaseException | str) -> bool:
    """Return whether a git failure points at container-only `/gza-git` metadata.

    These failures are infrastructure/worktree-availability issues, not user-edit
    conflicts. They typically surface when host-side git bookkeeping touches a
    worktree whose admin files still reference the container-only `/gza-git/...`
    mount view.
    """
    text = str(error)
    return "/gza-git" in text


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
class GitWorktreeHealthProbe:
    """Cheap non-raising probe for shared git worktree health."""

    command: str
    returncode: int
    stdout: str
    stderr: str

    @property
    def failed(self) -> bool:
        """Return whether the probe detected a git-health failure."""
        return self.returncode != 0


@dataclass(frozen=True)
class WorktreeAdminMetadataIssue:
    """One host-side worktree admin metadata problem."""

    registration_name: str
    admin_file: str
    admin_path: Path
    value: str
    problem: str
    details: str
    expected_value: str | None = None
    suspected_container_path_marker: str | None = None


@dataclass(frozen=True)
class WorktreeAdminMetadataValidation:
    """Validation outcome for shared ``.git/worktrees`` admin metadata."""

    common_dir: Path
    issues: tuple[WorktreeAdminMetadataIssue, ...]

    @property
    def suspected_container_path_marker(self) -> str | None:
        """Return the first detected container-path marker, if any."""
        for issue in self.issues:
            if issue.suspected_container_path_marker:
                return issue.suspected_container_path_marker
        return None

    @property
    def is_healthy(self) -> bool:
        """Return whether the scanned admin metadata is free of known issues."""
        return not self.issues


@dataclass(frozen=True)
class ResolvedGitRef:
    """Best-effort ref resolution outcome for callers with different warning policy."""

    sha: str | None
    warning: str | None = None


@dataclass(frozen=True)
class ResolvedMergeSourceRef:
    """Freshest merge-source selection for advance and merge workflows."""

    ref: str | None
    warning: str | None = None


def prime_advance_planning_refs(
    git: Any | None,
    *,
    branch_names: Iterable[str],
    target_branch: str | None,
    warning_logger: logging.Logger | None = None,
) -> None:
    """Prime read-only ref facts for advance and lineage planning."""
    if git is None:
        return

    resolve_refs = getattr(git, "resolve_refs", None)
    branches_exist = getattr(git, "branches_exist", None)
    branches = tuple(dict.fromkeys(branch for branch in branch_names if branch))

    try:
        if callable(branches_exist) and branches:
            branches_exist(branches)
        if callable(resolve_refs):
            commit_refs = list(branches)
            commit_refs.extend(f"origin/{branch}" for branch in branches)
            if target_branch:
                commit_refs.append(target_branch)
            if commit_refs:
                resolve_refs(commit_refs, peel="commit")
            if target_branch:
                resolve_refs((target_branch,), peel="tree")
    except Exception:
        if warning_logger is not None:
            warning_logger.warning("Failed to preload advance planning refs", exc_info=True)


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


def is_rebase_in_progress(worktree_path: Path) -> bool:
    """Check whether git still reports an in-progress rebase for this checkout."""
    git_file = worktree_path / ".git"
    if git_file.is_file():
        try:
            git_dir_text = git_file.read_text().strip()
            if git_dir_text.startswith("gitdir: "):
                raw = git_dir_text[len("gitdir: "):]
                git_dir = Path(raw) if Path(raw).is_absolute() else (worktree_path / raw).resolve()
            else:
                git_dir = git_file
        except OSError:
            git_dir = worktree_path / ".git"
    else:
        git_dir = worktree_path / ".git"
    return (git_dir / "rebase-merge").exists() or (git_dir / "rebase-apply").exists()


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
        self._cache: dict[tuple[Any, ...], Any] | None = None

    @contextmanager
    def cached(self):
        """Enable a per-invocation cache for repeated read-only probes."""
        created_cache = self._cache is None
        if created_cache:
            self._cache = {}
        try:
            yield self
        finally:
            if created_cache:
                self._cache = None

    def clear_cache(self) -> None:
        """Drop the active per-invocation cache, if any."""
        if self._cache is not None:
            self._cache.clear()

    @staticmethod
    def _git_executable() -> str:
        """Resolve the real git binary instead of a provider shell shim.

        Functional tests and host-side maintenance flows create temporary repos
        outside `/workspace`. They should use the system git binary directly;
        the `/tmp/gza-shims/git` guard exists for provider shell sessions, not
        for this trusted wrapper.
        """
        filtered_path = ":".join(
            entry for entry in os.environ.get("PATH", "").split(":") if entry != "/tmp/gza-shims"
        )
        return shutil.which("git", path=filtered_path) or "git"

    def _cache_key(
        self,
        *args: str,
        check: bool,
        stdin: bytes | None = None,
    ) -> tuple[Any, ...]:
        return (args, check, ("stdin-id", id(stdin)) if stdin is not None else None)

    def _lookup_cached_value(self, key: tuple[Any, ...]) -> tuple[bool, Any]:
        if self._cache is None or key not in self._cache:
            return (False, None)
        return (True, self._cache[key])

    def _store_cached_value(self, key: tuple[Any, ...], value: Any) -> Any:
        if self._cache is not None:
            self._cache[key] = value
        return value

    def _resolved_ref_cache_key(self, ref: str, peel: str) -> tuple[Any, ...]:
        return ("resolved-ref", peel, ref)

    def _ref_exists_cache_key(self, ref: str) -> tuple[Any, ...]:
        return ("ref-exists", ref)

    def _branch_exists_cache_key(self, branch: str) -> tuple[Any, ...]:
        return ("branch-exists", branch)

    def _lookup_cached_resolved_ref(self, ref: str, peel: str) -> tuple[bool, str | None]:
        hit, cached = self._lookup_cached_value(self._resolved_ref_cache_key(ref, peel))
        return (hit, cached if isinstance(cached, str) or cached is None else None)

    def _store_cached_resolved_ref(self, ref: str, peel: str, sha: str | None) -> None:
        self._store_cached_value(self._resolved_ref_cache_key(ref, peel), sha)
        if peel == "commit":
            self._store_cached_value(self._ref_exists_cache_key(ref), sha is not None)

    def _lookup_cached_ref_exists(self, ref: str) -> tuple[bool, bool]:
        hit, cached = self._lookup_cached_value(self._ref_exists_cache_key(ref))
        return (hit, bool(cached))

    def _store_cached_ref_exists(self, ref: str, exists: bool) -> None:
        self._store_cached_value(self._ref_exists_cache_key(ref), exists)

    def _lookup_cached_branch_exists(self, branch: str) -> tuple[bool, bool]:
        hit, cached = self._lookup_cached_value(self._branch_exists_cache_key(branch))
        return (hit, bool(cached))

    def _store_cached_branch_exists(self, branch: str, exists: bool) -> None:
        self._store_cached_value(self._branch_exists_cache_key(branch), exists)

    @staticmethod
    def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
        return tuple(dict.fromkeys(value for value in values if value))

    @staticmethod
    def _format_batch_ref(ref: str, peel: str) -> str:
        return f"{ref}^{{{peel}}}"

    @staticmethod
    def _parse_batch_check_line(line: str) -> str | None:
        stripped = line.strip()
        if not stripped:
            raise GitError("git cat-file --batch-check returned an empty line")
        if stripped.endswith(" missing"):
            return None
        fields = stripped.split()
        if len(fields) >= 3 and re.fullmatch(r"[0-9a-fA-F]+", fields[0]):
            return fields[0]
        raise GitError(f"git cat-file --batch-check returned unexpected output: {line!r}")

    @contextmanager
    def _mutation_scope(self):
        """Clear cached read state around git mutations."""
        self.clear_cache()
        try:
            yield
        finally:
            self.clear_cache()

    def _run_readonly_cached(
        self,
        *args: str,
        check: bool = False,
        stdin: bytes | None = None,
    ) -> subprocess.CompletedProcess:
        key = self._cache_key(*args, check=check, stdin=stdin)
        hit, cached = self._lookup_cached_value(key)
        if hit:
            return cached
        result = self._run(*args, check=check) if stdin is None else self._run(*args, check=check, stdin=stdin)
        return self._store_cached_value(key, result)

    def _run_readonly_success_cached(
        self,
        *args: str,
        check: bool = True,
        stdin: bytes | None = None,
    ) -> subprocess.CompletedProcess:
        key = self._cache_key(*args, check=check, stdin=stdin)
        hit, cached = self._lookup_cached_value(key)
        if hit:
            return cached
        result = self._run(*args, check=check) if stdin is None else self._run(*args, check=check, stdin=stdin)
        return self._store_cached_value(key, result)

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
            [self._git_executable(), *args],
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
        with self._mutation_scope():
            self._run("checkout", branch)

    def checkout_detached(self, ref: str) -> None:
        """Checkout a detached HEAD at ``ref``."""
        with self._mutation_scope():
            self._run("checkout", "--detach", ref)

    def pull(self) -> bool:
        """Pull latest changes. Returns True if successful."""
        with self._mutation_scope():
            result = self._run("pull", "--ff-only", check=False)
        return result.returncode == 0

    def fetch(self, remote: str = "origin") -> None:
        """Fetch latest changes from remote.

        Args:
            remote: The remote to fetch from (default: origin)

        Raises:
            GitError: If the fetch fails
        """
        with self._mutation_scope():
            self._run("fetch", remote)

    def remote_exists(self, remote: str = "origin") -> bool:
        """Return True when a named git remote is configured."""
        result = self._run("remote", "get-url", remote, check=False)
        return result.returncode == 0

    def create_branch(self, branch: str, force: bool = False) -> None:
        """Create and checkout a new branch."""
        with self._mutation_scope():
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

    def has_unmerged_paths(self) -> bool:
        """Return whether the index currently contains unmerged paths."""
        result = self._run("diff", "--name-only", "--diff-filter=U", check=False)
        if result.returncode != 0:
            error_output = result.stderr or result.stdout
            raise GitError(f"git diff --name-only --diff-filter=U failed:\n{error_output}")
        return bool(result.stdout.strip())

    def status_porcelain(self) -> set[tuple[str, str]]:
        """Return set of (status, filepath) tuples from git status --porcelain.

        Each entry is a tuple like ('M', 'src/foo.py') or ('??', 'new_file.txt').
        The status codes follow git's porcelain format (M, A, D, ??, etc.).
        """
        result = self._run("status", "--porcelain", check=False)
        if result.returncode != 0:
            error_output = result.stderr.strip() or result.stdout.strip() or "unknown git status failure"
            raise GitStatusError(
                f"git status --porcelain failed with exit code {result.returncode}: {error_output}"
            )
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
        with self._mutation_scope():
            self._run("add", path)

    def commit(self, message: str) -> None:
        """Create a commit."""
        with self._mutation_scope():
            self._run("commit", "-m", message)

    def amend(self) -> None:
        """Amend the last commit with staged changes."""
        with self._mutation_scope():
            self._run("commit", "--amend", "--no-edit")

    def branch_exists(self, branch: str) -> bool:
        """Check if a branch exists locally."""
        hit, exists = self._lookup_cached_branch_exists(branch)
        if hit:
            return exists
        result = self._run_readonly_cached(
            "show-ref",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}",
            check=False,
        )
        exists = result.returncode == 0
        self._store_cached_branch_exists(branch, exists)
        return exists

    def local_branch_names(self) -> frozenset[str]:
        """Return all local branch names in one call."""
        result = self._run_readonly_success_cached(
            "for-each-ref",
            "--format=%(refname:strip=2)",
            "refs/heads/",
        )
        return frozenset(line.strip() for line in result.stdout.splitlines() if line.strip())

    def branches_exist(self, branches: Iterable[str]) -> dict[str, bool]:
        """Resolve local branch existence in one read-only batch."""
        requested = self._ordered_unique(branches)
        if not requested:
            return {}

        resolved: dict[str, bool] = {}
        unresolved: list[str] = []
        for branch in requested:
            hit, exists = self._lookup_cached_branch_exists(branch)
            if hit:
                resolved[branch] = exists
            else:
                unresolved.append(branch)

        if unresolved:
            existing = self.local_branch_names()
            for branch in unresolved:
                exists = branch in existing
                self._store_cached_branch_exists(branch, exists)
                resolved[branch] = exists

        return {branch: resolved[branch] for branch in requested}

    def worktree_add(self, path: Path, branch: str, base_branch: str | None = None) -> Path:
        """Create a new worktree with a new branch.

        Args:
            path: Directory where worktree will be created
            branch: Name of the new branch to create
            base_branch: Branch to base the new branch on (defaults to HEAD)

        Returns:
            The path to the created worktree
        """
        with self._mutation_scope():
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

    def worktree_add_existing(self, path: Path, ref: str, *, detach: bool = False) -> Path:
        """Create a worktree attached to an existing ref or detached at that ref."""
        with self._mutation_scope():
            path.parent.mkdir(parents=True, exist_ok=True)
            args = ["worktree", "add"]
            if detach:
                args.append("--detach")
            args.extend([str(path), ref])
            self._run(*args)
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
        with self._mutation_scope():
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

    def worktree_health_probe(self) -> GitWorktreeHealthProbe:
        """Run a cheap non-raising shared worktree health probe."""
        command = "git worktree list --porcelain"
        result = self._run("worktree", "list", "--porcelain", check=False)
        stdout = result.stdout if isinstance(result.stdout, str) else ""
        stderr = result.stderr if isinstance(result.stderr, str) else ""
        return GitWorktreeHealthProbe(
            command=command,
            returncode=result.returncode,
            stdout=stdout,
            stderr=stderr,
        )

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
        with self._mutation_scope():
            self._run(*args)

    def push_force_with_lease(self, branch: str, remote: str = "origin") -> None:
        """Force push a branch with lease protection.

        Args:
            branch: The branch to push
            remote: The remote name (default: origin)

        Raises:
            GitError: If the force push fails
        """
        with self._mutation_scope():
            self._run("push", "--force-with-lease", remote, branch)

    def push_ref_force_with_lease(
        self,
        source_ref: str,
        branch: str,
        *,
        remote: str = "origin",
        expected_remote_oid: str,
    ) -> None:
        """Force-push ``source_ref`` to ``branch`` with an explicit lease."""
        remote_branch_ref = f"refs/heads/{branch}"
        with self._mutation_scope():
            self._run(
                "push",
                f"--force-with-lease={remote_branch_ref}:{expected_remote_oid}",
                remote,
                f"{source_ref}:{remote_branch_ref}",
            )

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
        result = self.get_diff_numstat_result(revision_range, paths)
        if result.returncode != 0:
            return ""
        stdout = result.stdout if isinstance(result.stdout, str) else ""
        return stdout.strip()

    def get_diff_numstat_result(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
    ) -> subprocess.CompletedProcess:
        """Run ``git diff --numstat`` and return the raw subprocess result."""
        args = ["diff", "--numstat", "--find-renames", "--find-copies", "--find-copies-harder", revision_range]
        if paths:
            args.append("--")
            args.extend(paths)
        return self._run(*args, check=False)

    def get_diff_numstat_checked(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
    ) -> str:
        """Return ``git diff --numstat`` output or raise ``GitError`` on failure."""
        result = self.get_diff_numstat_result(revision_range, paths)
        if result.returncode != 0:
            error_output = result.stderr or result.stdout
            raise GitError(
                f"git diff --numstat --find-renames --find-copies --find-copies-harder "
                f"{revision_range} failed:\n{error_output}"
            )
        stdout = result.stdout if isinstance(result.stdout, str) else ""
        return stdout.strip()

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
        hit, exists = self._lookup_cached_ref_exists(ref)
        if hit:
            return exists
        resolved_hit, resolved_sha = self._lookup_cached_resolved_ref(ref, "commit")
        if resolved_hit:
            return resolved_sha is not None
        result = self._run_readonly_cached(
            "rev-parse",
            "--verify",
            "--quiet",
            f"{ref}^{{commit}}",
            check=False,
        )
        exists = result.returncode == 0
        self._store_cached_ref_exists(ref, exists)
        return exists

    def refs_exist(self, refs: Iterable[str]) -> dict[str, bool]:
        """Resolve commit-ref existence in one read-only batch."""
        requested = self._ordered_unique(refs)
        if not requested:
            return {}

        resolved: dict[str, bool] = {}
        unresolved: list[str] = []
        for ref in requested:
            hit, exists = self._lookup_cached_ref_exists(ref)
            if hit:
                resolved[ref] = exists
                continue
            resolved_hit, resolved_sha = self._lookup_cached_resolved_ref(ref, "commit")
            if resolved_hit:
                exists = resolved_sha is not None
                self._store_cached_ref_exists(ref, exists)
                resolved[ref] = exists
                continue
            unresolved.append(ref)

        if unresolved:
            batch_resolved = self.resolve_refs(unresolved, peel="commit")
            for ref, sha in batch_resolved.items():
                exists = sha is not None
                self._store_cached_ref_exists(ref, exists)
                resolved[ref] = exists

        return {ref: resolved[ref] for ref in requested}

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

    def resolve_fresh_merge_source(self, branch: str, *, remote: str = "origin") -> ResolvedMergeSourceRef:
        """Return the freshest safe ref for merge planning/execution.

        Compare the local branch and remote-tracking ref when both exist. Prefer
        ``<remote>/<branch>`` when it is equal to or ahead of the local branch,
        prefer the local branch when it is strictly ahead, and fail closed with
        a warning when the two refs have diverged.
        """
        remote_ref = f"{remote}/{branch}"
        local_exists = self.branch_exists(branch)
        remote_exists = self.ref_exists(remote_ref)

        if remote_exists and not local_exists:
            return ResolvedMergeSourceRef(remote_ref)
        if local_exists and not remote_exists:
            return ResolvedMergeSourceRef(branch)
        if not local_exists and not remote_exists:
            return ResolvedMergeSourceRef(None)

        local_sha = self.rev_parse_if_exists(branch)
        remote_sha = self.rev_parse_if_exists(remote_ref)
        if local_sha and remote_sha and local_sha == remote_sha:
            return ResolvedMergeSourceRef(remote_ref)

        local_ahead = self.count_commits_ahead(branch, remote_ref)
        remote_ahead = self.count_commits_ahead(remote_ref, branch)
        if remote_ahead > 0 and local_ahead == 0:
            return ResolvedMergeSourceRef(remote_ref)
        if local_ahead > 0 and remote_ahead == 0:
            return ResolvedMergeSourceRef(branch)
        if local_ahead > 0 and remote_ahead > 0:
            return ResolvedMergeSourceRef(
                None,
                (
                    f"Local branch '{branch}' and remote-tracking ref '{remote_ref}' diverged. "
                    "Push, fetch, or reconcile them before advancing or merging."
                ),
            )
        return ResolvedMergeSourceRef(
            None,
            (
                f"Unable to determine the freshest merge source between '{branch}' "
                f"and '{remote_ref}'. Reconcile the refs before advancing or merging."
            ),
        )

    def resolve_fresh_merge_source_ref(self, branch: str, *, remote: str = "origin") -> str | None:
        """Return the freshest merge source ref when it is unambiguous."""
        return self.resolve_fresh_merge_source(branch, remote=remote).ref

    def rev_parse(self, ref: str) -> str:
        """Resolve a ref to its commit SHA."""
        hit, sha = self._lookup_cached_resolved_ref(ref, "commit")
        if hit and sha is not None:
            return sha
        result = self._run_readonly_success_cached(
            "rev-parse",
            "--verify",
            f"{ref}^{{commit}}",
        )
        sha = result.stdout.strip()
        self._store_cached_resolved_ref(ref, "commit", sha)
        return sha

    def rev_parse_if_exists(self, ref: str) -> str | None:
        """Resolve a ref to its commit SHA when it exists."""
        hit, sha = self._lookup_cached_resolved_ref(ref, "commit")
        if hit:
            return sha
        result = self._run_readonly_cached(
            "rev-parse",
            "--verify",
            "--quiet",
            f"{ref}^{{commit}}",
            check=False,
        )
        if result.returncode != 0:
            self._store_cached_resolved_ref(ref, "commit", None)
            return None
        sha = result.stdout.strip()
        self._store_cached_resolved_ref(ref, "commit", sha)
        return sha

    def resolve_refs(self, refs: Iterable[str], peel: str = "commit") -> dict[str, str | None]:
        """Resolve refs to peeled object ids in one read-only batch."""
        if peel not in {"commit", "tree"}:
            raise ValueError("peel must be 'commit' or 'tree'")

        requested = self._ordered_unique(refs)
        if not requested:
            return {}

        resolved: dict[str, str | None] = {}
        unresolved: list[str] = []
        for ref in requested:
            hit, sha = self._lookup_cached_resolved_ref(ref, peel)
            if hit:
                resolved[ref] = sha
            else:
                unresolved.append(ref)

        if unresolved:
            stdin = "".join(f"{self._format_batch_ref(ref, peel)}\n" for ref in unresolved).encode()
            result = self._run("cat-file", "--batch-check", check=False, stdin=stdin)
            if result.returncode != 0:
                error_output = result.stderr or result.stdout
                raise GitError(f"git cat-file --batch-check failed:\n{error_output}")
            lines = result.stdout.splitlines()
            if len(lines) != len(unresolved):
                raise GitError(
                    "git cat-file --batch-check returned an unexpected number of lines"
                )
            for ref, line in zip(unresolved, lines, strict=True):
                sha = self._parse_batch_check_line(line)
                self._store_cached_resolved_ref(ref, peel, sha)
                resolved[ref] = sha

        return {ref: resolved[ref] for ref in requested}

    def merge_base(self, ref1: str, ref2: str) -> str:
        """Return the merge-base commit SHA for two refs."""
        result = self._run("merge-base", ref1, ref2)
        return result.stdout.strip()

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        """Return True when ``ancestor`` is reachable from ``descendant``."""
        result = self._run_readonly_cached(
            "merge-base",
            "--is-ancestor",
            ancestor,
            descendant,
            check=False,
        )
        if result.returncode == 0:
            return True
        if result.returncode == 1:
            return False
        error_output = result.stderr or result.stdout
        raise GitError(
            f"git merge-base --is-ancestor {ancestor} {descendant} failed:\n{error_output}"
        )

    def is_on_first_parent_history(self, commit: str, target: str) -> bool:
        """Return True when ``commit`` lies on ``target``'s first-parent mainline.

        Distinguishes a branch tip that is a plain mainline ancestor (carried no
        work of its own) from one that was merged in as a ``--no-ff`` side branch
        (a second parent, off the first-parent line). Both look identical to
        ``merge_base``/``count_commits_ahead`` once fully merged, so this is the
        signal that tells a stale empty branch apart from a genuinely merged one.
        """
        resolved = self._run_readonly_cached(
            "rev-parse", "--verify", "--quiet", f"{commit}^{{commit}}", check=False
        )
        if resolved.returncode != 0:
            return False
        commit_sha = resolved.stdout.strip()
        if not commit_sha:
            return False
        result = self._run_readonly_cached(
            "rev-list", "--first-parent", target, check=False
        )
        if result.returncode != 0:
            return False
        return commit_sha in result.stdout.split()

    def get_commit_subject(self, commit_ref: str) -> str:
        """Get the subject line for a single committed revision."""
        result = self._run("show", "-s", "--format=%s", commit_ref, check=False)
        return result.stdout.strip()

    def update_ref(self, ref: str, new_oid: str, old_oid: str | None = None) -> None:
        """Update a ref, optionally requiring the current value to match ``old_oid``."""
        args = ["update-ref", ref, new_oid]
        if old_oid is not None:
            args.append(old_oid)
        with self._mutation_scope():
            self._run(*args)

    def get_diff_name_status(
        self,
        revision_range: str,
        paths: tuple[str, ...] | list[str] = (),
        *,
        check: bool = False,
    ) -> str:
        """Get machine-readable name/status output for a revision range and optional paths."""
        args = ["diff", "--name-status", "--find-renames", "--find-copies", "--find-copies-harder", revision_range]
        if paths:
            args.append("--")
            args.extend(paths)
        result = self._run(*args, check=False)
        if check and result.returncode != 0:
            error_output = result.stderr or result.stdout
            raise GitError(f"git diff --name-status {revision_range} failed:\n{error_output}")
        return result.stdout.strip()

    def has_non_empty_source_diff_against_target(self, source_ref: str, target_ref: str) -> bool | None:
        """Return whether ``source_ref`` has a non-empty three-dot diff from ``target_ref``."""
        result = self._run_readonly_cached(
            "diff",
            "--quiet",
            f"{target_ref}...{source_ref}",
            check=False,
        )
        if result.returncode == 0:
            return False
        if result.returncode == 1:
            return True
        return None

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
        with self._mutation_scope():
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
            target_tree = self.resolve_refs((into,), peel="tree").get(into)
            if target_tree is None:
                return False
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
        with self._mutation_scope():
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
        with self._mutation_scope():
            self._run("merge", "--abort")

    def reset_hard_head(self) -> None:
        """Reset tracked files to HEAD, discarding local tracked changes.

        Used as a fallback cleanup path when merge abort is unavailable
        (for example, failed squash merges without MERGE_HEAD).
        """
        self.reset_hard("HEAD")

    def reset_hard(self, ref: str) -> None:
        """Reset tracked files to the given ref, discarding local tracked changes."""
        with self._mutation_scope():
            self._run("reset", "--hard", ref)

    def clean_force(self) -> None:
        """Remove untracked files and directories."""
        with self._mutation_scope():
            self._run("clean", "-fd")

    def stash_push(self, message: str) -> str | None:
        """Stash tracked changes and return the created stash ref, if any."""
        with self._mutation_scope():
            result = self._run("stash", "push", "--message", message, check=False)
            if result.returncode != 0:
                error_output = result.stderr or result.stdout
                raise GitError(f"git stash push failed:\n{error_output}")
            if "No local changes to save" in result.stdout:
                return None
            stash_ref = self._run("stash", "list", "-1", "--format=%gd").stdout.strip()
            if not stash_ref:
                raise GitError("git stash push succeeded but did not return a stash ref")
            return stash_ref

    def stash_pop_if_clean(self, stash_ref: str) -> bool:
        """Pop ``stash_ref`` only when it applies cleanly.

        Returns ``True`` when the stash was restored and dropped. On conflict,
        any partial apply is undone, the stash is left intact, and ``False`` is
        returned.
        """
        with self._mutation_scope():
            result = self._run("stash", "pop", "--index", stash_ref, check=False)
            if result.returncode == 0:
                return True
            error_output = result.stderr or result.stdout
            conflict_probe_error: GitError | None = None
            try:
                produced_conflicts = self.has_unmerged_paths()
            except GitError as probe_error:
                produced_conflicts = False
                conflict_probe_error = probe_error
            try:
                self._run("reset", "--hard", "HEAD")
            except GitError as reset_error:
                raise GitError(
                    "git stash pop failed and cleanup reset also failed:\n"
                    f"{error_output}\n{reset_error}"
                ) from reset_error
            if conflict_probe_error is not None:
                raise GitError(
                    "git stash pop failed and conflict detection also failed:\n"
                    f"{error_output}\n{conflict_probe_error}"
                ) from conflict_probe_error
            if produced_conflicts:
                return False
            raise GitError(f"git stash pop failed:\n{error_output}")

    def rebase(self, branch: str) -> None:
        """Rebase the current branch onto another branch.

        Args:
            branch: The branch to rebase onto

        Raises:
            GitError: If the rebase fails
        """
        with self._mutation_scope():
            self._run("rebase", branch)

    def rebase_abort(self) -> None:
        """Abort a rebase in progress and restore clean state.

        This is called after a failed rebase to clean up the working directory
        and return to the state before the rebase was attempted.

        Raises:
            GitError: If aborting the rebase fails
        """
        with self._mutation_scope():
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
        with self._mutation_scope():
            self._run(*args)

    def count_commits_ahead_checked(self, branch: str, base: str) -> int | None:
        """Count how many commits a branch is ahead of base.

        Returns ``None`` when git cannot prove the count.
        """
        result = self._run_readonly_cached(
            "rev-list",
            "--count",
            f"{base}..{branch}",
            check=False,
        )
        if result.returncode != 0:
            return None
        try:
            return int(result.stdout.strip())
        except ValueError:
            return None

    def count_commits_ahead(self, branch: str, base: str) -> int:
        """Count how many commits a branch is ahead of base.

        Args:
            branch: The branch to check
            base: The base branch to compare against

        Returns:
            Number of commits that branch is ahead of base
        """
        result = self.count_commits_ahead_checked(branch, base)
        if result is None:
            return 0
        return result

    def count_commits_behind(self, source_ref: str, target_ref: str) -> int | None:
        """Count commits reachable from ``target_ref`` and not ``source_ref``."""
        result = self._run("rev-list", "--count", f"{source_ref}..{target_ref}", check=False)
        if result.returncode != 0:
            return None
        try:
            return int(result.stdout.strip())
        except ValueError:
            return None


def _resolve_permitted_worktree_roots(permitted_root_paths: Sequence[Path] | None) -> tuple[Path, ...] | None:
    """Resolve configured worktree roots for containment checks."""
    if permitted_root_paths is None:
        return None
    return tuple(path.resolve(strict=False) for path in permitted_root_paths)


def _path_is_under_any_root(path: Path, roots: Sequence[Path]) -> bool:
    """Return whether ``path`` is equal to or contained by any root."""
    resolved_path = path.resolve(strict=False)
    for root in roots:
        try:
            resolved_path.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _format_foreign_worktree_error(branch: str, worktree_path: Path, roots: Sequence[Path]) -> str:
    """Build the refusal message for non-managed live worktrees."""
    roots_block = "\n".join(f"  {root}" for root in roots) or "  (none)"
    return (
        f"Refusing to remove worktree for branch '{branch}' at '{worktree_path}'\n"
        "because it is outside Gza-managed worktree roots:\n"
        f"{roots_block}\n\n"
        "Remove it manually if this is intentional:\n"
        f"  git worktree remove {worktree_path}\n"
        f"  git worktree remove --force {worktree_path}"
    )


def cleanup_worktree_for_branch(
    git: "Git",
    branch: str,
    force: bool = False,
    *,
    permitted_root_paths: Sequence[Path] | None = None,
) -> Path | None:
    """Clean up worktree if branch is checked out in one.

    Args:
        git: Git instance for the main repository
        branch: Branch name to check for worktree
        force: If True, remove worktree even with uncommitted changes
        permitted_root_paths: Allowed root directories for live worktree removal.

    Returns:
        Path to cleaned worktree, or None if no worktree found

    Raises:
        ValueError: If worktree has uncommitted changes and force=False
        GitError: If a live worktree is outside the permitted roots
    """
    worktree_path = active_worktree_path_for_branch(git, branch)
    permitted_roots = _resolve_permitted_worktree_roots(permitted_root_paths)

    remove_succeeded = False
    if worktree_path:
        resolved_worktree_path = worktree_path.resolve(strict=False)
        if permitted_roots is not None and not _path_is_under_any_root(resolved_worktree_path, permitted_roots):
            raise GitError(_format_foreign_worktree_error(branch, resolved_worktree_path, permitted_roots))

        # Check if worktree has uncommitted changes
        worktree_git = Git(resolved_worktree_path)
        if worktree_git.has_changes(include_untracked=True) and not force:
            raise ValueError(
                f"Worktree at {resolved_worktree_path} has uncommitted changes.\n"
                f"\nOptions:\n"
                f"  1. cd {resolved_worktree_path} and commit or discard changes\n"
                f"  2. Use --force to remove the worktree anyway (loses changes)"
            )

        # Remove the worktree
        remove_result = git.worktree_remove(resolved_worktree_path, force=force)
        if remove_result.returncode != 0:
            error_output = remove_result.stderr or remove_result.stdout
            raise GitError(
                f"git worktree remove failed for branch '{branch}' at '{resolved_worktree_path}':\n{error_output}"
            )
        remove_succeeded = True
        worktree_path = resolved_worktree_path

    registration_dir = _worktree_registration_dir_for_branch(git, branch)
    should_remove_registration = worktree_path is None or remove_succeeded
    if should_remove_registration and registration_dir is not None:
        _remove_worktree_registration_dir(
            registration_dir,
            context=f"branch '{branch}'",
        )

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


def _worktree_registration_dir_for_path(git: "Git", path: Path) -> Path | None:
    """Return the registration directory for a worktree path under ``.git/worktrees``."""
    worktrees_dir = _git_common_dir(git) / "worktrees"
    if not worktrees_dir.is_dir():
        return None

    expected_gitdir = (path / ".git").resolve(strict=False)
    for registration_dir in worktrees_dir.iterdir():
        if not registration_dir.is_dir():
            continue
        registration_gitdir = _worktree_registration_gitdir(registration_dir)
        if registration_gitdir is not None and registration_gitdir == expected_gitdir:
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


def _worktree_registration_gitdir(registration_dir: Path) -> Path | None:
    """Return the worktree ``.git`` path recorded in a registration directory."""
    gitdir_path = registration_dir / "gitdir"
    try:
        gitdir_content = gitdir_path.read_text().strip()
    except OSError:
        return None
    if not gitdir_content:
        return None

    recorded_path = Path(gitdir_content)
    if not recorded_path.is_absolute():
        recorded_path = (registration_dir / recorded_path).resolve(strict=False)
    else:
        recorded_path = recorded_path.resolve(strict=False)
    return recorded_path


def _read_worktree_admin_file(path: Path) -> str | None:
    """Return stripped admin-file content when readable."""
    try:
        content = path.read_text().strip()
    except OSError:
        return None
    return content or None


def _known_container_git_root_marker(value: str) -> str | None:
    """Return the known container-only git root marker embedded in ``value``."""
    return "/gza-git" if "/gza-git" in value else None


def validate_host_worktree_admin_metadata(git: "Git") -> WorktreeAdminMetadataValidation:
    """Inspect shared ``.git/worktrees`` admin files for host-invalid metadata."""
    common_dir = _git_common_dir(git)
    worktrees_dir = common_dir / "worktrees"
    if not worktrees_dir.is_dir():
        return WorktreeAdminMetadataValidation(common_dir=common_dir, issues=())

    issues: list[WorktreeAdminMetadataIssue] = []
    for registration_dir in sorted(worktrees_dir.iterdir()):
        if not registration_dir.is_dir():
            continue
        registration_name = registration_dir.name
        for admin_file, expected_value in (("commondir", "../.."), ("gitdir", None)):
            admin_path = registration_dir / admin_file
            value = _read_worktree_admin_file(admin_path)
            if value is None:
                continue
            marker = _known_container_git_root_marker(value)
            if marker is None:
                continue
            if admin_file == "commondir":
                problem = "containerized-commondir"
                details = (
                    f"{admin_path} contains container-only path '{value}'; "
                    "host commondir metadata should stay relative to the canonical repository."
                )
            else:
                problem = "containerized-gitdir"
                details = (
                    f"{admin_path} contains container-only path '{value}'; "
                    "host gitdir metadata must not point at container-only mounts."
                )
            issues.append(
                WorktreeAdminMetadataIssue(
                    registration_name=registration_name,
                    admin_file=admin_file,
                    admin_path=admin_path,
                    value=value,
                    problem=problem,
                    details=details,
                    expected_value=expected_value,
                    suspected_container_path_marker=marker,
                )
            )

    return WorktreeAdminMetadataValidation(common_dir=common_dir, issues=tuple(issues))


def remove_worktree_registration_for_path(git: "Git", path: Path) -> Path | None:
    """Remove the stale registration directory for one worktree path, if present."""
    registration_dir = _worktree_registration_dir_for_path(git, path)
    if registration_dir is None:
        return None
    _remove_worktree_registration_dir(
        registration_dir,
        context=f"worktree '{path.resolve(strict=False)}'",
    )
    return registration_dir


def _remove_worktree_registration_dir(registration_dir: Path, *, context: str) -> None:
    """Remove one worktree registration directory and wrap filesystem errors."""
    try:
        shutil.rmtree(registration_dir)
    except OSError as exc:
        raise GitError(
            f"failed to remove stale worktree registration for {context} at '{registration_dir}': {exc}"
        ) from exc


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
