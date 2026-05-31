"""GitHub operations for Gza."""

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path


class GitHubError(Exception):
    """GitHub operation failed."""
    pass


class GitHubLookupError(GitHubError):
    """GitHub PR lookup failed for reasons other than explicit not-found."""
    pass


class GitHubRepoUnsupportedError(GitHubLookupError):
    """gh cannot map the current repository to a known GitHub host."""
    pass


@dataclass
class PullRequest:
    """A GitHub pull request."""
    url: str
    number: int


@dataclass(frozen=True)
class PullRequestDetails:
    """Detailed GitHub pull request metadata."""

    url: str
    number: int
    state: str
    base_ref_name: str


class GitHub:
    """GitHub operations wrapper using gh CLI."""

    _PR_NOT_FOUND_MARKERS = (
        "could not resolve to a pull request",
        "no pull requests found",
        "pull request not found",
    )
    _NON_GITHUB_REMOTE_MARKERS = (
        "none of the git remotes configured for this repository point to a known github host",
    )
    _PR_SUPPORT_CACHE: dict[str, bool] = {}

    @classmethod
    def _cache_key(cls) -> str:
        return str(Path.cwd().resolve())

    @classmethod
    def cached_pr_support(cls) -> bool | None:
        """Return cached PR capability verdict for the current project, if any."""
        return cls._PR_SUPPORT_CACHE.get(cls._cache_key())

    @classmethod
    def _mark_pr_supported(cls) -> None:
        cls._PR_SUPPORT_CACHE[cls._cache_key()] = True

    @classmethod
    def _mark_pr_unsupported(cls) -> None:
        cls._PR_SUPPORT_CACHE[cls._cache_key()] = False

    @classmethod
    def clear_pr_support_cache(cls) -> None:
        """Reset cached PR capability verdicts. Used by tests."""
        cls._PR_SUPPORT_CACHE.clear()

    def _is_repo_unsupported(self, stderr: str) -> bool:
        message = str(stderr).lower()
        return any(marker in message for marker in self._NON_GITHUB_REMOTE_MARKERS)

    def _raise_repo_unsupported(self, command: str, stderr: str) -> None:
        self._mark_pr_unsupported()
        raise GitHubRepoUnsupportedError(f"{command} failed: {stderr.strip()}")

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a gh command."""
        result = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            command = f"gh {' '.join(args)}"
            if self._is_repo_unsupported(result.stderr):
                self._raise_repo_unsupported(command, result.stderr)
            raise GitHubError(f"gh {' '.join(args)} failed: {result.stderr}")
        return result

    def is_available(self) -> bool:
        """Check if gh CLI is available and authenticated."""
        try:
            result = self._run("auth", "status", check=False)
        except FileNotFoundError:
            return False
        return result.returncode == 0

    def create_pr(
        self,
        head: str,
        base: str,
        title: str,
        body: str,
        draft: bool = False,
    ) -> PullRequest:
        """Create a pull request.

        Args:
            head: The branch containing changes
            base: The branch to merge into
            title: PR title
            body: PR description (markdown)
            draft: Create as draft PR

        Returns:
            PullRequest with url and number
        """
        args = [
            "pr", "create",
            "--head", head,
            "--base", base,
            "--title", title,
            "--body", body,
        ]
        if draft:
            args.append("--draft")

        result = self._run(*args)
        self._mark_pr_supported()

        # gh pr create outputs the PR URL
        url = result.stdout.strip()

        # Extract PR number from URL (e.g., https://github.com/owner/repo/pull/123)
        try:
            number = int(url.rstrip("/").split("/")[-1])
        except (ValueError, IndexError):
            number = 0

        return PullRequest(url=url, number=number)

    def pr_exists(self, head: str) -> str | None:
        """Check if a PR already exists for a branch.

        Args:
            head: The branch to check

        Returns:
            PR URL if exists, None otherwise
        """
        result = self._run("pr", "view", head, "--json", "url", check=False)
        if result.returncode == 0:
            self._mark_pr_supported()
            data = json.loads(result.stdout)
            return data.get("url")
        if self._is_repo_unsupported(result.stderr):
            self._raise_repo_unsupported(f"gh pr view {head}", result.stderr)
        return None

    def get_pr_number(self, branch: str) -> int | None:
        """Get PR number for a branch, or None if no PR exists.

        Args:
            branch: Branch name to check

        Returns:
            PR number if PR exists, None otherwise
        """
        result = self._run("pr", "view", branch, "--json", "number", "-q", ".number", check=False)
        if result.returncode == 0 and result.stdout.strip():
            self._mark_pr_supported()
            try:
                return int(result.stdout.strip())
            except ValueError:
                return None
        if result.returncode != 0 and self._is_repo_unsupported(result.stderr):
            self._raise_repo_unsupported(f"gh pr view {branch}", result.stderr)
        return None

    def _is_pr_not_found(self, stderr: str) -> bool:
        """Return True when gh explicitly reported that a PR does not exist."""
        message = stderr.lower()
        return any(marker in message for marker in self._PR_NOT_FOUND_MARKERS)

    def _parse_pr_details(self, payload: str, *, command: str) -> PullRequestDetails:
        """Parse detailed PR metadata from a gh JSON payload."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GitHubLookupError(f"{command} returned invalid JSON: {exc}") from exc
        try:
            return PullRequestDetails(
                url=str(data["url"]),
                number=int(data["number"]),
                state=str(data["state"]).lower(),
                base_ref_name=str(data["baseRefName"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GitHubLookupError(f"{command} returned malformed PR details") from exc

    def get_pr_details(self, pr_ref: str | int) -> PullRequestDetails | None:
        """Return detailed PR metadata for a PR number/ref."""
        result = self._run(
            "pr",
            "view",
            str(pr_ref),
            "--json",
            "number,url,state,baseRefName",
            check=False,
        )
        command = f"gh pr view {pr_ref}"
        if result.returncode != 0:
            if self._is_repo_unsupported(result.stderr):
                self._raise_repo_unsupported(command, result.stderr)
            if self._is_pr_not_found(result.stderr):
                return None
            raise GitHubLookupError(f"{command} failed: {result.stderr.strip()}")
        if not result.stdout.strip():
            raise GitHubLookupError(f"{command} returned empty output")
        self._mark_pr_supported()
        return self._parse_pr_details(result.stdout, command=command)

    def discover_pr_by_branch(self, branch: str) -> PullRequestDetails | None:
        """Return the most recent PR associated with a branch, if any."""
        result = self._run(
            "pr",
            "list",
            "--head",
            branch,
            "--state",
            "all",
            "--limit",
            "1",
            "--json",
            "number,url,state,baseRefName",
            check=False,
        )
        command = f"gh pr list --head {branch}"
        if result.returncode != 0:
            if self._is_repo_unsupported(result.stderr):
                self._raise_repo_unsupported(command, result.stderr)
            raise GitHubLookupError(f"{command} failed: {result.stderr.strip()}")
        if not result.stdout.strip():
            raise GitHubLookupError(f"{command} returned empty output")
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise GitHubLookupError(f"{command} returned invalid JSON: {exc}") from exc
        if not data:
            return None
        if not isinstance(data, list):
            raise GitHubLookupError(f"{command} returned malformed PR list")
        item = data[0]
        if not isinstance(item, dict):
            raise GitHubLookupError(f"{command} returned malformed PR list")
        self._mark_pr_supported()
        return self._parse_pr_details(json.dumps(item), command=command)

    def get_pr_url(self, pr_ref: str | int) -> str | None:
        """Get PR URL for a PR number/ref, or None if no live PR exists."""
        details = self.get_pr_details(pr_ref)
        return details.url if details is not None else None

    def add_pr_comment(self, pr_number: int, body: str) -> None:
        """Add a comment to a PR.

        Args:
            pr_number: PR number
            body: Comment body (markdown)

        Raises:
            GitHubError: If comment fails
        """
        self._run("pr", "comment", str(pr_number), "--body", body)
        self._mark_pr_supported()

    def close_pr(self, pr_number: int) -> None:
        """Close a pull request without deleting its branch."""
        self._run("pr", "close", str(pr_number))
        self._mark_pr_supported()


def is_github_repo_unsupported_error(error: Exception | str) -> bool:
    """Return True when gh reported the current repo is not on a known GitHub host."""
    if isinstance(error, GitHubRepoUnsupportedError):
        return True
    return any(
        marker in str(error).lower()
        for marker in GitHub._NON_GITHUB_REMOTE_MARKERS
    )
