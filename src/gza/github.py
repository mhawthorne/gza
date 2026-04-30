"""GitHub operations for Gza."""

import json
import subprocess
from dataclasses import dataclass


class GitHubError(Exception):
    """GitHub operation failed."""
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

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a gh command."""
        result = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            raise GitHubError(f"gh {' '.join(args)} failed: {result.stderr}")
        return result

    def is_available(self) -> bool:
        """Check if gh CLI is available and authenticated."""
        result = self._run("auth", "status", check=False)
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
            data = json.loads(result.stdout)
            return data.get("url")
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
            try:
                return int(result.stdout.strip())
            except ValueError:
                return None
        return None

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
        if result.returncode != 0 or not result.stdout.strip():
            return None
        data = json.loads(result.stdout)
        try:
            return PullRequestDetails(
                url=str(data["url"]),
                number=int(data["number"]),
                state=str(data["state"]).lower(),
                base_ref_name=str(data["baseRefName"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

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
        if result.returncode != 0 or not result.stdout.strip():
            return None
        data = json.loads(result.stdout)
        if not data:
            return None
        item = data[0]
        try:
            return PullRequestDetails(
                url=str(item["url"]),
                number=int(item["number"]),
                state=str(item["state"]).lower(),
                base_ref_name=str(item["baseRefName"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

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

    def close_pr(self, pr_number: int) -> None:
        """Close a pull request without deleting its branch."""
        self._run("pr", "close", str(pr_number))
