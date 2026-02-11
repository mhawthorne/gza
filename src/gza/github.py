"""GitHub operations for Gza."""

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
            import json
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

    def add_pr_comment(self, pr_number: int, body: str) -> None:
        """Add a comment to a PR.

        Args:
            pr_number: PR number
            body: Comment body (markdown)

        Raises:
            GitHubError: If comment fails
        """
        self._run("pr", "comment", str(pr_number), "--body", body)
