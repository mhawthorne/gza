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
