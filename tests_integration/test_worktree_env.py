"""Integration tests for worktree environment setup.

Verifies that the worktree environment has the files and structure
that agents expect at runtime. These tests create real git worktrees
and run the same setup steps as the runner.

Run with: uv run pytest tests_integration/test_worktree_env.py -v -m integration
"""

import subprocess
from pathlib import Path

import pytest

from gza.git import Git

pytestmark = pytest.mark.integration


def _init_git_repo(path: Path) -> None:
    """Initialize a minimal git repo with one commit."""
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "config", "user.email", "test@test.com"],
        capture_output=True, check=True,
    )
    subprocess.run(
        ["git", "-C", str(path), "config", "user.name", "Test"],
        capture_output=True, check=True,
    )
    (path / "README.md").write_text("# Test")
    subprocess.run(["git", "-C", str(path), "add", "."], capture_output=True, check=True)
    subprocess.run(
        ["git", "-C", str(path), "commit", "-m", "init"],
        capture_output=True, check=True,
    )


class TestWorktreeEnvironment:
    """Verify that worktrees are set up with the files agents need."""

    def test_learnings_file_available_in_worktree(self, tmp_path: Path):
        """Agent should be able to read .gza/learnings.md in its worktree."""
        from unittest.mock import Mock
        from gza.config import Config
        from gza.git import Git
        from gza.runner import _copy_learnings_to_worktree

        # Set up a real git repo with a .gza/learnings.md
        project_dir = tmp_path / "project"
        _init_git_repo(project_dir)

        gza_dir = project_dir / ".gza"
        gza_dir.mkdir()
        learnings_content = "- Always run mypy before committing\n- Use tmp_path fixtures\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        # Create a real worktree
        worktree_dir = tmp_path / "worktrees" / "task-1"
        git = Git(project_dir)
        git.worktree_add(worktree_dir, "test-branch")

        # Verify .gza/ is NOT in the worktree by default (gitignored)
        assert not (worktree_dir / ".gza" / "learnings.md").exists()

        # Run the same setup the runner does
        config = Mock(spec=Config)
        config.project_dir = project_dir
        _copy_learnings_to_worktree(config, worktree_dir)

        # Now the agent can find it
        assert (worktree_dir / ".gza" / "learnings.md").exists()
        assert (worktree_dir / ".gza" / "learnings.md").read_text() == learnings_content

    def test_skills_available_in_worktree(self, tmp_path: Path):
        """Agent should have bundled skills installed in its worktree."""
        from gza.skills_utils import ensure_all_skills

        # Set up a real git repo and worktree
        project_dir = tmp_path / "project"
        _init_git_repo(project_dir)

        worktree_dir = tmp_path / "worktrees" / "task-1"
        git = Git(project_dir)
        git.worktree_add(worktree_dir, "test-branch")

        # Run skill installation (same as runner does)
        skills_dir = worktree_dir / ".claude" / "skills"
        n_installed = ensure_all_skills(skills_dir)

        assert n_installed > 0
        assert skills_dir.is_dir()
        # At minimum, gza-rebase should be installed
        assert any(
            p.name == "gza-rebase" for p in skills_dir.iterdir() if p.is_dir()
        ), f"Expected gza-rebase skill, found: {list(skills_dir.iterdir())}"

    def test_summary_dir_exists_in_worktree(self, tmp_path: Path):
        """Agent should have a summary directory to write to."""
        from gza.runner import SUMMARY_DIR

        project_dir = tmp_path / "project"
        _init_git_repo(project_dir)

        worktree_dir = tmp_path / "worktrees" / "task-1"
        git = Git(project_dir)
        git.worktree_add(worktree_dir, "test-branch")

        # Reproduce what the runner does
        summary_dir = project_dir / SUMMARY_DIR
        summary_dir.mkdir(parents=True, exist_ok=True)
        worktree_summary_dir = worktree_dir / SUMMARY_DIR
        worktree_summary_dir.mkdir(parents=True, exist_ok=True)

        assert worktree_summary_dir.is_dir()

    def test_git_tracked_docs_available(self, tmp_path: Path):
        """Git-tracked files like docs/internal/ should be in the worktree."""
        project_dir = tmp_path / "project"
        _init_git_repo(project_dir)

        # Add a docs/internal file and commit it
        docs_dir = project_dir / "docs" / "internal"
        docs_dir.mkdir(parents=True)
        (docs_dir / "test.md").write_text("# Internal doc")
        subprocess.run(
            ["git", "-C", str(project_dir), "add", "."],
            capture_output=True, check=True,
        )
        subprocess.run(
            ["git", "-C", str(project_dir), "commit", "-m", "add docs"],
            capture_output=True, check=True,
        )

        worktree_dir = tmp_path / "worktrees" / "task-1"
        git = Git(project_dir)
        git.worktree_add(worktree_dir, "test-branch")

        # Git-tracked files are automatically in the worktree
        assert (worktree_dir / "docs" / "internal" / "test.md").exists()
