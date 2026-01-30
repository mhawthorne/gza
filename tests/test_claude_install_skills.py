"""Tests for the claude-install-skills command."""

import subprocess
from pathlib import Path

import pytest


def run_gza(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    """Run gza command and return result."""
    return subprocess.run(
        ["uv", "run", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def setup_config(tmp_path: Path) -> None:
    """Set up a minimal gza config file."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text("project_name: test-project\n")


class TestClaudeInstallSkillsCommand:
    """Tests for 'gza claude-install-skills' command."""

    def test_list_available_skills(self, tmp_path: Path):
        """List command shows all available skills."""
        setup_config(tmp_path)
        result = run_gza("claude-install-skills", "--list", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Available skills:" in result.stdout
        assert "gza-task-add" in result.stdout
        assert "gza-task-info" in result.stdout
        assert "rebase" in result.stdout
        assert "review-docs" in result.stdout

    def test_install_all_skills(self, tmp_path: Path):
        """Install all skills to a project."""
        setup_config(tmp_path)
        result = run_gza("claude-install-skills", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing" in result.stdout
        assert "Installed" in result.stdout

        # Verify skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert skills_dir.exists()
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert (skills_dir / "gza-task-info" / "SKILL.md").exists()
        assert (skills_dir / "rebase" / "SKILL.md").exists()
        # review-docs uses lowercase skill.md
        assert (skills_dir / "review-docs" / "skill.md").exists()

    def test_install_specific_skills(self, tmp_path: Path):
        """Install only specific skills."""
        setup_config(tmp_path)
        result = run_gza("claude-install-skills", "gza-task-add", "rebase", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing 2 skill(s)" in result.stdout
        assert "Installed 2 skill(s)" in result.stdout

        # Verify only requested skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert (skills_dir / "rebase" / "SKILL.md").exists()
        assert not (skills_dir / "gza-task-info").exists()
        assert not (skills_dir / "review-docs").exists()

    def test_skip_existing_skills_without_force(self, tmp_path: Path):
        """Existing skills are skipped without --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = run_gza("claude-install-skills", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Try to install again without --force
        result2 = run_gza("claude-install-skills", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" in result2.stdout
        assert "already exists" in result2.stdout

    def test_overwrite_with_force_flag(self, tmp_path: Path):
        """Existing skills are overwritten with --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = run_gza("claude-install-skills", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Modify one of the skills
        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text("Modified content")

        # Install again with --force
        result2 = run_gza("claude-install-skills", "--force", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" not in result2.stdout

        # Verify skill was overwritten
        assert skill_file.read_text() == original_content

    def test_install_nonexistent_skill(self, tmp_path: Path):
        """Error when requesting a skill that doesn't exist."""
        setup_config(tmp_path)
        result = run_gza("claude-install-skills", "nonexistent-skill", "--project", str(tmp_path))

        assert result.returncode == 1
        assert "Error: Skill 'nonexistent-skill' not found" in result.stdout
        assert "Available skills:" in result.stdout

    def test_create_target_directory(self, tmp_path: Path):
        """Target directory is created if it doesn't exist."""
        setup_config(tmp_path)

        # Ensure .claude/skills doesn't exist
        skills_dir = tmp_path / ".claude" / "skills"
        assert not skills_dir.exists()

        result = run_gza("claude-install-skills", "--project", str(tmp_path))
        assert result.returncode == 0
        assert skills_dir.exists()

    def test_install_from_different_directory(self, tmp_path: Path):
        """Skills can be installed to a different project directory."""
        project_dir = tmp_path / "my-project"
        project_dir.mkdir()
        setup_config(project_dir)

        # Run from tmp_path but target project_dir
        result = run_gza("claude-install-skills", "--project", str(project_dir), cwd=tmp_path)
        assert result.returncode == 0

        # Verify skills were created in project_dir
        skills_dir = project_dir / ".claude" / "skills"
        assert skills_dir.exists()
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
