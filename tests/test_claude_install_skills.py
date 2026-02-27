"""Tests for the skills-install command."""

import os
import subprocess
from pathlib import Path

import pytest
import yaml


def run_gza(
    *args: str,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Run gza command and return result."""
    run_env = os.environ.copy()
    if env:
        run_env.update(env)
    return subprocess.run(
        ["uv", "run", "gza", *args],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=run_env,
    )


def setup_config(tmp_path: Path) -> None:
    """Set up a minimal gza config file."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text("project_name: test-project\n")


class TestSkillsInstallClaudeTarget:
    """Tests for `gza skills-install --target claude` behavior."""

    def test_list_available_skills(self, tmp_path: Path):
        """List command shows all available skills."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "--list", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Available skills:" in result.stdout
        assert "gza-task-add" in result.stdout
        assert "gza-task-info" in result.stdout
        assert "gza-plan-review" in result.stdout

    def test_install_all_skills(self, tmp_path: Path):
        """Install all skills to a project."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing" in result.stdout
        assert "Installed" in result.stdout

        # Verify skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert skills_dir.exists()
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert (skills_dir / "gza-task-info" / "SKILL.md").exists()
        assert (skills_dir / "gza-plan-review" / "SKILL.md").exists()

    def test_install_specific_skills(self, tmp_path: Path):
        """Install only specific skills."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "gza-task-add", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing 1 skill(s)" in result.stdout
        assert "Installed 1 skill(s)" in result.stdout

        # Verify only requested skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert not (skills_dir / "gza-task-info").exists()

    def test_skip_existing_skills_without_force(self, tmp_path: Path):
        """Existing skills are skipped without --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Try to install again without --force
        result2 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" in result2.stdout
        assert "already exists" in result2.stdout

    def test_overwrite_with_force_flag(self, tmp_path: Path):
        """Existing skills are overwritten with --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Modify one of the skills
        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text("Modified content")

        # Install again with --force
        result2 = run_gza("skills-install", "--target", "claude", "--force", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" not in result2.stdout

        # Verify skill was overwritten
        assert skill_file.read_text() == original_content

    def test_install_nonexistent_skill(self, tmp_path: Path):
        """Error when requesting a skill that doesn't exist."""
        setup_config(tmp_path)
        result = run_gza(
            "skills-install", "--target", "claude", "nonexistent-skill", "--project", str(tmp_path)
        )

        assert result.returncode == 1
        assert "Error: Skill 'nonexistent-skill' not found" in result.stdout
        assert "Available skills:" in result.stdout

    def test_create_target_directory(self, tmp_path: Path):
        """Target directory is created if it doesn't exist."""
        setup_config(tmp_path)

        # Ensure .claude/skills doesn't exist
        skills_dir = tmp_path / ".claude" / "skills"
        assert not skills_dir.exists()

        result = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result.returncode == 0
        assert skills_dir.exists()

    def test_install_from_different_directory(self, tmp_path: Path):
        """Skills can be installed to a different project directory."""
        project_dir = tmp_path / "my-project"
        project_dir.mkdir()
        setup_config(project_dir)

        # Run from tmp_path but target project_dir
        result = run_gza("skills-install", "--target", "claude", "--project", str(project_dir), cwd=tmp_path)
        assert result.returncode == 0

        # Verify skills were created in project_dir
        skills_dir = project_dir / ".claude" / "skills"
        assert skills_dir.exists()
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()


class TestSkillsInstallCommand:
    """Tests for 'gza skills-install' command."""

    def test_list_available_skills(self, tmp_path: Path):
        """List command works via flat skills-install command."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--list", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Available skills:" in result.stdout
        assert "gza-task-add" in result.stdout
        assert "gza-plan-review" in result.stdout

    def test_install_all_targets_by_default(self, tmp_path: Path):
        """skills-install defaults to both claude and codex targets."""
        setup_config(tmp_path)
        codex_home = tmp_path / "codex-home"
        result = run_gza("skills-install", "--project", str(tmp_path), env={"CODEX_HOME": str(codex_home)})

        assert result.returncode == 0
        assert "[claude]" in result.stdout
        assert "[codex]" in result.stdout

        claude_skills_dir = tmp_path / ".claude" / "skills"
        codex_skills_dir = codex_home / "skills"
        assert claude_skills_dir.exists()
        assert codex_skills_dir.exists()
        assert (claude_skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert (codex_skills_dir / "gza-task-add" / "SKILL.md").exists()

    def test_install_only_codex_target(self, tmp_path: Path):
        """Target filter installs only to the requested runtime directory."""
        setup_config(tmp_path)
        codex_home = tmp_path / "codex-home"
        result = run_gza(
            "skills-install",
            "--target",
            "codex",
            "--project",
            str(tmp_path),
            env={"CODEX_HOME": str(codex_home)},
        )

        assert result.returncode == 0
        assert "[codex]" in result.stdout
        assert "[claude]" not in result.stdout

        assert (codex_home / "skills" / "gza-task-add" / "SKILL.md").exists()
        assert not (tmp_path / ".claude" / "skills").exists()

    def test_install_only_gemini_target(self, tmp_path: Path):
        """Gemini target installs skills into GEMINI_HOME runtime directory."""
        setup_config(tmp_path)
        gemini_home = tmp_path / "gemini-home"
        result = run_gza(
            "skills-install",
            "--target",
            "gemini",
            "--project",
            str(tmp_path),
            env={"GEMINI_HOME": str(gemini_home)},
        )

        assert result.returncode == 0
        assert "[gemini]" in result.stdout
        assert "[claude]" not in result.stdout
        assert "[codex]" not in result.stdout
        assert (gemini_home / "skills" / "gza-task-add" / "SKILL.md").exists()


class TestSkillContentValidation:
    """Tests to validate skill content format and structure."""

    def test_all_skills_use_uppercase_skill_md(self):
        """All skills must use SKILL.md (uppercase) naming convention."""
        from gza.skills_utils import get_skills_source_path

        skills_path = get_skills_source_path()
        if not skills_path.exists():
            pytest.skip("Skills directory not found")

        for skill_dir in skills_path.iterdir():
            if not skill_dir.is_dir():
                continue

            skill_file = skill_dir / "SKILL.md"
            lowercase_file = skill_dir / "skill.md"

            # Check that SKILL.md exists
            assert skill_file.exists(), f"Skill {skill_dir.name} must have SKILL.md (uppercase)"

            # Check that skill.md (lowercase) does not exist (unless on case-insensitive filesystem)
            # On case-insensitive filesystems, SKILL.md and skill.md are the same file
            if lowercase_file.exists() and lowercase_file.samefile(skill_file):
                continue  # Same file, OK
            assert not lowercase_file.exists(), f"Skill {skill_dir.name} should not have lowercase skill.md"

    def test_all_skills_have_valid_yaml_frontmatter(self):
        """All skills must have valid YAML frontmatter."""
        from gza.skills_utils import get_skills_source_path, get_available_skills

        skills_path = get_skills_source_path()
        available_skills = get_available_skills()

        for skill_name in available_skills:
            skill_file = skills_path / skill_name / "SKILL.md"
            content = skill_file.read_text()

            # Check it starts with frontmatter
            assert content.startswith("---"), f"Skill {skill_name} must start with YAML frontmatter"

            # Extract frontmatter
            lines = content.split("\n")
            frontmatter_lines = []
            in_frontmatter = False
            for i, line in enumerate(lines):
                if i == 0 and line.strip() == "---":
                    in_frontmatter = True
                    continue
                if in_frontmatter:
                    if line.strip() == "---":
                        break
                    frontmatter_lines.append(line)

            frontmatter_text = "\n".join(frontmatter_lines)

            # Parse as YAML
            try:
                frontmatter = yaml.safe_load(frontmatter_text)
            except yaml.YAMLError as e:
                pytest.fail(f"Skill {skill_name} has invalid YAML frontmatter: {e}")

            assert isinstance(frontmatter, dict), f"Skill {skill_name} frontmatter must be a dictionary"

    def test_all_skills_have_required_fields(self):
        """All skills must have required frontmatter fields."""
        from gza.skills_utils import get_skills_source_path, get_available_skills

        skills_path = get_skills_source_path()
        available_skills = get_available_skills()
        required_fields = ["name", "description", "allowed-tools", "version"]

        for skill_name in available_skills:
            skill_file = skills_path / skill_name / "SKILL.md"
            content = skill_file.read_text()

            # Extract and parse frontmatter
            lines = content.split("\n")
            frontmatter_lines = []
            in_frontmatter = False
            for i, line in enumerate(lines):
                if i == 0 and line.strip() == "---":
                    in_frontmatter = True
                    continue
                if in_frontmatter:
                    if line.strip() == "---":
                        break
                    frontmatter_lines.append(line)

            frontmatter_text = "\n".join(frontmatter_lines)
            frontmatter = yaml.safe_load(frontmatter_text)

            # Check required fields
            for field in required_fields:
                assert field in frontmatter, f"Skill {skill_name} missing required field '{field}'"
                assert frontmatter[field], f"Skill {skill_name} has empty '{field}' field"

    def test_all_skills_have_valid_allowed_tools_format(self):
        """All skills must have properly formatted allowed-tools field."""
        from gza.skills_utils import get_skills_source_path, get_available_skills

        skills_path = get_skills_source_path()
        available_skills = get_available_skills()

        for skill_name in available_skills:
            skill_file = skills_path / skill_name / "SKILL.md"
            content = skill_file.read_text()

            # Extract and parse frontmatter
            lines = content.split("\n")
            frontmatter_lines = []
            in_frontmatter = False
            for i, line in enumerate(lines):
                if i == 0 and line.strip() == "---":
                    in_frontmatter = True
                    continue
                if in_frontmatter:
                    if line.strip() == "---":
                        break
                    frontmatter_lines.append(line)

            frontmatter_text = "\n".join(frontmatter_lines)
            frontmatter = yaml.safe_load(frontmatter_text)

            # Check allowed-tools format
            allowed_tools = frontmatter.get("allowed-tools")
            assert allowed_tools, f"Skill {skill_name} missing allowed-tools"

            # Should be a string (comma-separated list of tools)
            # e.g., "Read, Glob, Grep, Bash(git:*)"
            assert isinstance(allowed_tools, str), f"Skill {skill_name} allowed-tools must be a string"
            assert len(allowed_tools.strip()) > 0, f"Skill {skill_name} allowed-tools cannot be empty"

            # Basic format check: should contain at least one tool name
            # Valid tool names include: Read, Write, Edit, Glob, Grep, Bash, etc.
            valid_tool_pattern = any(
                tool in allowed_tools
                for tool in ["Read", "Write", "Edit", "Glob", "Grep", "Bash", "AskUserQuestion", "WebFetch"]
            )
            assert valid_tool_pattern, f"Skill {skill_name} allowed-tools should contain valid tool names"

    def test_all_skills_have_semantic_versions(self):
        """All skills must have semantic version numbers."""
        from gza.skills_utils import get_skills_source_path, get_available_skills
        import re

        skills_path = get_skills_source_path()
        available_skills = get_available_skills()

        # Semantic versioning pattern: MAJOR.MINOR.PATCH
        semver_pattern = re.compile(r"^\d+\.\d+\.\d+$")

        for skill_name in available_skills:
            skill_file = skills_path / skill_name / "SKILL.md"
            content = skill_file.read_text()

            # Extract and parse frontmatter
            lines = content.split("\n")
            frontmatter_lines = []
            in_frontmatter = False
            for i, line in enumerate(lines):
                if i == 0 and line.strip() == "---":
                    in_frontmatter = True
                    continue
                if in_frontmatter:
                    if line.strip() == "---":
                        break
                    frontmatter_lines.append(line)

            frontmatter_text = "\n".join(frontmatter_lines)
            frontmatter = yaml.safe_load(frontmatter_text)

            version = frontmatter.get("version")
            assert version, f"Skill {skill_name} missing version field"
            assert isinstance(version, str), f"Skill {skill_name} version must be a string"
            assert semver_pattern.match(version), f"Skill {skill_name} version '{version}' must follow semantic versioning (e.g., 1.0.0)"
