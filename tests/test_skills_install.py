"""Tests for the skills-install command."""

import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from tests.helpers.cli import invoke_gza


def setup_config(tmp_path: Path) -> None:
    """Set up a minimal gza config file."""
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        "project_name: test-project\n"
        "db_path: .gza/gza.db\n"
    )


def setup_shared_db_config(tmp_path: Path) -> Path:
    """Set up a shared-DB config that requires project-bound store resolution."""
    shared_db = tmp_path / "shared" / "gza.db"
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        "project_name: test-project\n"
        "project_id: testproject\n"
        "project_prefix: testproject\n"
        f"db_path: {shared_db}\n"
    )
    return shared_db


def _create_store_for_project(tmp_path: Path):
    from gza.config import Config
    from gza.db import SqliteTaskStore

    config = Config.load(tmp_path)
    store = SqliteTaskStore.from_config(config)
    return config, store


def _assign_slug_like_runner(task, store, config, *, git=None) -> None:
    from gza.git import Git
    from gza.runner import _compute_slug_override, generate_slug

    if task.slug is not None:
        return
    if git is None:
        git = Git(config.project_dir)
    slug_override = _compute_slug_override(task, store)
    task.slug = generate_slug(
        task.prompt,
        existing_id=None,
        log_path=config.log_path,
        git=git,
        store=store,
        exclude_task_id=task.id,
        project_name=config.project_name,
        project_prefix=config.project_prefix,
        slug_override=slug_override,
        branch_strategy=config.branch_strategy,
        explicit_type=task.task_type_hint,
    )
    store.update(task)


def _persist_manual_skill_output(
    created,
    store,
    config,
    *,
    content_with_origin: str,
    output_body: str,
    path_kind: str,
    clear_review_task_id: str | None = None,
    resolve_comments_task_id: str | None = None,
):
    from gza.runner import get_task_output_paths

    try:
        report_path, summary_path = get_task_output_paths(created, config.project_dir)
        output_path = summary_path if path_kind == "summary" else report_path
        assert output_path is not None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content_with_origin)

        created.report_file = str(output_path.relative_to(config.project_dir))
        created.status = "completed"
        created.completed_at = datetime.now(UTC)
        created.output_content = output_body
        store.update(created)
    except Exception:
        created.status = "dropped"
        store.update(created)
        raise

    if clear_review_task_id is not None:
        store.clear_review_state(clear_review_task_id)
    if resolve_comments_task_id is not None:
        store.resolve_comments(resolve_comments_task_id)


def _finalize_manual_skill_output_like_snippet(
    created,
    store,
    config,
    *,
    content_with_origin: str,
    output_body: str,
    path_kind: str,
    git=None,
    clear_review_task_id: str | None = None,
    resolve_comments_task_id: str | None = None,
):
    from gza.git import Git
    import gza.runner as runner

    try:
        if created.slug is None:
            slug_override = runner._compute_slug_override(created, store)
            slug_git = None if created.task_type == "review" else (git if git is not None else Git(config.project_dir))
            created.slug = runner.generate_slug(
                created.prompt,
                existing_id=None,
                log_path=config.log_path,
                git=slug_git,
                store=store,
                exclude_task_id=created.id,
                project_name=config.project_name,
                project_prefix=config.project_prefix,
                slug_override=slug_override,
                branch_strategy=config.branch_strategy,
                explicit_type=created.task_type_hint,
            )
            store.update(created)

        report_path, summary_path = runner.get_task_output_paths(created, config.project_dir)
        output_path = summary_path if path_kind == "summary" else report_path
        assert output_path is not None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content_with_origin)

        created.report_file = str(output_path.relative_to(config.project_dir))
        created.status = "completed"
        created.completed_at = datetime.now(UTC)
        created.output_content = output_body
        store.update(created)
    except Exception:
        created.status = "dropped"
        store.update(created)
        raise

    if clear_review_task_id is not None:
        store.clear_review_state(clear_review_task_id)
    if resolve_comments_task_id is not None:
        store.resolve_comments(resolve_comments_task_id)


def _extract_generate_slug_calls(snippet: str) -> list[str]:
    calls: list[str] = []
    start = 0
    marker = "generate_slug("

    while True:
        idx = snippet.find(marker, start)
        if idx == -1:
            return calls

        depth = 0
        for pos in range(idx, len(snippet)):
            char = snippet[pos]
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    calls.append(snippet[idx : pos + 1])
                    start = pos + 1
                    break
        else:
            raise AssertionError("Unbalanced generate_slug() call in skill snippet")


class TestSkillsInstallClaudeTarget:
    """Tests for `gza skills-install --target claude` behavior."""

    def test_list_available_skills(self, tmp_path: Path):
        """List command shows all available skills."""
        setup_config(tmp_path)
        result = invoke_gza("skills-install", "--target", "claude", "--list", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Available skills:" in result.stdout
        assert "gza-spec-coherence" in result.stdout
        assert "gza-task-add" in result.stdout
        assert "gza-task-info" in result.stdout
        assert "gza-plan-review" in result.stdout

    def test_install_all_skills(self, tmp_path: Path):
        """Install all skills to a project."""
        setup_config(tmp_path)
        result = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing" in result.stdout
        assert "Installed" in result.stdout

        # Verify skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert skills_dir.exists()
        assert (skills_dir / "gza-spec-coherence" / "SKILL.md").exists()
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert (skills_dir / "gza-task-info" / "SKILL.md").exists()
        assert (skills_dir / "gza-plan-review" / "SKILL.md").exists()

    def test_install_dev_skills(self, tmp_path: Path):
        """Install with --dev includes non-public skills."""
        setup_config(tmp_path)
        result = invoke_gza("skills-install", "--target", "claude", "--dev", "--project", str(tmp_path))

        assert result.returncode == 0
        skills_dir = tmp_path / ".claude" / "skills"
        assert (skills_dir / "gza-docs-review" / "SKILL.md").exists()

    def test_dev_skills_excluded_by_default(self, tmp_path: Path):
        """Non-public skills are not installed without --dev."""
        setup_config(tmp_path)
        result = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))

        assert result.returncode == 0
        skills_dir = tmp_path / ".claude" / "skills"
        # gza-spec-review is now public, so the dev-only check uses a different skill
        assert not (skills_dir / "gza-docs-review").exists()

    def test_install_specific_skills(self, tmp_path: Path):
        """Install only specific skills."""
        setup_config(tmp_path)
        result = invoke_gza("skills-install", "--target", "claude", "gza-task-add", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Installing 1 skill(s)" in result.stdout
        assert "Installed 1" in result.stdout

        # Verify only requested skills were created
        skills_dir = tmp_path / ".claude" / "skills"
        assert (skills_dir / "gza-task-add" / "SKILL.md").exists()
        assert not (skills_dir / "gza-task-info").exists()

    def test_skip_existing_skills_without_force(self, tmp_path: Path):
        """Existing skills are skipped without --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Try to install again without --force
        result2 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" in result2.stdout
        assert "up to date" in result2.stdout

    def test_outdated_skill_shows_update_available(self, tmp_path: Path):
        """Outdated skills are reported with an update hint when --update is not used."""
        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text(f"{original_content}\n# local edit\n")

        result2 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "update available" in result2.stdout
        assert "use --update" in result2.stdout

    def test_update_flag_overwrites_outdated_skills(self, tmp_path: Path):
        """--update refreshes outdated installed skills to bundled content."""
        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text(f"{original_content}\n# local edit\n")

        result2 = invoke_gza("skills-install", "--target", "claude", "--update", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout
        assert skill_file.read_text() == original_content

    def test_update_flag_refreshes_gza_task_review_verify_workflow(self, tmp_path: Path):
        """--update should refresh gza-task-review to the bundled verify-first workflow."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "gza-task-review", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-review" / "SKILL.md"
        skill_file.write_text(
            "---\n"
            "name: gza-task-review\n"
            "description: stale\n"
            "allowed-tools: Bash(uv run:*), Bash(git:*), Bash(gh:*)\n"
            "version: 1.0.0\n"
            "public: true\n"
            "---\n\n"
            "git log main..<impl_branch> --oneline\n"
            "git diff main...<impl_branch>\n"
        )

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            "gza-task-review",
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout

        refreshed = skill_file.read_text()
        bundled = (get_skills_source_path() / "gza-task-review" / "SKILL.md").read_text()
        assert refreshed == bundled
        assert "Bash(git:*)" not in refreshed
        assert "Run `verify_command` from `gza.yaml` as part of every review iteration." in refreshed
        assert "Pass the result forward as a `## verify_command result` section." in refreshed

    def test_update_flag_refreshes_gza_rebase_verify_contract(self, tmp_path: Path):
        """--update should restore the bundled gza-rebase verify-command contract."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "gza-rebase", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-rebase" / "SKILL.md"
        skill_file.write_text(
            "---\n"
            "name: gza-rebase\n"
            "description: stale\n"
            "allowed-tools: Bash(git:*), Bash(python:*)\n"
            "version: 1.0.0\n"
            "public: true\n"
            "---\n\n"
            "Run `python -m py_compile` and `uv run pytest` before success.\n"
            "Use `origin/main` (default) and fetch when needed.\n"
        )

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            "gza-rebase",
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout

        refreshed = skill_file.read_text()
        bundled = (get_skills_source_path() / "gza-rebase" / "SKILL.md").read_text()
        assert refreshed == bundled
        assert "configured `verify_command`" in refreshed
        assert "after any stashed changes have been restored" in refreshed
        assert "python -m py_compile" not in refreshed
        assert "`origin/main` (default)" not in refreshed

    def test_update_flag_refreshes_gza_test_and_fix_verify_lookup_contract(self, tmp_path: Path):
        """--update should restore the bundled gza-test-and-fix gza.yaml-first lookup contract."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "gza-test-and-fix", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-test-and-fix" / "SKILL.md"
        skill_file.write_text(
            "---\n"
            "name: gza-test-and-fix\n"
            "description: stale\n"
            "allowed-tools: Read, Edit, Bash(uv run:*), Bash(git:*)\n"
            "version: 3.0.0\n"
            "public: true\n"
            "---\n\n"
            "Run `uv run gza config` and extract `verify_command` first.\n"
            "If that fails, fall back to `gza.yaml`.\n"
        )

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            "gza-test-and-fix",
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout

        refreshed = skill_file.read_text()
        bundled = (get_skills_source_path() / "gza-test-and-fix" / "SKILL.md").read_text()
        assert refreshed == bundled
        assert "Read `verify_command` directly from `gza.yaml`" in refreshed
        assert "do not treat `gza config` failure as an error when `gza.yaml` was readable" in refreshed
        assert "Run `uv run gza config` and extract `verify_command` first." not in refreshed

    def test_update_flag_refreshes_gza_code_review_full_stale_importer_reference(self, tmp_path: Path):
        """--update should replace stale installed importer references in gza-code-review-full."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "gza-code-review-full", "--dev", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-code-review-full" / "SKILL.md"
        stale_location = "importer" + ".py:89"
        refreshed_location = "path/to/file.py:89"
        skill_file.write_text(
            "---\n"
            "name: gza-code-review-full\n"
            "description: stale\n"
            "allowed-tools: Read\n"
            "version: 1.0.0\n"
            "public: false\n"
            "---\n\n"
            "| Resource Type | Location | Issue |\n"
            "|---------------|----------|-------|\n"
            f"| file | {stale_location} | open() without context manager |\n"
        )

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            "--dev",
            "gza-code-review-full",
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout

        refreshed = skill_file.read_text()
        bundled = (get_skills_source_path() / "gza-code-review-full" / "SKILL.md").read_text()
        assert refreshed == bundled
        assert stale_location not in refreshed
        assert refreshed_location in refreshed

    def test_update_flag_refreshes_gza_task_fix_retired_recommend_rebase_schema(self, tmp_path: Path):
        """--update should remove retired recommend_rebase guidance from installed gza-task-fix."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result1 = invoke_gza("skills-install", "--target", "claude", "gza-task-fix", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-fix" / "SKILL.md"
        skill_file.write_text(
            "---\n"
            "name: gza-task-fix\n"
            "description: stale\n"
            "public: true\n"
            "---\n\n"
            "recommend_rebase:\n"
            "  recommended: true\n"
            "  reasons:\n"
            "    - branch_behind_target\n"
            "- `recommend_rebase.recommended=true` whenever either trigger fires.\n"
            "- `recommend_rebase.operator_action` must remain advisory. Do not run a rebase from this skill.\n"
            "- No automatic rebase. If the stale-branch recommendation fires, report it in the ledger and final handoff.\n"
        )

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            "gza-task-fix",
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout

        refreshed = skill_file.read_text()
        bundled = (get_skills_source_path() / "gza-task-fix" / "SKILL.md").read_text()
        assert refreshed == bundled
        assert "recommend_rebase:" not in refreshed
        assert "branch_behind_target" not in refreshed
        assert "recommend_rebase.recommended=true" not in refreshed
        assert "stale-branch recommendation" not in refreshed

    def test_update_flag_refreshes_manual_skill_generate_slug_collision_guards(self, tmp_path: Path):
        """--update should restore collision-aware generate_slug() kwargs in installed manual skills."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        skill_names = ["gza-task-fix", "gza-task-improve", "gza-task-review"]
        result1 = invoke_gza("skills-install", "--target", "claude", *skill_names, "--project", str(tmp_path))
        assert result1.returncode == 0

        stale_contents = {
            "gza-task-fix": (
                "```python\n"
                "created.slug = generate_slug(\n"
                "    created.prompt,\n"
                "    existing_id=created.id,\n"
                "    log_path=config.log_path,\n"
                "    git=Git(config.project_dir),\n"
                ")\n"
                "```\n"
            ),
            "gza-task-improve": (
                "```python\n"
                "created.slug = generate_slug(\n"
                "    created.prompt,\n"
                "    existing_id=created.id,\n"
                "    log_path=config.log_path,\n"
                "    git=Git(config.project_dir),\n"
                ")\n"
                "```\n"
            ),
            "gza-task-review": (
                "```python\n"
                "created.slug = generate_slug(\n"
                "    created.prompt,\n"
                "    existing_id=created.id,\n"
                "    log_path=config.log_path,\n"
                "    git=None,\n"
                "    store=store,\n"
                ")\n"
                "```\n"
            ),
        }

        for skill_name, stale_content in stale_contents.items():
            skill_file = tmp_path / ".claude" / "skills" / skill_name / "SKILL.md"
            skill_file.write_text(stale_content)

        result2 = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "--update",
            *skill_names,
            "--project",
            str(tmp_path),
        )
        assert result2.returncode == 0
        assert "updated 3" in result2.stdout

        skills_path = get_skills_source_path()
        for skill_name in skill_names:
            refreshed = (tmp_path / ".claude" / "skills" / skill_name / "SKILL.md").read_text()
            bundled = (skills_path / skill_name / "SKILL.md").read_text()
            assert refreshed == bundled
            assert "generate_slug(" in refreshed
            assert "store=store," in refreshed
            assert "exclude_task_id=created.id," in refreshed

    def test_overwrite_with_force_flag(self, tmp_path: Path):
        """Existing skills are overwritten with --force flag."""
        setup_config(tmp_path)

        # Install skills first time
        result1 = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Modify one of the skills
        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text("Modified content")

        # Install again with --force
        result2 = invoke_gza("skills-install", "--target", "claude", "--force", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" not in result2.stdout

        # Verify skill was overwritten
        assert skill_file.read_text() == original_content

    def test_install_nonexistent_skill(self, tmp_path: Path):
        """Error when requesting a skill that doesn't exist."""
        setup_config(tmp_path)
        result = invoke_gza(
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

        result = invoke_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result.returncode == 0
        assert skills_dir.exists()

    def test_install_from_different_directory(self, tmp_path: Path):
        """Skills can be installed to a different project directory."""
        project_dir = tmp_path / "my-project"
        project_dir.mkdir()
        setup_config(project_dir)

        # Run from tmp_path but target project_dir
        result = invoke_gza("skills-install", "--target", "claude", "--project", str(project_dir))
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
        result = invoke_gza("skills-install", "--list", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Available skills:" in result.stdout
        assert "gza-spec-coherence" in result.stdout
        assert "gza-task-add" in result.stdout
        assert "gza-plan-review" in result.stdout

    def test_install_all_targets_by_default(self, tmp_path: Path):
        """skills-install defaults to both claude and codex targets."""
        setup_config(tmp_path)
        codex_home = tmp_path / "codex-home"
        result = invoke_gza("skills-install", "--project", str(tmp_path), env={"CODEX_HOME": str(codex_home)})

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
        result = invoke_gza(
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
        result = invoke_gza(
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

    def test_gza_rebase_uses_project_verify_command_instead_of_python_specific_checks(self):
        """gza-rebase should keep project verification generic and post-stash."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-rebase" / "SKILL.md"
        content = skill_file.read_text()

        assert "project `verify_command`" in content
        assert "after any stashed changes have been restored" in content
        assert "rely on the configured `verify_command`, not language-specific hardcoded checks" in content
        assert "python -m py_compile" not in content
        assert "verifies Python syntax" not in content

    def test_gza_rebase_preserves_named_target_and_local_primary_branch_fallback(self):
        """gza-rebase should use the caller target when present and resolve the primary branch otherwise."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-rebase" / "SKILL.md"
        content = skill_file.read_text()

        assert "If the caller named a target branch (for example `master`), use that exact branch name." in content
        assert "Do not substitute `main` or any other default." in content
        assert 'git -C "$GZA_WORKTREE_ROOT" symbolic-ref --quiet --short refs/remotes/origin/HEAD' in content
        assert "whichever of `main` or `master` exists locally" in content
        assert "If no primary branch can be determined, stop and report the failure instead of assuming `main`." in content

    def test_worker_skills_read_gza_yaml_before_optional_gza_config_lookup(self):
        """Worker-facing skills should prefer gza.yaml because gza CLI may be unavailable in containers."""
        from gza.skills_utils import get_skills_source_path

        rebase_content = (get_skills_source_path() / "gza-rebase" / "SKILL.md").read_text()
        test_and_fix_content = (get_skills_source_path() / "gza-test-and-fix" / "SKILL.md").read_text()

        assert "read `verify_command` directly from `gza.yaml`" in rebase_content
        assert "do not treat `gza config` failure as an error when `gza.yaml` was readable" in rebase_content
        assert "Read `verify_command` directly from `gza.yaml`" in test_and_fix_content
        assert "do not treat `gza config` failure as an error when `gza.yaml` was readable" in test_and_fix_content

    def test_gza_plan_review_uses_supported_history_flag(self):
        """gza-plan-review should use supported gza history flags in command examples."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-plan-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza history --type plan --limit 10" in content
        assert "--last 10" not in content

    def test_gza_plan_review_requires_full_prefixed_task_id(self):
        """gza-plan-review should require full prefixed IDs instead of numeric shorthand."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-plan-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "full prefixed plan task ID" in content
        assert "supports `42` or `#42`" not in content
        assert "strip the leading `#`" not in content

    def test_gza_plan_review_go_path_defaults_to_queued_implement(self):
        """gza-plan-review should point go-path follow-up actions to queued implement."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-plan-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza implement -q <TASK_ID>" in content
        assert 'uv run gza implement <TASK_ID> [--review] "..."' not in content

    def test_gza_plan_review_go_path_collects_tags_and_pr_requirement(self):
        """gza-plan-review should ask about tags and PR requirement before suggesting implement."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-plan-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "use AskUserQuestion before giving the implement command" in content
        assert "suggest inheriting the plan task's existing tags by default" in content
        assert "Ask whether this implement task should pass `--pr`" in content
        assert "uv run gza implement -q <TASK_ID> --tag <tag> [--pr]" in content

    def test_gza_explore_summarize_requires_full_prefixed_task_id(self):
        """gza-explore-summarize should require full prefixed IDs instead of numeric shorthand."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-explore-summarize" / "SKILL.md"
        content = skill_file.read_text()

        assert "full prefixed explore task ID" in content
        assert "supports `42` or `#42`" not in content
        assert "strip the leading `#`" not in content

    def test_gza_spec_coherence_defines_required_behavior_spec_checks(self):
        """gza-spec-coherence should enforce overlap, cross-reference, and plain-language review."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-spec-coherence" / "SKILL.md"
        content = skill_file.read_text()
        normalized = " ".join(content.split())

        assert "This skill is blind to authorship." in content
        assert "never who wrote it" in content
        assert "Bash(git log:*)" not in content
        assert "Bash(git blame:*)" not in content
        assert "00-overview.md" in content
        assert "lifecycle-engine.md" in content
        assert "restated shared vocabulary or invariants" in normalized
        assert "Broken or missing cross-references" in content
        assert "RFC-2119 keyword misuse" in content
        assert "quote the clause, then propose a tighter rewrite" in normalized
        assert "MUST NOT edit the spec or the code" in content
        assert "reviews/<timestamp>-spec-coherence.md" in content

    def test_gza_behavior_check_installed_skill_includes_machine_readable_findings_appendix(
        self, tmp_path: Path
    ):
        """Installed behavior-check skill should bundle the machine-readable findings appendix contract."""
        from gza.skills_utils import get_skills_source_path

        setup_config(tmp_path)

        result = invoke_gza(
            "skills-install",
            "--target",
            "claude",
            "gza-behavior-check",
            "--project",
            str(tmp_path),
        )
        assert result.returncode == 0

        installed = (tmp_path / ".claude" / "skills" / "gza-behavior-check" / "SKILL.md").read_text()
        bundled = (get_skills_source_path() / "gza-behavior-check" / "SKILL.md").read_text()
        normalized = " ".join(installed.split())

        assert installed == bundled
        assert "## Machine-readable findings" in installed
        assert '"assertion_id": "LE-§6-IMPROVE-CHAIN"' in installed
        assert '"recommendation": null' in installed
        assert '"report_path": "reviews/<timestamp>-behavior-check.md"' in installed
        assert "Emit **one JSON object per checked assertion** (HOLDS, DIVERGES, and UNDETERMINED)" in installed
        assert "use `null` for `HOLDS` and `UNDETERMINED`." in normalized

    def test_gza_task_run_routes_to_first_class_run_inline_command(self):
        """gza-task-run should delegate execution to `gza run-inline` instead of synthetic lifecycle steps."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza run-inline <TASK_ID>" in content
        assert "write_log_entry" not in content

    def test_gza_task_run_mentions_runner_owned_lifecycle(self):
        """gza-task-run should explicitly state that runner owns lifecycle responsibilities."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "runner-managed task execution" in content
        assert "lifecycle ownership stays in the runner" in content

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve"])
    def test_manual_review_improve_skills_use_explicit_project_dir_for_all_config_loads(
        self, skill_name: str
    ):
        """Manual review/improve skills should not ship zero-arg Config.load() snippets anywhere."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "Config.load()" not in content
        assert content.count("Config.load(Path.cwd())") >= 2

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve", "gza-task-fix"])
    def test_manual_persistence_snippets_use_project_bound_store_api(
        self, skill_name: str
    ):
        """Manual persistence snippets should use project-bound SqliteTaskStore.from_config()."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "from gza.db import SqliteTaskStore" in content
        assert "from gza.models import Task" not in content
        assert "config = Config.load(Path.cwd())" in content
        assert "SqliteTaskStore.from_config(config)" in content
        assert "SqliteTaskStore(config.db_path)" not in content
        assert "store.add(" in content
        assert "store.update(created)" in content
        assert "store.create(" not in content

    def test_manual_task_skill_bootstrap_resolves_existing_task_in_shared_db(self, tmp_path: Path):
        """The documented manual-task bootstrap must resolve tasks through shared-DB project scoping."""
        from gza.config import Config
        from gza.db import SqliteTaskStore

        setup_shared_db_config(tmp_path)
        config = Config.load(tmp_path)

        shared_store = SqliteTaskStore.from_config(config)
        created = shared_store.add("Shared DB task for skill bootstrap")
        assert created.id is not None

        unscoped_store = SqliteTaskStore(config.db_path)
        assert unscoped_store.get(created.id) is None

        scoped_store = SqliteTaskStore.from_config(config)
        resolved = scoped_store.get(created.id)
        assert resolved is not None
        assert resolved.id == created.id
        assert resolved.prompt == created.prompt

    @pytest.mark.parametrize(
        ("skill_name", "expects_git_import", "path_var"),
        [
            ("gza-task-review", False, "report_path"),
            ("gza-task-improve", True, "summary_path"),
            ("gza-task-fix", True, "summary_path"),
        ],
    )
    def test_manual_persistence_snippets_assign_slug_before_output_path_lookup(
        self, skill_name: str, expects_git_import: bool, path_var: str
    ):
        """Manual persistence snippets should assign/persist slug before calling get_task_output_paths()."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "from gza.runner import _compute_slug_override, generate_slug, get_task_output_paths" in content
        assert "if created.slug is None:" in content
        assert "store.update(created)" in content
        assert "get_task_output_paths(created, config.project_dir)" in content
        assert f"assert {path_var} is not None" in content
        assert content.rfind("try:\n    if created.slug is None:") != -1
        assert content.rfind("if created.slug is None:") < content.rfind(
            "get_task_output_paths(created, config.project_dir)"
        )
        if expects_git_import:
            assert "from gza.git import Git" in content
        else:
            assert "from gza.git import Git" not in content

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve", "gza-task-fix"])
    def test_manual_persistence_snippets_drop_task_on_post_add_failure(self, skill_name: str):
        """Manual persistence snippets should mark the created row dropped before re-raising."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "except Exception:" in content
        assert "created.status = 'dropped'" in content
        assert "store.update(created)" in content
        assert "raise" in content
        assert content.rfind("assert created.id is not None") < content.rfind("try:")
        assert content.rfind("try:") < content.rfind("if created.slug is None:")

    def test_manual_review_skill_persistence_snippet_stays_checkout_neutral(self):
        """gza-task-review persistence should avoid Git-backed slug collision checks."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "from gza.git import Git" not in content
        assert "Git(config.project_dir)" not in content
        assert "git=None," in content
        assert "store=store," in content
        assert "exclude_task_id=created.id," in content

    def test_all_skill_generate_slug_invocations_pass_store_and_exclude_task_id(self):
        """Bundled skills must pass store and exclude_task_id to every generate_slug() call."""
        from gza.skills_utils import get_available_skills, get_skills_source_path

        skills_path = get_skills_source_path()

        for skill_name in get_available_skills():
            skill_file = skills_path / skill_name / "SKILL.md"
            content = skill_file.read_text()
            if "generate_slug(" not in content:
                continue

            snippets = re.findall(r"```(?:python|bash)\n(.*?)```", content, flags=re.DOTALL)
            calls = [
                call
                for snippet in snippets
                for call in _extract_generate_slug_calls(snippet)
            ]
            assert calls, f"{skill_name} contains generate_slug text but no fenced-code call"

            for call in calls:
                assert "store=" in call, f"{skill_name} generate_slug() must pass store="
                assert "exclude_task_id=" in call, (
                    f"{skill_name} generate_slug() must pass exclude_task_id="
                )

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve"])
    def test_manual_review_improve_persistence_snippets_keep_completed_at_as_datetime(
        self, skill_name: str
    ):
        """Manual persistence snippets should keep completed_at as datetime for SqliteTaskStore.update()."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "created.completed_at = datetime.now(timezone.utc)" in content
        assert "created.completed_at = datetime.now(timezone.utc).isoformat()" not in content

    def test_manual_review_persistence_flow_works_for_fresh_task(self, tmp_path: Path):
        """Fresh manual review persistence should produce a slug-backed prompt path and store update cleanly."""
        from gza.runner import get_task_output_paths

        setup_config(tmp_path)
        config, store = _create_store_for_project(tmp_path)

        impl_task = store.add("Implement feature for manual review flow", task_type="implement")
        assert impl_task.id is not None

        review_markdown = "## Summary\n\n- Manual review body\n"
        origin_date = datetime.now(UTC).strftime("%Y-%m-%d")
        file_content = f"<!-- origin: /gza-task-review (manual, {origin_date}) -->\n{review_markdown}"

        created = store.add(
            prompt="Manual review via /gza-task-review",
            task_type="review",
            depends_on=impl_task.id,
            group="qa",
        )
        assert created.id is not None

        report_path_before, _summary_path_before = get_task_output_paths(created, config.project_dir)
        assert report_path_before is None

        mock_git = Mock()
        mock_git.branch_exists.return_value = False
        _assign_slug_like_runner(created, store, config, git=mock_git)
        refreshed = store.get(created.id)
        assert refreshed is not None

        report_path_value, _summary_path_after = get_task_output_paths(refreshed, config.project_dir)
        assert report_path_value is not None

        _persist_manual_skill_output(
            created,
            store,
            config,
            content_with_origin=file_content,
            output_body=review_markdown,
            path_kind="report",
        )

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.report_file == created.report_file
        assert isinstance(persisted.completed_at, datetime)
        assert persisted.output_content == review_markdown
        report_path = Path(report_path_value)
        assert report_path.read_text() == file_content

    def test_manual_improve_persistence_flow_works_for_fresh_task(self, tmp_path: Path):
        """Fresh manual improve persistence should produce a slug-backed prompt path and clear review state."""
        from gza.runner import get_task_output_paths

        setup_config(tmp_path)
        config, store = _create_store_for_project(tmp_path)

        impl_task = store.add("Implement feature for manual improve flow", task_type="implement")
        assert impl_task.id is not None
        review_task = store.add(
            "Review task for manual improve flow",
            task_type="review",
            depends_on=impl_task.id,
        )
        assert review_task.id is not None

        summary_body = "Addressed 2 must-fix items."
        origin_date = datetime.now(UTC).strftime("%Y-%m-%d")
        summary_with_origin = (
            f"<!-- origin: /gza-task-improve (manual, {origin_date}) -->\n{summary_body}"
        )

        created = store.add(
            prompt="Manual improve via /gza-task-improve",
            task_type="improve",
            depends_on=review_task.id,
            based_on=impl_task.id,
        )
        assert created.id is not None

        _report_path_before, summary_path_before = get_task_output_paths(created, config.project_dir)
        assert summary_path_before is None

        mock_git = Mock()
        mock_git.branch_exists.return_value = False
        _assign_slug_like_runner(created, store, config, git=mock_git)
        refreshed = store.get(created.id)
        assert refreshed is not None

        _report_path_after, summary_path_value = get_task_output_paths(refreshed, config.project_dir)
        assert summary_path_value is not None

        _persist_manual_skill_output(
            created,
            store,
            config,
            content_with_origin=summary_with_origin,
            output_body=summary_body,
            path_kind="summary",
            clear_review_task_id=impl_task.id,
            resolve_comments_task_id=impl_task.id,
        )

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.report_file == created.report_file
        assert persisted.depends_on == review_task.id
        assert isinstance(persisted.completed_at, datetime)
        assert persisted.output_content == summary_body
        summary_path = Path(summary_path_value)
        assert summary_path.read_text() == summary_with_origin
        assert store.get_improve_tasks_for(impl_task.id, review_task.id) == [persisted]

        impl_refreshed = store.get(impl_task.id)
        assert impl_refreshed is not None
        assert impl_refreshed.review_cleared_at is not None

    @pytest.mark.parametrize(
        ("task_type", "prompt", "path_kind"),
        [
            ("review", "Manual review via /gza-task-review", "report"),
            ("improve", "Manual improve via /gza-task-improve", "summary"),
            ("fix", "Manual rescue via /gza-task-fix", "summary"),
        ],
    )
    def test_manual_persistence_flow_marks_task_dropped_when_output_path_resolution_fails(
        self, tmp_path: Path, task_type: str, prompt: str, path_kind: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Post-add failures must not leave manual review/improve/fix rows pending."""
        setup_config(tmp_path)
        config, store = _create_store_for_project(tmp_path)

        depends_on = None
        based_on = None
        if task_type in {"review", "improve", "fix"}:
            impl_task = store.add("Implementation task for persistence failure", task_type="implement")
            assert impl_task.id is not None
            if task_type == "review":
                depends_on = impl_task.id
            else:
                review_task = store.add(
                    "Review task for persistence failure",
                    task_type="review",
                    depends_on=impl_task.id,
                )
                assert review_task.id is not None
                depends_on = review_task.id
                based_on = impl_task.id

        created = store.add(
            prompt=prompt,
            task_type=task_type,
            depends_on=depends_on,
            based_on=based_on,
        )
        assert created.id is not None

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated persistence failure")

        import gza.runner as runner

        monkeypatch.setattr(runner, "get_task_output_paths", _boom)

        mock_git = Mock()
        mock_git.branch_exists.return_value = False

        with pytest.raises(RuntimeError, match="simulated persistence failure"):
            _finalize_manual_skill_output_like_snippet(
                created,
                store,
                config,
                content_with_origin="ignored",
                output_body="ignored",
                path_kind=path_kind,
                git=mock_git,
            )

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.status == "dropped"
        assert persisted.report_file is None
        assert persisted.completed_at is None
        assert persisted.output_content is None

    @pytest.mark.parametrize(
        ("task_type", "prompt", "path_kind", "failure_stage"),
        [
            ("review", "Manual review via /gza-task-review", "report", "slug_override"),
            ("improve", "Manual improve via /gza-task-improve", "summary", "slug_override"),
            ("fix", "Manual rescue via /gza-task-fix", "summary", "slug_override"),
            ("review", "Manual review via /gza-task-review", "report", "slug_update"),
            ("improve", "Manual improve via /gza-task-improve", "summary", "slug_update"),
            ("fix", "Manual rescue via /gza-task-fix", "summary", "slug_update"),
        ],
    )
    def test_manual_persistence_flow_marks_task_dropped_when_slug_stage_fails(
        self,
        tmp_path: Path,
        task_type: str,
        prompt: str,
        path_kind: str,
        failure_stage: str,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Failures before output-path lookup must still drop the created manual task."""
        import gza.runner as runner

        setup_config(tmp_path)
        config, store = _create_store_for_project(tmp_path)

        depends_on = None
        based_on = None
        if task_type in {"review", "improve", "fix"}:
            impl_task = store.add("Implementation task for slug failure", task_type="implement")
            assert impl_task.id is not None
            if task_type == "review":
                depends_on = impl_task.id
            else:
                review_task = store.add(
                    "Review task for slug failure",
                    task_type="review",
                    depends_on=impl_task.id,
                )
                assert review_task.id is not None
                depends_on = review_task.id
                based_on = impl_task.id

        created = store.add(
            prompt=prompt,
            task_type=task_type,
            depends_on=depends_on,
            based_on=based_on,
        )
        assert created.id is not None

        if failure_stage == "slug_override":
            def _slug_override_boom(*args, **kwargs):
                raise RuntimeError("simulated slug override failure")

            monkeypatch.setattr(runner, "_compute_slug_override", _slug_override_boom)
        else:
            monkeypatch.setattr(runner, "_compute_slug_override", lambda *args, **kwargs: None)
            monkeypatch.setattr(runner, "generate_slug", lambda *args, **kwargs: "20260514-test-slug")
            original_update = store.update
            update_calls = 0

            def _update_then_boom(task):
                nonlocal update_calls
                update_calls += 1
                if update_calls == 1:
                    raise RuntimeError("simulated slug update failure")
                return original_update(task)

            monkeypatch.setattr(store, "update", _update_then_boom)

        mock_git = Mock()
        mock_git.branch_exists.return_value = False

        expected_error = (
            "simulated slug override failure"
            if failure_stage == "slug_override"
            else "simulated slug update failure"
        )
        with pytest.raises(RuntimeError, match=expected_error):
            _finalize_manual_skill_output_like_snippet(
                created,
                store,
                config,
                content_with_origin="ignored",
                output_body="ignored",
                path_kind=path_kind,
                git=mock_git,
            )

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.status == "dropped"
        assert persisted.report_file is None
        assert persisted.completed_at is None
        assert persisted.output_content is None

    @pytest.mark.parametrize(
        ("prompt", "task_type", "path_kind"),
        [
            ("Manual rescue via /gza-task-fix", "fix", "summary"),
            ("Manual improve via /gza-task-improve", "improve", "summary"),
        ],
    )
    def test_manual_skill_slug_assignment_suffixes_collisions(
        self, tmp_path: Path, prompt: str, task_type: str, path_kind: str
    ):
        """Repeated manual skill persistence should allocate distinct slugs and output paths."""
        from gza.runner import get_task_output_paths

        setup_config(tmp_path)
        config, store = _create_store_for_project(tmp_path)

        first = store.add(prompt=prompt, task_type=task_type)
        second = store.add(prompt=prompt, task_type=task_type)
        assert first.id is not None
        assert second.id is not None

        mock_git = Mock()
        mock_git.branch_exists.return_value = False

        _assign_slug_like_runner(first, store, config, git=mock_git)
        _assign_slug_like_runner(second, store, config, git=mock_git)

        first = store.get(first.id)
        second = store.get(second.id)
        assert first is not None
        assert second is not None
        assert first.slug is not None
        assert second.slug is not None
        assert first.slug != second.slug
        assert second.slug.endswith("-2")

        first_report_path, first_summary_path = get_task_output_paths(first, config.project_dir)
        second_report_path, second_summary_path = get_task_output_paths(second, config.project_dir)
        if path_kind == "summary":
            assert first_summary_path is not None
            assert second_summary_path is not None
            assert first_summary_path != second_summary_path
        else:
            assert first_report_path is not None
            assert second_report_path is not None
            assert first_report_path != second_report_path

    def test_manual_improve_skill_documents_review_linkage(self):
        """gza-task-improve should document persists with depends_on review linkage."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-improve" / "SKILL.md"
        content = skill_file.read_text()

        assert "depends_on='<REVIEW_TASK_ID>'" in content
        assert "Use the `review_task_id` already resolved in Step 1" in content

    def test_manual_improve_skill_preserves_starting_checkout(self):
        """gza-task-improve should restore the user's starting checkout before exit."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-improve" / "SKILL.md"
        content = skill_file.read_text()

        assert "git symbolic-ref --quiet --short HEAD || git rev-parse --short HEAD" in content
        assert "<START_CHECKOUT>" in content
        assert "git checkout --detach <START_CHECKOUT>" in content

    def test_manual_improve_skill_requires_restoring_starting_checkout_before_exit(self):
        """gza-task-improve should explicitly restore the initial checkout after persisting results."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-improve" / "SKILL.md"
        content = skill_file.read_text()

        assert "### Step 9: Restore the starting checkout" in content
        assert "git checkout <START_CHECKOUT>" in content
        assert "Do not silently finish on the task branch." in content

    def test_manual_improve_skill_requires_commit_and_push_before_persisting_results(self):
        """gza-task-improve should require both a commit and a push on the implementation branch."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-improve" / "SKILL.md"
        content = skill_file.read_text()

        assert "A successful `/gza-task-improve` run always ends with a commit" in content
        assert "### Step 7: Push the implementation branch" in content
        assert "git push -u origin <impl_branch>" in content
        assert "After a successful commit and push" in content
        assert "Push: pushed to <IMPL_BRANCH>" in content

    def test_manual_review_skill_requires_verify_command_every_cycle(self):
        """gza-task-review should always run verify alongside the code review and fold failures into blockers."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "Every review iteration must do both the normal code review work and an independent `verify_command` run" in content
        assert "This is required even when the diff already has obvious code-review blockers; do not skip verify" in content
        assert "If verify passed, do not add findings just because verify ran." in content
        assert "If verify failed, synthesize one or more blocking findings" in content
        assert "verify_command failure" in content
        assert "Treat verify failures as blocking even if the code review itself would otherwise approve the diff." in content

    def test_manual_review_skill_passes_verify_result_to_subagent(self):
        """gza-task-review should hand off verify output to the reviewer in canonical context."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "Pass the result forward as a `## verify_command result` section." in content
        assert "Independently evaluate the provided `## verify_command result` section in addition to the normal code review." in content
        assert "Pass the branch name, authoritative diff context, the `## verify_command result` section" in content

    def test_manual_review_skill_forbids_manual_checkout_switching(self):
        """gza-task-review should avoid forbidden manual checkout/switch instructions while keeping verify guidance."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-review" / "SKILL.md"
        content = skill_file.read_text()

        assert "Bash(git:*)" not in content
        assert "git checkout <impl_branch>" not in content
        assert "git switch <impl_branch>" not in content
        assert "git checkout <START_CHECKOUT>" not in content
        assert "git checkout --detach <START_CHECKOUT>" not in content
        assert "Do not run `git checkout`, `git switch`, or other manual branch-switching commands as part of this skill." in content
        assert "Run `verify_command` from `gza.yaml` as part of every review iteration." in content
        assert "Pass the result forward as a `## verify_command result` section." in content

    def test_gza_task_run_no_longer_documents_manual_mark_completed_recovery(self):
        """gza-task-run should route only through run-inline, without manual completion recovery steps."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza mark-completed <TASK_ID>" not in content
        assert "Outcome: completed (inline skill)" not in content

    def test_gza_task_run_does_not_document_skill_inline_set_status_fallback(self):
        """gza-task-run should not document synthetic skill_inline lifecycle mutations."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza set-status <TASK_ID> in_progress --execution-mode skill_inline" not in content

    def test_gza_task_resume_routes_to_resume_or_run_inline_resume(self):
        """gza-task-resume should route users to first-class CLI resume flows."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-resume" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza resume <TASK_ID>" in content
        assert "uv run gza run-inline <TASK_ID> --resume" in content
        assert "gza set-status" not in content

    def test_gza_task_resume_uses_supported_history_flags(self):
        """gza-task-resume should use supported gza history flags in examples."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-resume" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza history --status failed --last 10" in content
        assert "--status failed --limit 10" not in content

    @pytest.mark.parametrize(
        "skill_name",
        [
            "gza-explore-summarize",
            "gza-plan-review",
            "gza-task-review",
            "gza-summary",
        ],
    )
    def test_skill_examples_do_not_use_unsupported_gza_log_task_flag(self, skill_name: str):
        """Skill docs should not include invalid `gza log ... --task` guidance."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "gza log --task" not in content
        assert "gza log gza-p --task" not in content

    @pytest.mark.parametrize(
        "skill_name",
        [
            "gza-explore-summarize",
            "gza-task-run",
            "gza-task-review",
            "gza-plan-review",
            "gza-task-resume",
            "gza-task-improve",
            "gza-task-fix",
            "gza-task-info",
            "gza-task-debug",
        ],
    )
    def test_task_id_sensitive_skills_avoid_numeric_or_shorthand_guidance(self, skill_name: str):
        """Task-ID-sensitive skills should require full prefixed task IDs."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        lowered = content.lower()
        assert "full prefixed" in lowered and "task id" in lowered
        assert "task id (numeric)" not in lowered
        assert "actual numeric task id" not in lowered
        assert "supports `42`" not in content
        assert "#42" not in content

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
        from gza.skills_utils import get_available_skills, get_skills_source_path

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
        from gza.skills_utils import get_available_skills, get_skills_source_path

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
        from gza.skills_utils import get_available_skills, get_skills_source_path

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
        import re

        from gza.skills_utils import get_available_skills, get_skills_source_path

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
