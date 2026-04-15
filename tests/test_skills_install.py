"""Tests for the skills-install command."""

import json
import os
import subprocess
from datetime import UTC, datetime
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


def _create_store_for_project(tmp_path: Path):
    from gza.config import Config
    from gza.db import SqliteTaskStore

    config = Config.load(tmp_path)
    store = SqliteTaskStore(config.db_path, prefix=config.project_prefix)
    return config, store


def _assign_slug_like_runner(task, store, config) -> None:
    from gza.git import Git
    from gza.runner import _compute_slug_override, generate_slug

    if task.slug is not None:
        return
    slug_override = _compute_slug_override(task, store)
    task.slug = generate_slug(
        task.prompt,
        existing_id=None,
        log_path=config.log_path,
        git=Git(config.project_dir),
        project_name=config.project_name,
        project_prefix=config.project_prefix,
        slug_override=slug_override,
        branch_strategy=config.branch_strategy,
        explicit_type=task.task_type_hint,
    )
    store.update(task)


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

    def test_install_dev_skills(self, tmp_path: Path):
        """Install with --dev includes non-public skills."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "--dev", "--project", str(tmp_path))

        assert result.returncode == 0
        skills_dir = tmp_path / ".claude" / "skills"
        assert (skills_dir / "gza-docs-review" / "SKILL.md").exists()

    def test_dev_skills_excluded_by_default(self, tmp_path: Path):
        """Non-public skills are not installed without --dev."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))

        assert result.returncode == 0
        skills_dir = tmp_path / ".claude" / "skills"
        # gza-spec-review is now public, so the dev-only check uses a different skill
        assert not (skills_dir / "gza-docs-review").exists()

    def test_install_specific_skills(self, tmp_path: Path):
        """Install only specific skills."""
        setup_config(tmp_path)
        result = run_gza("skills-install", "--target", "claude", "gza-task-add", "--project", str(tmp_path))

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
        result1 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        # Try to install again without --force
        result2 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "skipped" in result2.stdout
        assert "up to date" in result2.stdout

    def test_outdated_skill_shows_update_available(self, tmp_path: Path):
        """Outdated skills are reported with an update hint when --update is not used."""
        setup_config(tmp_path)

        result1 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text(f"{original_content}\n# local edit\n")

        result2 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "update available" in result2.stdout
        assert "use --update" in result2.stdout

    def test_update_flag_overwrites_outdated_skills(self, tmp_path: Path):
        """--update refreshes outdated installed skills to bundled content."""
        setup_config(tmp_path)

        result1 = run_gza("skills-install", "--target", "claude", "--project", str(tmp_path))
        assert result1.returncode == 0

        skill_file = tmp_path / ".claude" / "skills" / "gza-task-add" / "SKILL.md"
        original_content = skill_file.read_text()
        skill_file.write_text(f"{original_content}\n# local edit\n")

        result2 = run_gza("skills-install", "--target", "claude", "--update", "--project", str(tmp_path))
        assert result2.returncode == 0
        assert "updated 1" in result2.stdout
        assert "(updated)" in result2.stdout
        assert skill_file.read_text() == original_content

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
        result = run_gza("skills-install", "--target", "claude", "--project", str(project_dir))
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

    def test_gza_task_run_routes_to_first_class_run_inline_command(self):
        """gza-task-run should delegate execution to `gza run-inline` instead of synthetic lifecycle steps."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza run-inline <TASK_ID>" in content
        assert "uv run gza set-status" not in content
        assert "uv run gza mark-completed" not in content
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

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve"])
    def test_manual_review_improve_persistence_snippets_use_valid_store_api(
        self, skill_name: str
    ):
        """Manual persistence snippets should use SqliteTaskStore.add/update, not nonexistent create()."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "from gza.db import SqliteTaskStore" in content
        assert "from gza.models import Task" not in content
        assert "config = Config.load(Path.cwd())" in content
        assert "store.add(" in content
        assert "store.update(created)" in content
        assert "store.create(" not in content

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve"])
    def test_manual_review_improve_persistence_snippets_assign_slug_before_show_prompt(
        self, skill_name: str
    ):
        """Manual persistence snippets should assign/persist slug before calling gza show --prompt."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
        content = skill_file.read_text()

        assert "from gza.git import Git" in content
        assert "from gza.runner import _compute_slug_override, generate_slug" in content
        assert "if created.slug is None:" in content
        assert "store.update(created)" in content
        assert "['uv', 'run', 'gza', 'show', '--prompt', created.id]" in content
        assert content.find("if created.slug is None:") < content.find(
            "['uv', 'run', 'gza', 'show', '--prompt', created.id]"
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

        prompt_before = run_gza("show", "--prompt", created.id, "--project", str(tmp_path))
        assert prompt_before.returncode == 0
        prompt_before_data = json.loads(prompt_before.stdout)
        assert prompt_before_data["report_path"] is None

        _assign_slug_like_runner(created, store, config)

        prompt_after = run_gza("show", "--prompt", created.id, "--project", str(tmp_path))
        assert prompt_after.returncode == 0
        prompt_after_data = json.loads(prompt_after.stdout)
        report_path_value = prompt_after_data["report_path"]
        assert report_path_value is not None

        report_path = Path(report_path_value)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(file_content)

        created.report_file = str(report_path.relative_to(config.project_dir))
        created.status = "completed"
        created.completed_at = datetime.now(UTC)
        created.output_content = review_markdown
        store.update(created)

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.report_file == created.report_file
        assert isinstance(persisted.completed_at, datetime)
        assert persisted.output_content == review_markdown
        assert report_path.read_text() == file_content

    def test_manual_improve_persistence_flow_works_for_fresh_task(self, tmp_path: Path):
        """Fresh manual improve persistence should produce a slug-backed prompt path and clear review state."""
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

        prompt_before = run_gza("show", "--prompt", created.id, "--project", str(tmp_path))
        assert prompt_before.returncode == 0
        prompt_before_data = json.loads(prompt_before.stdout)
        assert prompt_before_data["summary_path"] is None

        _assign_slug_like_runner(created, store, config)

        prompt_after = run_gza("show", "--prompt", created.id, "--project", str(tmp_path))
        assert prompt_after.returncode == 0
        prompt_after_data = json.loads(prompt_after.stdout)
        summary_path_value = prompt_after_data["summary_path"]
        assert summary_path_value is not None

        summary_path = Path(summary_path_value)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(summary_with_origin)

        created.report_file = str(summary_path.relative_to(config.project_dir))
        created.status = "completed"
        created.completed_at = datetime.now(UTC)
        created.output_content = summary_body
        store.update(created)
        store.clear_review_state(impl_task.id)

        persisted = store.get(created.id)
        assert persisted is not None
        assert persisted.report_file == created.report_file
        assert persisted.depends_on == review_task.id
        assert isinstance(persisted.completed_at, datetime)
        assert persisted.output_content == summary_body
        assert summary_path.read_text() == summary_with_origin
        assert store.get_improve_tasks_for(impl_task.id, review_task.id) == [persisted]

        impl_refreshed = store.get(impl_task.id)
        assert impl_refreshed is not None
        assert impl_refreshed.review_cleared_at is not None

    def test_manual_improve_skill_documents_review_linkage(self):
        """gza-task-improve should document persists with depends_on review linkage."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-improve" / "SKILL.md"
        content = skill_file.read_text()

        assert "depends_on='<REVIEW_TASK_ID>'" in content
        assert "Use the `review_task_id` already resolved in Step 1" in content

    @pytest.mark.parametrize("skill_name", ["gza-task-review", "gza-task-improve"])
    def test_manual_review_improve_skills_preserve_starting_checkout(self, skill_name: str):
        """Manual review/improve skills should restore the user's starting checkout before exit."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / skill_name / "SKILL.md"
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

    def test_gza_task_run_completion_guidance_matches_mark_completed_cli_contract(self):
        """gza-task-run should not document unsupported mark-completed branch flags."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza mark-completed <TASK_ID> --branch <BRANCH_NAME>" not in content
        assert "uv run gza mark-completed <TASK_ID>" in content

    def test_gza_task_run_logs_success_outcome_only_after_mark_completed_step(self):
        """gza-task-run should document success outcome logging after mark-completed succeeds."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        mark_completed_cmd = "uv run gza mark-completed <TASK_ID>"
        success_outcome = "Outcome: completed (inline skill)"

        mark_idx = content.find(mark_completed_cmd)
        outcome_idx = content.find(success_outcome)
        assert mark_idx != -1
        assert outcome_idx != -1
        assert outcome_idx > mark_idx

    def test_gza_task_run_marks_in_progress_with_skill_inline_execution_mode(self):
        """gza-task-run should stamp in-progress inline runs with skill_inline provenance."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-run" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza set-status <TASK_ID> in_progress --execution-mode skill_inline" in content

    def test_gza_task_resume_routes_to_resume_or_run_inline_resume(self):
        """gza-task-resume should route users to first-class CLI resume flows."""
        from gza.skills_utils import get_skills_source_path

        skill_file = get_skills_source_path() / "gza-task-resume" / "SKILL.md"
        content = skill_file.read_text()

        assert "uv run gza resume <TASK_ID>" in content
        assert "uv run gza run-inline <TASK_ID> --resume" in content
        assert "gza set-status" not in content

    @pytest.mark.parametrize(
        "skill_name",
        [
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
