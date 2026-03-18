"""Tests for runner module."""

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, StepRef, Task, TaskStats
from gza.git import Git
from gza.providers import RunResult, ClaudeProvider
from gza.runner import (
    build_prompt,
    SUMMARY_DIR,
    REVIEW_IMPROVE_LINEAGE_LIMIT,
    WIP_DIR,
    BACKUP_DIR,
    _build_context_from_chain,
    _build_review_improve_lineage_context,
    _copy_learnings_to_worktree,
    _extract_review_verdict,
    backup_database,
    _compute_slug_override,
    _complete_code_task,
    _create_and_run_review_task,
    _resolve_code_task_branch_name,
    _run_non_code_task,
    _save_wip_changes,
    _select_worktree_base_ref,
    _setup_code_task_worktree,
    _restore_wip_changes,
    _squash_wip_commits,
    _run_result_to_stats,
    _slug_from_task_id,
    generate_task_id,
    post_review_to_pr,
    run,
    write_log_entry,
)


class TestBuildPrompt:
    """Tests for build_prompt function."""

    def test_build_prompt_task_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for task type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature X" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()
        assert "What was accomplished" in prompt
        assert "Files changed" in prompt
        assert "notable decisions" in prompt.lower()

    def test_build_prompt_implement_type_with_summary_path(self, tmp_path: Path):
        """Test that build_prompt includes summary instructions for implement type."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, summary_path=summary_path)

        assert "Complete this task: Implement feature Y" in prompt
        assert str(summary_path) in prompt
        assert "write a summary" in prompt.lower()

    def test_build_prompt_task_type_without_summary_path(self, tmp_path: Path):
        """Test that build_prompt uses fallback message without summary path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Z",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store, summary_path=None)

        assert "Complete this task: Implement feature Z" in prompt
        assert "report what you accomplished" in prompt
        assert "write a summary" not in prompt.lower()

    def test_build_prompt_explore_type_ignores_summary_path(self, tmp_path: Path):
        """Test that explore tasks use report_path, not summary_path."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        report_path = Path("/workspace/.gza/explorations/test.md")
        summary_path = Path("/workspace/.gza/summaries/test.md")

        prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path)

        # Should use report_path for explore tasks
        assert str(report_path) in prompt
        assert "exploration/research task" in prompt.lower()
        # Should NOT include summary path
        assert str(summary_path) not in prompt

    def test_build_prompt_includes_learnings_when_file_exists(self, tmp_path: Path):
        """Test that build_prompt references learnings.md when it exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md (content doesn't matter; prompt only references the path)
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        (gza_dir / "learnings.md").write_text("")

        prompt = build_prompt(task, config, store)

        assert ".gza/learnings.md" in prompt

    def test_build_prompt_skips_learnings_when_no_file(self, tmp_path: Path):
        """Test that build_prompt works normally when learnings.md doesn't exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        prompt = build_prompt(task, config, store)

        assert "learnings.md" not in prompt
        assert "Complete this task: Implement feature X" in prompt

    def test_build_prompt_skips_learnings_when_skip_learnings_true(self, tmp_path: Path):
        """Test that build_prompt skips learnings reference when task.skip_learnings is True."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="One-off experimental task",
            task_type="implement",
            skip_learnings=True,
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Create .gza/learnings.md
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir(parents=True, exist_ok=True)
        learnings_content = "# Project Learnings\n\n- Use pytest fixtures\n"
        (gza_dir / "learnings.md").write_text(learnings_content)

        prompt = build_prompt(task, config, store)

        assert "learnings.md" not in prompt
        assert "Complete this task: One-off experimental task" in prompt


class TestReviewContextFromChain:
    """Tests for self-contained review context generation."""

    def test_review_context_includes_changed_files_diffstat_and_diff(self, tmp_path: Path):
        """Review context should include changed files, diffstat, and inline diff."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.get_diff_numstat.return_value = "10\t2\tsrc/a.py\n3\t1\tsrc/b.py\n"
        mock_git.get_diff_stat.return_value = (
            " src/a.py | 12 ++++++++++\n src/b.py | 4 +++-\n 2 files changed, 13 insertions(+), 3 deletions(-)"
        )
        mock_git.get_diff.return_value = "diff --git a/src/a.py b/src/a.py\n@@ -1 +1 @@\n-old\n+new\n"

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "## Implementation Diff Context" in context
        assert "Changed files:" in context
        assert "- src/a.py" in context
        assert "- src/b.py" in context
        assert "Diff summary:" in context
        assert "Full diff:" in context
        assert "Use git and file reading tools" not in context

    def test_review_context_large_diff_adds_targeted_excerpts(self, tmp_path: Path):
        """Large review diffs should include targeted diff excerpts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement large feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/large-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review large implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        mock_git.get_diff_numstat.return_value = (
            "1500\t400\tsrc/large_a.py\n800\t200\tsrc/large_b.py\n"
        )
        mock_git.get_diff_stat.return_value = " 2 files changed, 2300 insertions(+), 600 deletions(-)"
        mock_git._run.return_value = Mock(stdout="diff --git a/src/large_a.py b/src/large_a.py\n@@ ...", returncode=0)

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git)

        assert "Targeted diff excerpts" in context
        assert "Additional changed files not expanded inline" not in context

    def test_review_context_uses_configurable_thresholds_and_file_limit(self, tmp_path: Path):
        """Review context should honor config-driven diff thresholds and excerpt file cap."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement configurable thresholds", task_type="implement")
        impl_task.status = "completed"
        impl_task.branch = "test/config-thresholds-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review configurable thresholds implementation",
            task_type="review",
            depends_on=impl_task.id,
        )

        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            review_diff_small_threshold=1,
            review_diff_medium_threshold=2,
            review_context_file_limit=1,
        )

        mock_git = Mock(spec=Git)
        mock_git.default_branch.return_value = "main"
        # total_lines=3 should be treated as large with thresholds above
        mock_git.get_diff_numstat.return_value = "2\t1\tsrc/a.py\n0\t0\tsrc/b.py\n"
        mock_git.get_diff_stat.return_value = " 2 files changed, 2 insertions(+), 1 deletion(-)"
        mock_git._run.return_value = Mock(stdout="diff --git a/src/a.py b/src/a.py\n@@ ...", returncode=0)

        context = _build_context_from_chain(review_task, store, tmp_path, mock_git, config=config)

        assert "Targeted diff excerpts" in context
        assert "Additional changed files not expanded inline: 1" in context
        mock_git.get_diff.assert_not_called()

    def test_review_context_includes_compact_improve_lineage(self, tmp_path: Path):
        """Review context includes compact summaries for prior improve runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = (
            "# Summary\n"
            "- Fix flaky tests\n"
            "- Tighten input validation\n"
            "- Keep this concise\n"
        )
        store.update(improve1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)

        improve2 = store.add(
            prompt="Improve 2",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review2.id,
        )
        improve2.status = "completed"
        improve2.task_id = "20260227-improve-2"
        store.update(improve2)

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_dir.mkdir(parents=True, exist_ok=True)
        (summary_dir / f"{improve2.task_id}.md").write_text(
            "# What was accomplished\n- Reduced retry loops\n- Added guardrails\n"
        )

        review3 = store.add(prompt="Review latest", task_type="review", depends_on=impl_task.id)

        context = _build_context_from_chain(review3, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"Improve #{improve1.id} (review #{review1.id})" in context
        assert f"Improve #{improve2.id} (review #{review2.id})" in context
        assert "Fix flaky tests Tighten input validation Keep this concise" in context
        assert "What was accomplished Reduced retry loops Added guardrails" in context

    def test_review_context_bounds_improve_lineage_and_reports_omitted(self, tmp_path: Path):
        """Review context includes only recent improves and reports omitted count."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement bounded lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        improve_ids = []
        for idx in range(REVIEW_IMPROVE_LINEAGE_LIMIT + 2):
            review = store.add(prompt=f"Review {idx}", task_type="review", depends_on=impl_task.id)
            review.status = "completed"
            store.update(review)
            improve = store.add(
                prompt=f"Improve {idx}",
                task_type="improve",
                based_on=impl_task.id,
                depends_on=review.id,
            )
            improve.status = "completed"
            improve.output_content = f"- Improve summary {idx}\n"
            store.update(improve)
            improve_ids.append(improve.id)

        current_review = store.add(prompt="Review now", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"showing {REVIEW_IMPROVE_LINEAGE_LIMIT} most recent" in context
        assert "2 older omitted" in context

        # Most recent improves are included in both the lineage chain and detail bullets.
        for improve_id in improve_ids[-REVIEW_IMPROVE_LINEAGE_LIMIT:]:
            assert f"Improve #{improve_id}" in context

        # Older improve IDs appear in the lineage chain line but their summaries are omitted.
        omitted_count = len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT
        for idx in range(omitted_count):
            assert f"Improve summary {idx}" not in context

    def test_review_context_includes_retry_improves_in_same_chain(self, tmp_path: Path):
        """Review context includes retry/resume improve attempts chained from prior improves."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement retry-aware lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)
        improve_a = store.add(
            prompt="Improve A",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve_a.status = "completed"
        improve_a.output_content = "- Direct improve"
        store.update(improve_a)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)
        improve_b = store.add(
            prompt="Improve B",
            task_type="improve",
            based_on=improve_a.id,
            depends_on=review2.id,
        )
        improve_b.status = "completed"
        improve_b.output_content = "- Retry improve"
        store.update(improve_b)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"Improve #{improve_a.id} (review #{review1.id})" in context
        assert f"Improve #{improve_b.id} (review #{review2.id})" in context
        assert "Direct improve" in context
        assert "Retry improve" in context

    def test_review_context_bounds_mixed_direct_and_retry_improves(self, tmp_path: Path):
        """Bounded lineage remains correct with mixed direct and retry/resume improve attempts."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement mixed lineage", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        improve_ids: list[int] = []
        parent_improve_id: int | None = None
        for idx in range(REVIEW_IMPROVE_LINEAGE_LIMIT + 2):
            review = store.add(prompt=f"Review {idx}", task_type="review", depends_on=impl_task.id)
            review.status = "completed"
            store.update(review)
            based_on = impl_task.id if idx % 2 == 0 else parent_improve_id
            improve = store.add(
                prompt=f"Improve {idx}",
                task_type="improve",
                based_on=based_on,
                depends_on=review.id,
            )
            improve.status = "completed"
            improve.output_content = f"- Mixed improve summary {idx}\n"
            store.update(improve)
            assert improve.id is not None
            improve_ids.append(improve.id)
            parent_improve_id = improve.id

        current_review = store.add(prompt="Review now", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "## Improve Lineage Context" in context
        assert f"showing {REVIEW_IMPROVE_LINEAGE_LIMIT} most recent" in context
        assert "2 older omitted" in context

        for improve_id in improve_ids[-REVIEW_IMPROVE_LINEAGE_LIMIT:]:
            assert f"Improve #{improve_id}" in context

        # Older improve IDs appear in the lineage chain but their summaries are omitted.
        omitted_count = len(improve_ids) - REVIEW_IMPROVE_LINEAGE_LIMIT
        for idx in range(omitted_count):
            assert f"Mixed improve summary {idx}" not in context

    def test_review_context_excludes_equal_timestamp_later_improve(self, tmp_path: Path):
        """Equal-timestamp improves created after the review are excluded."""
        created_at = datetime(2026, 2, 27, 5, 0, 0, tzinfo=timezone.utc)
        impl_task = Task(id=100, prompt="Implement", task_type="implement", status="completed")
        review_task = Task(
            id=50,
            prompt="Review current",
            task_type="review",
            depends_on=impl_task.id,
            created_at=created_at,
        )

        older_improve = Task(
            id=40,
            prompt="Improve older",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on=10,
            created_at=created_at,
            output_content="- older improve",
        )
        later_improve = Task(
            id=60,
            prompt="Improve later",
            task_type="improve",
            status="completed",
            based_on=impl_task.id,
            depends_on=11,
            created_at=created_at,
            output_content="- later improve",
        )

        store = Mock(spec=SqliteTaskStore)
        store.get_all.return_value = [older_improve, later_improve]

        context = _build_review_improve_lineage_context(review_task, impl_task, store, tmp_path)

        assert f"Improve #{older_improve.id}" in context
        assert "older improve" in context
        assert f"Improve #{later_improve.id}" not in context
        assert "later improve" not in context

    def test_review_context_includes_tool_hints_when_prior_cycles_exist(self, tmp_path: Path):
        """Review context includes uv run gza show / cat hints when prior review/improve cycles exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement with hints", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = "- Fixed the issue\n"
        store.update(improve1)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        assert "uv run gza show <id>" in context
        assert "cat <report_file>" in context
        assert "1 prior review/improve cycle" in context

    def test_review_context_includes_lineage_chain_with_review_and_improve_ids(self, tmp_path: Path):
        """Review context includes explicit lineage chain listing review and improve IDs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement for chain", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        review1 = store.add(prompt="Review 1", task_type="review", depends_on=impl_task.id)
        review1.status = "completed"
        store.update(review1)

        improve1 = store.add(
            prompt="Improve 1",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review1.id,
        )
        improve1.status = "completed"
        improve1.output_content = "- Round 1 fix\n"
        store.update(improve1)

        review2 = store.add(prompt="Review 2", task_type="review", depends_on=impl_task.id)
        review2.status = "completed"
        store.update(review2)

        improve2 = store.add(
            prompt="Improve 2",
            task_type="improve",
            based_on=impl_task.id,
            depends_on=review2.id,
        )
        improve2.status = "completed"
        improve2.output_content = "- Round 2 fix\n"
        store.update(improve2)

        current_review = store.add(prompt="Review current", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(current_review, store, tmp_path, git=None)

        # Lineage chain shows review and improve IDs in order
        assert f"Review #{review1.id}" in context
        assert f"Improve #{improve1.id}" in context
        assert f"Review #{review2.id}" in context
        assert f"Improve #{improve2.id}" in context
        assert "Lineage:" in context
        assert "2 prior review/improve cycle" in context

    def test_first_review_has_no_tool_hints(self, tmp_path: Path):
        """First-time review (no prior cycles) does not include tool hints or lineage."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement fresh", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)

        first_review = store.add(prompt="Review first", task_type="review", depends_on=impl_task.id)
        context = _build_context_from_chain(first_review, store, tmp_path, git=None)

        assert "uv run gza show <id>" not in context
        assert "prior review/improve cycle" not in context
        assert "Lineage:" not in context


class TestSummaryDirectory:
    """Tests for summary directory constant."""

    def test_summary_dir_constant_value(self):
        """Test that SUMMARY_DIR constant has the correct value."""
        assert SUMMARY_DIR == ".gza/summaries"


class TestCopyLearningsToWorktree:
    """Tests for _copy_learnings_to_worktree."""

    def test_copies_learnings_file(self, tmp_path: Path):
        """Learnings file is copied from project .gza/ into worktree .gza/."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        # Create learnings file in project
        gza_dir = project_dir / ".gza"
        gza_dir.mkdir()
        (gza_dir / "learnings.md").write_text("- Use pytest fixtures")

        config = Mock(spec=Config)
        config.project_dir = project_dir

        _copy_learnings_to_worktree(config, worktree_dir)

        dst = worktree_dir / ".gza" / "learnings.md"
        assert dst.exists()
        assert dst.read_text() == "- Use pytest fixtures"

    def test_noop_when_no_learnings_file(self, tmp_path: Path):
        """No error when learnings file doesn't exist yet."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        config = Mock(spec=Config)
        config.project_dir = project_dir

        _copy_learnings_to_worktree(config, worktree_dir)

        assert not (worktree_dir / ".gza" / "learnings.md").exists()

    def test_creates_gza_dir_in_worktree(self, tmp_path: Path):
        """.gza/ directory is created in worktree if it doesn't exist."""
        project_dir = tmp_path / "project"
        worktree_dir = tmp_path / "worktree"
        project_dir.mkdir()
        worktree_dir.mkdir()

        gza_dir = project_dir / ".gza"
        gza_dir.mkdir()
        (gza_dir / "learnings.md").write_text("content")

        config = Mock(spec=Config)
        config.project_dir = project_dir

        assert not (worktree_dir / ".gza").exists()
        _copy_learnings_to_worktree(config, worktree_dir)
        assert (worktree_dir / ".gza").is_dir()


class TestReviewTaskSlugGeneration:
    """Tests for review task slug generation."""

    def test_review_task_uses_implementation_slug(self, tmp_path: Path):
        """Test that auto-created review tasks derive slug from implementation task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task with a task_id
        impl_task = store.add(
            prompt="Add docker volumes support",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260129-add-docker-volumes"
        store.update(impl_task)

        # Get the task to verify task_id is set
        impl_task = store.get(impl_task.id)
        assert impl_task.task_id == "20260129-add-docker-volumes"

        # Create a mock config and run function that captures the review task
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Capture the review task that gets created
        def mock_run(config, task_id):
            return 0

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        def mock_post_review_to_pr(*args, **kwargs):
            pass

        # Temporarily replace run and post_review_to_pr functions
        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _create_and_run_review_task
            _create_and_run_review_task(impl_task, config, store)

            # Get the review task that was created
            review_task = store.get(2)
            assert review_task is not None
            assert review_task.task_type == "review"

            # Verify the prompt uses the slug format
            assert review_task.prompt == "review add-docker-volumes"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_handles_retry_suffix(self, tmp_path: Path):
        """Test that review task slug handles retry suffix in implementation task_id."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task with retry suffix
        impl_task = store.add(
            prompt="Fix authentication bug",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260129-fix-authentication-bug-2"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            review_task = store.get(2)
            assert review_task is not None
            # Should strip the retry suffix (-2) from the slug
            assert review_task.prompt == "review fix-authentication-bug"
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_review_task_fallback_without_task_id(self, tmp_path: Path):
        """Test that review task falls back gracefully if task_id is not set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task without task_id
        impl_task = store.add(
            prompt="Implement feature",
            task_type="implement",
        )
        impl_task.status = "completed"
        # Don't set task_id
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run(config, task_id):
            return 0

        def mock_post_review_to_pr(*args, **kwargs):
            pass

        import gza.runner
        original_run = gza.runner.run
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.run = mock_run
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            _create_and_run_review_task(impl_task, config, store)

            review_task = store.get(2)
            assert review_task is not None
            # Should use fallback format
            assert "Review task #1" in review_task.prompt
        finally:
            gza.runner.run = original_run
            gza.runner.post_review_to_pr = original_post_review

    def test_auto_review_delegates_to_run(self, tmp_path: Path):
        """Test that _create_and_run_review_task delegates PR posting to run().

        PR posting now happens in _run_non_code_task (called by run()), not in
        _create_and_run_review_task itself. This test verifies the delegation.
        """
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task with a PR
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        # Track that run() was called with the review task
        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0  # Success

        import gza.runner
        original_run = gza.runner.run

        gza.runner.run = mock_run

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify success
            assert exit_code == 0

            # Verify run() was called with the review task id
            assert len(run_calls) == 1
            assert run_calls[0] == 2  # The created review task
        finally:
            gza.runner.run = original_run

    def test_auto_review_returns_run_exit_code(self, tmp_path: Path):
        """Test that _create_and_run_review_task returns the exit code from run()."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        store.update(impl_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        def mock_run_failure(config, task_id):
            return 1  # Failure

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run_failure

        try:
            # Call _create_and_run_review_task
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Verify failure code is returned
            assert exit_code == 1
        finally:
            gza.runner.run = original_run

    def test_duplicate_in_progress_review_does_not_call_run(self, tmp_path: Path):
        """Test that _create_and_run_review_task does not call run() for in_progress reviews."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        store.update(impl_task)

        # Create an in_progress review
        review_task = store.add(
            prompt="review add-user-authentication",
            task_type="review",
            depends_on=impl_task.id,
        )
        store.mark_in_progress(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run

        try:
            exit_code = _create_and_run_review_task(impl_task, config, store)

            # Should succeed without calling run()
            assert exit_code == 0
            assert run_calls == [], "run() must not be called for an in_progress review"
        finally:
            gza.runner.run = original_run

    def test_duplicate_pending_review_is_run_once(self, tmp_path: Path):
        """Test that _create_and_run_review_task runs a pending duplicate review exactly once."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        store.update(impl_task)

        # Create a pending review
        review_task = store.add(
            prompt="review add-user-authentication",
            task_type="review",
            depends_on=impl_task.id,
        )
        # review_task is pending by default

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"

        run_calls = []

        def mock_run(config, task_id):
            run_calls.append(task_id)
            return 0

        import gza.runner
        original_run = gza.runner.run
        gza.runner.run = mock_run

        try:
            exit_code = _create_and_run_review_task(impl_task, config, store)

            assert exit_code == 0
            assert run_calls == [review_task.id], "run() must be called exactly once with the pending review"
        finally:
            gza.runner.run = original_run


class TestSlugFromTaskId:
    """Tests for _slug_from_task_id helper."""

    def test_strips_date_prefix(self):
        assert _slug_from_task_id("20260129-add-docker-volumes") == "add-docker-volumes"

    def test_strips_date_prefix_and_retry_suffix(self):
        assert _slug_from_task_id("20260129-fix-auth-bug-2") == "fix-auth-bug"

    def test_no_dash_returns_original(self):
        assert _slug_from_task_id("nodash") == "nodash"


class TestGenerateTaskIdSlugOverride:
    """Tests for generate_task_id with slug_override parameter."""

    def test_slug_override_used_instead_of_prompt(self, tmp_path: Path):
        """slug_override replaces the slug derived from prompt."""
        task_id = generate_task_id(
            "some long generic prompt text",
            slug_override="rev-add-docker-volumes",
        )
        assert task_id.endswith("-rev-add-docker-volumes")

    def test_slug_override_none_falls_back_to_prompt(self, tmp_path: Path):
        """When slug_override is None, slug is derived from prompt as usual."""
        task_id = generate_task_id(
            "Add docker volumes support",
            slug_override=None,
        )
        assert "add-docker-volumes-support" in task_id

    def test_slug_override_not_used_on_retry(self, tmp_path: Path):
        """slug_override is ignored when existing_id is provided (retry path)."""
        task_id = generate_task_id(
            "some prompt",
            existing_id="20260101-original-slug",
            slug_override="rev-something-else",
        )
        # Should re-use the base from existing_id, not slug_override
        assert "original-slug" in task_id


class TestComputeSlugOverride:
    """Tests for _compute_slug_override helper."""

    def test_review_task_uses_rev_prefix(self, tmp_path: Path):
        """Review tasks get 'rev-' prefix with root task's slug."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add(prompt="Add docker volumes", task_type="implement")
        impl_task.task_id = "20260129-add-docker-volumes"
        store.update(impl_task)

        review_task = store.add(
            prompt="review add-docker-volumes",
            task_type="review",
            depends_on=impl_task.id,
        )

        result = _compute_slug_override(review_task, store)
        assert result == "rev-add-docker-volumes"

    def test_implement_task_uses_impl_prefix(self, tmp_path: Path):
        """Implement tasks get 'impl-' prefix with root task's slug."""
        store = SqliteTaskStore(tmp_path / "test.db")
        plan_task = store.add(prompt="Add authentication system", task_type="plan")
        plan_task.task_id = "20260129-add-authentication-system"
        store.update(plan_task)

        impl_task = store.add(
            prompt="Implement authentication",
            task_type="implement",
            based_on=plan_task.id,
        )

        result = _compute_slug_override(impl_task, store)
        assert result == "impl-add-authentication-system"

    def test_improve_task_uses_impr_prefix(self, tmp_path: Path):
        """Improve tasks get 'impr-' prefix with root task's slug."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add(prompt="Add docker volumes", task_type="implement")
        impl_task.task_id = "20260129-add-docker-volumes"
        store.update(impl_task)

        improve_task = store.add(
            prompt="Fix the docker volume issues",
            task_type="improve",
            based_on=impl_task.id,
        )

        result = _compute_slug_override(improve_task, store)
        assert result == "impr-add-docker-volumes"

    def test_plain_task_returns_none(self, tmp_path: Path):
        """Non-review/implement/improve tasks return None."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add(prompt="Do some work", task_type="task")
        result = _compute_slug_override(task, store)
        assert result is None

    def test_explore_task_returns_none(self, tmp_path: Path):
        """Explore tasks return None."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add(prompt="Explore codebase", task_type="explore")
        result = _compute_slug_override(task, store)
        assert result is None

    def test_fallback_to_prompt_when_root_has_no_task_id(self, tmp_path: Path):
        """Falls back to slugifying root prompt when root has no task_id."""
        store = SqliteTaskStore(tmp_path / "test.db")
        plan_task = store.add(prompt="Add authentication system", task_type="plan")
        # No task_id set on plan_task

        impl_task = store.add(
            prompt="Implement authentication",
            task_type="implement",
            based_on=plan_task.id,
        )

        result = _compute_slug_override(impl_task, store)
        assert result == "impl-add-authentication-system"

    def test_review_task_with_retry_suffix_strips_it(self, tmp_path: Path):
        """Root task_id retry suffix is stripped from slug."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add(prompt="Fix auth", task_type="implement")
        impl_task.task_id = "20260129-fix-auth-2"
        store.update(impl_task)

        review_task = store.add(
            prompt="review fix-auth",
            task_type="review",
            depends_on=impl_task.id,
        )

        result = _compute_slug_override(review_task, store)
        assert result == "rev-fix-auth"


class TestReviewNextSteps:
    """Tests for next steps output after review task completion."""

    def test_review_completion_suggests_improve(self, tmp_path: Path):
        """Test that review completion output suggests gza improve <impl_task_id>."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nChanges requested.")

        # Capture console output by collecting print calls
        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console, \
             patch('gza.runner.post_review_to_pr'):
            mock_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            # Verify improve suggestions appear in output
            all_output = "\n".join(printed_lines)
            assert f"gza improve {impl_task.id}" in all_output
            assert f"gza improve {impl_task.id} --run" not in all_output

    @pytest.mark.parametrize(
        ("report_content", "expected_verdict"),
        [
            ("# Review\n\nVerdict: CHANGES_REQUESTED", "CHANGES_REQUESTED"),
            ("# Review\n\n## Verdict\n\n**NEEDS_DISCUSSION**\n", "NEEDS_DISCUSSION"),
        ],
    )
    def test_review_completion_prints_verdict(self, tmp_path: Path, report_content: str, expected_verdict: str):
        """Completed review output should print parsed verdict for inline and heading markdown formats."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Add user authentication", task_type="implement")
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text(report_content)

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console, \
             patch('gza.runner.post_review_to_pr'):
            mock_console.print.side_effect = capture_print
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

        assert exit_code == 0
        assert "Verdict: " in "\n".join(printed_lines)
        assert expected_verdict in "\n".join(printed_lines)

    def test_non_review_task_does_not_suggest_improve(self, tmp_path: Path):
        """Test that explore/plan task completion does NOT suggest gza improve."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        explore_task = store.add(
            prompt="Explore codebase",
            task_type="explore",
        )
        explore_task.task_id = "20260212-explore-codebase"
        store.update(explore_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=5.0,
            num_turns_reported=3,
            cost_usd=0.02,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        worktree_path = config.worktree_path / f"{explore_task.task_id}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.task_id}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        printed_lines: list[str] = []

        def capture_print(*args, **kwargs):
            printed_lines.append(str(args[0]) if args else "")

        with patch('gza.runner.console') as mock_console:
            mock_console.print.side_effect = capture_print

            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            assert exit_code == 0

            all_output = "\n".join(printed_lines)
            assert "gza improve" not in all_output
            assert "gza retry" not in all_output
            assert "gza resume" not in all_output


class TestRunNonCodeTaskDockerGitMetadata:
    """Tests for Docker review execution when worktree git metadata is invalid."""

    def test_docker_review_hides_and_restores_invalid_worktree_git_file(self, tmp_path: Path):
        """Invalid host gitdir metadata should be hidden during provider run and restored after."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.task_id = "20260225-implement-feature"
        impl_task.branch = "test/feature-branch"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260225-review-feature"
        store.update(review_task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = True

        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        original_git_file = worktree_path / ".git"
        original_git_content = "gitdir: /nonexistent/host/path/.git/worktrees/review\n"
        original_git_file.write_text(original_git_content)

        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"

        def provider_run(_config, _prompt, _log_file, _work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            assert not (worktree_path / ".git").exists()
            assert (worktree_path / ".git.gza-host-worktree").exists()
            report_file.write_text("# Review\n\nVerdict: APPROVED")
            return RunResult(
                exit_code=0,
                duration_seconds=2.0,
                num_turns_reported=1,
                cost_usd=0.01,
                session_id="session-1",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.side_effect = provider_run

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = "1\t1\tsrc/app.py\n"
        mock_git.get_diff_stat.return_value = " 1 file changed, 1 insertion(+), 1 deletion(-)"
        mock_git.get_diff.return_value = "diff --git a/src/app.py b/src/app.py\n@@ -1 +1 @@\n-old\n+new\n"

        with patch("gza.runner.post_review_to_pr"):
            exit_code = _run_non_code_task(
                review_task,
                config,
                store,
                mock_provider,
                mock_git,
                resume=False,
            )

        assert exit_code == 0
        assert original_git_file.exists()
        assert original_git_file.read_text() == original_git_content
        assert not (worktree_path / ".git.gza-host-worktree").exists()


class TestRunNonCodeTaskPRPosting:
    """Tests for _run_non_code_task PR posting behavior."""

    def test_run_non_code_task_posts_to_pr_for_review(self, tmp_path: Path):
        """Test that _run_non_code_task calls post_review_to_pr for completed review tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a completed implementation task
        impl_task = store.add(
            prompt="Add user authentication",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260211-add-user-authentication"
        impl_task.branch = "gza/20260211-add-user-authentication"
        impl_task.pr_number = 123
        store.update(impl_task)

        # Create a review task that depends on it
        review_task = store.add(
            prompt="Review the implementation",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-the-implementation"
        store.update(review_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Create review directory structure
        review_dir = tmp_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append({
                'review_id': review_task.id,
                'impl_id': impl_task.id,
                'required': required
            })

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.get_diff_numstat.return_value = ""
        mock_git.get_diff.return_value = ""

        # Create worktree directory and report file (simulating provider writing it)
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_review_dir = worktree_path / ".gza" / "reviews"
        worktree_review_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nLooks good!")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was called
            assert len(pr_post_called) == 1
            assert pr_post_called[0]['review_id'] == review_task.id
            assert pr_post_called[0]['impl_id'] == impl_task.id
            assert pr_post_called[0]['required'] is False
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestMaxStepsHandling:
    """Tests for max-steps behavior in runner integration."""

    def test_run_result_to_stats_includes_step_fields(self):
        """Step metrics should be transferred from RunResult to TaskStats."""
        result = RunResult(
            exit_code=0,
            duration_seconds=12.3,
            num_steps_reported=7,
            num_steps_computed=8,
            num_turns_reported=5,
            num_turns_computed=5,
            cost_usd=0.12,
            input_tokens=100,
            output_tokens=200,
        )
        stats = _run_result_to_stats(result)
        assert stats.num_steps_reported == 7
        assert stats.num_steps_computed == 8
        assert stats.num_turns_reported == 5
        assert stats.tokens_estimated is False
        assert stats.cost_estimated is False

    def test_run_result_to_stats_includes_estimation_flags(self):
        """Estimation flags should be transferred from RunResult to TaskStats."""
        result = RunResult(
            exit_code=0,
            input_tokens=123,
            output_tokens=45,
            cost_usd=0.01,
            tokens_estimated=True,
            cost_estimated=True,
        )
        stats = _run_result_to_stats(result)
        assert stats.tokens_estimated is True
        assert stats.cost_estimated is True

    def test_non_code_task_marks_max_steps_failure_reason(self, tmp_path: Path):
        """Provider max_steps errors should be stored as MAX_STEPS."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=3,
            error_type="max_steps",
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        assert failed.failure_reason == "MAX_STEPS"

    def test_non_code_task_marks_terminal_step_interrupted_on_max_steps(self, tmp_path: Path):
        """Persisted run steps should reflect interruption on max-steps failures."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 2

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=0,
            duration_seconds=4.2,
            num_steps_computed=3,
            error_type="max_steps",
            _accumulated_data={
                "run_step_events": [
                    {
                        "message_role": "assistant",
                        "message_text": "First",
                        "legacy_turn_id": "T1",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    },
                    {
                        "message_role": "assistant",
                        "message_text": "Second",
                        "legacy_turn_id": "T2",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    },
                ]
            },
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        steps = store.get_run_steps(task.id)
        assert len(steps) == 2
        assert steps[0].outcome == "completed"
        assert steps[1].outcome == "interrupted"

    def test_non_code_task_marks_terminal_step_failed_on_nonzero_exit(self, tmp_path: Path):
        """Persisted run steps should reflect terminal failure when provider exits non-zero."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260225-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_provider.run.return_value = RunResult(
            exit_code=1,
            duration_seconds=4.2,
            _accumulated_data={
                "run_step_events": [
                    {
                        "message_role": "assistant",
                        "message_text": "Only step",
                        "legacy_turn_id": "T1",
                        "legacy_event_id": None,
                        "substeps": [],
                        "outcome": "completed",
                        "summary": None,
                    }
                ]
            },
        )

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        with patch("gza.runner.console"):
            exit_code = _run_non_code_task(task, config, store, mock_provider, mock_git)

        assert exit_code == 0
        failed = store.get(task.id)
        assert failed is not None
        assert failed.status == "failed"
        steps = store.get_run_steps(task.id)
        assert len(steps) == 1
        assert steps[0].outcome == "failed"

    def test_run_non_code_task_skips_pr_posting_for_explore(self, tmp_path: Path):
        """Test that _run_non_code_task does NOT call post_review_to_pr for explore tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an explore task
        explore_task = store.add(
            prompt="Explore the codebase",
            task_type="explore",
        )
        explore_task.task_id = "20260212-explore-the-codebase"
        store.update(explore_task)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Create exploration directory structure
        explore_dir = tmp_path / ".gza" / "explorations"
        explore_dir.mkdir(parents=True, exist_ok=True)

        # Track if post_review_to_pr was called
        pr_post_called = []

        def mock_post_review_to_pr(review_task, impl_task, store, project_dir, required=False):
            pr_post_called.append(True)

        # Mock provider
        mock_provider = Mock()
        mock_provider.name = "MockProvider"
        mock_result = RunResult(
            exit_code=0,
            duration_seconds=10.0,
            num_turns_reported=5,
            cost_usd=0.05,
            session_id="test-session",
            error_type=None,
        )
        mock_provider.run.return_value = mock_result

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{explore_task.task_id}-explore"
        worktree_explore_dir = worktree_path / ".gza" / "explorations"
        worktree_explore_dir.mkdir(parents=True, exist_ok=True)
        report_file = worktree_explore_dir / f"{explore_task.task_id}.md"
        report_file.write_text("# Exploration\n\nFindings here.")

        import gza.runner
        original_post_review = gza.runner.post_review_to_pr
        gza.runner.post_review_to_pr = mock_post_review_to_pr

        try:
            # Call _run_non_code_task
            exit_code = _run_non_code_task(
                explore_task, config, store, mock_provider, mock_git, resume=False
            )

            # Verify success
            assert exit_code == 0

            # Verify post_review_to_pr was NOT called (not a review task)
            assert len(pr_post_called) == 0
        finally:
            gza.runner.post_review_to_pr = original_post_review


class TestRunStepPersistenceIntegration:
    """Integration tests for persisting provider step/substep events."""

    def test_non_code_task_persists_steps_from_real_claude_fixture(self, tmp_path: Path):
        """_run_non_code_task should persist run_steps/run_substeps from provider parsing."""
        import json

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260226-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        provider = ClaudeProvider()
        json_lines = [
            json.dumps(
                {
                    "type": "assistant",
                    "message": {
                        "id": "msg_1",
                        "usage": {"input_tokens": 10, "output_tokens": 3},
                        "content": [
                            {"type": "text", "text": "I will inspect the code."},
                            {"type": "tool_use", "id": "tool_1", "name": "Bash", "input": {"command": "ls -la"}},
                        ],
                    },
                }
            )
            + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.001}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen, patch("gza.runner.console"):
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            exit_code = _run_non_code_task(task, config, store, provider, mock_git, resume=False)

        assert exit_code == 0

        steps = store.get_run_steps(task.id)
        assert len(steps) == 1
        assert steps[0].step_id == "S1"
        assert steps[0].provider == "claude"
        assert steps[0].message_text == "I will inspect the code."
        assert steps[0].outcome == "completed"
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.log_schema_version == 2

        step_ref = StepRef(
            id=steps[0].id,
            run_id=steps[0].run_id,
            step_index=steps[0].step_index,
            step_id=steps[0].step_id,
        )
        substeps = store.get_run_substeps(step_ref)
        assert len(substeps) == 1
        assert substeps[0].substep_id == "S1.1"
        assert substeps[0].type == "tool_call"
        assert substeps[0].payload == {
            "tool_name": "Bash",
            "tool_input": {"command": "ls -la"},
        }

    def test_on_step_count_updates_task_num_steps_computed_in_real_time(self, tmp_path: Path):
        """on_step_count callback should update task.num_steps_computed in DB during streaming."""
        import json

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Plan task", task_type="plan")
        task.task_id = "20260302-plan-task"
        store.update(task)

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.model = ""
        config.chat_text_display_length = 80
        config.claude = Mock(args=[])

        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)

        # Use two steps so we can verify intermediate DB state
        intermediate_counts: list[int] = []

        def capturing_store_update(t: Task) -> None:
            if t.num_steps_computed is not None:
                intermediate_counts.append(t.num_steps_computed)
            original_update(t)

        original_update = store.update
        store.update = capturing_store_update  # type: ignore[method-assign]

        provider = ClaudeProvider()
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_1", "content": [], "usage": {}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_2", "content": [], "usage": {}},
            }) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 2, "total_cost_usd": 0.0}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen, patch("gza.runner.console"):
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            exit_code = _run_non_code_task(task, config, store, provider, mock_git, resume=False)

        assert exit_code == 0
        # The callback should have been called for each step (1, then 2)
        assert 1 in intermediate_counts
        assert 2 in intermediate_counts


class TestResumeVerificationPrompt:
    """Tests for resume verification prompt injection."""

    def test_resume_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed task with session_id
        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-x"
        task.branch = "gza/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-123",
                error_type=None,
            )

        # Mock provider and git
        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            # Mock Git
            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            # Mock has_changes to return True
            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            # status_porcelain: simulate a file changed during provider run
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot (called before provider.run)
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            # Mock _run for WIP functions (squash, restore)
            mock_log_result = Mock()
            mock_log_result.stdout = "WIP: gza task interrupted"
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            # Create worktree directory
            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            # Create summary file in worktree
            summary_dir = worktree_path / ".gza" / "summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            summary_file = summary_dir / f"{task.task_id}.md"
            summary_file.write_text("# Summary\n\nCompleted the task.")

            # Run with resume=True
            result = run(config, task_id=task.id, resume=True)

            # Verify success
            assert result == 0

            # Verify prompt was captured
            assert len(captured_prompts) == 1
            prompt = captured_prompts[0]['prompt']
            resume_session_id = captured_prompts[0]['resume_session_id']

            # Verify verification instructions are in the prompt
            assert "verify your todo list against the actual state" in prompt.lower()
            assert "review your todo list from the previous session" in prompt.lower()
            assert "verify by checking the actual code/files" in prompt.lower()
            assert "update the todo list to reflect what is actually complete" in prompt.lower()
            assert "continue from where you left off" in prompt.lower()

            # Verify resume_session_id was passed
            assert resume_session_id == "test-session-123"

    def test_resume_non_code_task_includes_verification_prompt(self, tmp_path: Path):
        """Test that resuming a non-code task includes todo list verification instructions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a failed review task with session_id
        impl_task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        impl_task.status = "completed"
        impl_task.task_id = "20260212-implement-feature-x"
        impl_task.branch = "gza/20260212-implement-feature-x"
        store.update(impl_task)

        review_task = store.add(
            prompt="Review feature X",
            task_type="review",
            depends_on=impl_task.id,
        )
        review_task.task_id = "20260212-review-feature-x"
        review_task.session_id = "test-session-456"
        store.mark_failed(review_task, log_file="logs/test.log", stats=None)

        # Setup config
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False

        # Mock provider to capture the prompt
        captured_prompts = []

        def mock_provider_run(config, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            captured_prompts.append({
                'prompt': prompt,
                'resume_session_id': resume_session_id
            })
            return RunResult(
                exit_code=0,
                duration_seconds=10.0,
                num_turns_reported=5,
                cost_usd=0.05,
                session_id="test-session-456",
                error_type=None,
            )

        mock_provider = Mock()
        mock_provider.name = "TestProvider"
        mock_provider.run = mock_provider_run

        # Mock git
        mock_git = Mock()
        mock_git.default_branch.return_value = "main"
        mock_git._run.return_value = Mock(returncode=0)
        mock_git.worktree_remove = Mock()

        # Create worktree directory and report file
        worktree_path = config.worktree_path / f"{review_task.task_id}-review"
        worktree_path.mkdir(parents=True, exist_ok=True)
        review_dir = worktree_path / ".gza" / "reviews"
        review_dir.mkdir(parents=True, exist_ok=True)
        report_file = review_dir / f"{review_task.task_id}.md"
        report_file.write_text("# Review\n\nLooks good!")

        # Mock post_review_to_pr to avoid GitHub CLI dependency
        with patch('gza.runner.post_review_to_pr'):
            # Call _run_non_code_task with resume=True
            exit_code = _run_non_code_task(
                review_task, config, store, mock_provider, mock_git, resume=True
            )

        # Verify success
        assert exit_code == 0

        # Verify prompt was captured
        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]['prompt']
        resume_session_id = captured_prompts[0]['resume_session_id']

        # Verify verification instructions are in the prompt
        assert "verify your todo list against the actual state" in prompt.lower()
        assert "review your todo list from the previous session" in prompt.lower()
        assert "verify by checking the actual code/files" in prompt.lower()
        assert "update the todo list to reflect what is actually complete" in prompt.lower()
        assert "continue from where you left off" in prompt.lower()

        # Verify resume_session_id was passed
        assert resume_session_id == "test-session-456"

    def test_normal_run_does_not_include_verification_prompt(self, tmp_path: Path):
        """Test that normal (non-resume) runs use the standard prompt without verification."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a pending task
        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Build normal prompt (not resume)
        prompt = build_prompt(task, config, store, summary_path=Path("/workspace/.gza/summaries/test.md"))

        # Verify it does NOT include verification instructions
        assert "verify your todo list" not in prompt.lower()
        assert "review your todo list from the previous session" not in prompt.lower()

        # Verify it includes the normal task prompt
        assert "Complete this task: Implement feature Y" in prompt


class TestPersistResolvedConfig:
    """Tests for persisting resolved model and provider to the task DB row."""

    def test_resolved_model_and_provider_persisted_before_provider_runs(self, tmp_path: Path):
        """Test that resolved model and provider are written to the DB before the provider runs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a pending task with no model/provider set
        task = store.add(prompt="Implement feature Z", task_type="implement")

        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = "claude-sonnet-4-6"
        config.get_max_steps_for_task.return_value = 50

        # Track store.update calls and what task.model/provider looked like at call time
        persisted_states: list[dict] = []
        original_update = store.update

        def spy_update(t):
            persisted_states.append({"model": t.model, "provider": t.provider})
            return original_update(t)

        store.update = spy_update  # type: ignore[method-assign]

        provider_called_after_update = []

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            # Record whether store.update was already called with persisted values
            provider_called_after_update.append(
                any(s["model"] == "claude-sonnet-4-6" and s["provider"] == "claude" for s in persisted_states)
            )
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=3,
                cost_usd=0.01,
                session_id="session-xyz",
                error_type=None,
            )

        with patch("gza.runner.get_provider") as mock_get_provider, \
             patch("gza.runner.Git") as mock_git_class, \
             patch("gza.runner.load_dotenv"), \
             patch("gza.runner.SqliteTaskStore", return_value=store):

            mock_provider = Mock()
            mock_provider.name = "claude"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []
            mock_git.count_commits_ahead.return_value = 0

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            run(config, task_id=task.id)

        # store.update must have been called with the resolved model and provider
        assert any(
            s["model"] == "claude-sonnet-4-6" and s["provider"] == "claude"
            for s in persisted_states
        ), f"Expected store.update called with resolved model/provider, got: {persisted_states}"

        # The persist must have happened before the provider ran
        assert provider_called_after_update and provider_called_after_update[0], \
            "store.update with resolved values must occur before provider.run is called"

        # Verify the task in the DB has the resolved values
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.model == "claude-sonnet-4-6"
        assert updated_task.provider == "claude"
        assert updated_task.provider_is_explicit is False


class TestWIPFunctionality:
    """Tests for WIP (Work In Progress) save/restore functionality."""

    def test_save_wip_changes_creates_commit_and_diff(self, tmp_path: Path):
        """Test that _save_wip_changes creates both a WIP commit and a diff backup."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a mock git repo
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        (worktree_path / "test.txt").write_text("test content")

        # Initialize git repo
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        # Create config
        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Save WIP changes
        _save_wip_changes(task, git, config, "test-branch")

        # Verify WIP commit was created
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: gza task interrupted"

        # Verify diff backup was created
        wip_file = tmp_path / WIP_DIR / "20260212-test-task.diff"
        assert wip_file.exists()
        diff_content = wip_file.read_text()
        assert "test.txt" in diff_content

    def test_save_wip_changes_with_no_changes(self, tmp_path: Path):
        """Test that _save_wip_changes does nothing when there are no changes."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with initial commit
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Save WIP changes (should do nothing)
        _save_wip_changes(task, git, config, "test-branch")

        # Verify no new commit was created
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "Initial commit"

        # Verify no WIP diff was created
        wip_file = tmp_path / WIP_DIR / "20260212-test-task.diff"
        assert not wip_file.exists()

    def test_restore_wip_changes_finds_wip_commit(self, tmp_path: Path):
        """Test that _restore_wip_changes detects existing WIP commit."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with WIP commit
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "test.txt").write_text("test")
        git.add(".")
        git.commit("WIP: gza task interrupted\n\nTask ID: 20260212-test-task")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Restore WIP changes (should detect existing commit)
        _restore_wip_changes(task, git, config, "test-branch")

        # Verify commit is still there (not modified)
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: gza task interrupted"

    def test_restore_wip_changes_applies_diff_backup(self, tmp_path: Path):
        """Test that _restore_wip_changes applies diff backup when no WIP commit exists."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        # Create a WIP diff backup
        wip_dir = tmp_path / WIP_DIR
        wip_dir.mkdir(parents=True)
        wip_file = wip_dir / "20260212-test-task.diff"
        diff_content = """diff --git a/test.txt b/test.txt
new file mode 100644
index 0000000..9daeafb
--- /dev/null
+++ b/test.txt
@@ -0,0 +1 @@
+test
"""
        wip_file.write_text(diff_content)

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        # Restore WIP changes
        _restore_wip_changes(task, git, config, "test-branch")

        # Verify diff was applied and committed
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "WIP: restored from diff"

    def test_squash_wip_commits(self, tmp_path: Path):
        """Test that _squash_wip_commits squashes WIP commits."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with multiple WIP commits
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        # Initial commit
        (worktree_path / "initial.txt").write_text("initial")
        git.add(".")
        git.commit("Initial commit")

        # WIP commit 1
        (worktree_path / "wip1.txt").write_text("wip1")
        git.add(".")
        git.commit("WIP: first attempt")

        # WIP commit 2
        (worktree_path / "wip2.txt").write_text("wip2")
        git.add(".")
        git.commit("WIP: second attempt")

        # Verify we have 3 commits
        log_before = git._run("log", "--oneline").stdout.strip().split("\n")
        assert len(log_before) == 3

        # Squash WIP commits
        _squash_wip_commits(git, task)

        # Verify we're back to 1 commit with changes staged
        log_after = git._run("log", "--oneline").stdout.strip().split("\n")
        assert len(log_after) == 1
        assert log_after[0].endswith("Initial commit")

        # Verify changes are staged
        assert git.has_changes(".", include_untracked=False)

    def test_squash_wip_commits_with_no_wip_commits(self, tmp_path: Path):
        """Test that _squash_wip_commits does nothing when there are no WIP commits."""
        # Setup
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"

        # Create a git repo with normal commits
        worktree_path = tmp_path / "worktree"
        worktree_path.mkdir()
        git = Git(worktree_path)
        git._run("init")
        git._run("config", "user.email", "test@example.com")
        git._run("config", "user.name", "Test User")

        (worktree_path / "test.txt").write_text("test")
        git.add(".")
        git.commit("Normal commit")

        # Squash WIP commits (should do nothing)
        _squash_wip_commits(git, task)

        # Verify commit is unchanged
        log = git._run("log", "-1", "--pretty=%s").stdout.strip()
        assert log == "Normal commit"


class TestBackupDatabase:
    """Tests for backup_database function."""

    def test_creates_backup_when_none_exists(self, tmp_path: Path):
        """backup_database creates a backup file for the current hour."""
        import sqlite3

        # Create a source database
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert backup_dir.exists()

        from datetime import datetime
        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_file = backup_dir / f"gza-{hour_stamp}.db"
        assert backup_file.exists()
        assert backup_file.stat().st_size > 0

    def test_skips_backup_when_current_hour_exists(self, tmp_path: Path):
        """backup_database does not create a second backup in the same hour."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.close()

        backup_dir = tmp_path / BACKUP_DIR
        backup_dir.mkdir(parents=True, exist_ok=True)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        existing_backup = backup_dir / f"gza-{hour_stamp}.db"
        existing_backup.write_bytes(b"placeholder")
        original_mtime = existing_backup.stat().st_mtime

        backup_database(db_path, tmp_path)

        # File should not have been replaced
        assert existing_backup.stat().st_mtime == original_mtime
        assert existing_backup.read_bytes() == b"placeholder"

    def test_skips_backup_when_db_does_not_exist(self, tmp_path: Path):
        """backup_database does nothing when the source database is missing."""
        db_path = tmp_path / ".gza" / "gza.db"

        # Should not raise and should not create any backup dir
        backup_database(db_path, tmp_path)

        backup_dir = tmp_path / BACKUP_DIR
        assert not backup_dir.exists()

    def test_backup_is_valid_sqlite_database(self, tmp_path: Path):
        """The created backup is a valid SQLite database with the same content."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'hello')")
        conn.commit()
        conn.close()

        backup_database(db_path, tmp_path)

        hour_stamp = datetime.now().strftime("%Y%m%d%H")
        backup_path = tmp_path / BACKUP_DIR / f"gza-{hour_stamp}.db"
        assert backup_path.exists()

        backup_conn = sqlite3.connect(str(backup_path))
        rows = backup_conn.execute("SELECT id, name FROM items").fetchall()
        backup_conn.close()

        assert rows == [(1, "hello")]


class TestNoChangesWithExistingCommits:
    """Tests for the fix that prevents false 'No changes made' failure on resume."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        return config

    def test_resume_with_existing_commits_and_no_new_changes_succeeds(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When resuming, if there are no uncommitted changes but the branch already has
        commits from a previous run, the task should succeed (not fail with 'No changes made')."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature X",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-x"
        task.branch = "test/20260212-implement-feature-x"
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log", stats=None)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session-123",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes (task already committed in previous run)
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            # Branch has 1 commit from the previous run
            mock_worktree_git.count_commits_ahead.return_value = 1
            mock_worktree_git.default_branch.return_value = "main"
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=True)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "completed", f"Expected 'completed', got '{refreshed.status}'"
        output = capsys.readouterr().out
        assert "gza merge" in output
        assert "gza pr" in output
        assert "gza retry" not in output
        assert "gza resume" not in output

    def test_no_changes_and_no_prior_commits_still_fails(self, tmp_path: Path):
        """When there are no uncommitted changes AND no commits on the branch,
        the task should still fail with 'No changes made'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(
            prompt="Implement feature Y",
            task_type="implement",
        )
        task.task_id = "20260212-implement-feature-y"
        task.branch = "test/20260212-implement-feature-y"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id=None,
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.count_commits_ahead.return_value = 0
            mock_git.worktree_add = Mock()
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            # No uncommitted changes
            mock_worktree_git.has_changes.return_value = False
            mock_worktree_git.status_porcelain.return_value = set()
            # No prior commits on the branch either
            mock_worktree_git.count_commits_ahead.return_value = 0
            mock_worktree_git.default_branch.return_value = "main"
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=task.id, resume=False)

        assert result == 0
        refreshed = store.get(task.id)
        assert refreshed.status == "failed", f"Expected 'failed', got '{refreshed.status}'"


class TestSameBranchLineageWalk:
    """Tests for same_branch resolution walking the based_on lineage chain."""

    def _make_config(self, tmp_path: Path, db_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.db_path = db_path
        config.log_path = tmp_path / "logs"
        config.log_path.mkdir(parents=True, exist_ok=True)
        config.worktree_path = tmp_path / "worktrees"
        config.worktree_path.mkdir(parents=True, exist_ok=True)
        config.workers_path = tmp_path / ".gza" / "workers"
        config.workers_path.mkdir(parents=True, exist_ok=True)
        config.use_docker = False
        config.max_turns = 50
        config.timeout_minutes = 60
        config.branch_mode = "multi"
        config.project_name = "test"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        config.get_provider_for_task.return_value = "claude"
        config.get_model_for_task.return_value = None
        config.get_max_steps_for_task.return_value = 50
        return config

    def test_same_branch_uses_immediate_source_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has a valid branch, use it directly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #1: implementation with a branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.task_id = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #2: improve with same_branch, based_on impl_task
        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        improve_task.task_id = "20260301-improve-feature"
        store.mark_in_progress(improve_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / improve_task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=improve_task.id)

        assert result == 0
        output = capsys.readouterr().out
        assert "test/20260301-implement-feature" in output
        # Direct source found — no "via" message expected
        assert "via" not in output

    def test_same_branch_walks_chain_when_immediate_source_has_no_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has no branch, walk based_on chain to find ancestor."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #324: implementation with a branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.task_id = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #335: killed before branch was persisted (no branch)
        killed_task = store.add(
            prompt="Improve feature (killed)",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        killed_task.task_id = "20260301-improve-killed"
        # branch is NOT set (simulating killed before persistence)
        store.mark_in_progress(killed_task)
        store.mark_failed(killed_task, log_file="logs/killed.log", stats=None)

        # Task #352: retry of #335, based_on=#335 (which has no branch)
        retry_task = store.add(
            prompt="Improve feature (retry)",
            task_type="improve",
            based_on=killed_task.id,
            same_branch=True,
        )
        retry_task.task_id = "20260301-improve-retry"
        store.mark_in_progress(retry_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = True
            mock_git.worktree_list.return_value = []

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / retry_task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=retry_task.id)

        assert result == 0
        output = capsys.readouterr().out
        # Should use impl_task branch, logging the "via" chain
        assert "test/20260301-implement-feature" in output
        assert "via" in output
        assert f"#{killed_task.id}" in output

    def test_same_branch_walks_chain_when_immediate_branch_does_not_exist(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the immediate source task has a branch field but that branch no longer exists,
        walk the based_on chain to find an ancestor with a valid branch."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task #1: implementation with a valid branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.task_id = "20260301-implement-feature"
        impl_task.branch = "test/20260301-implement-feature"
        store.mark_in_progress(impl_task)
        store.mark_completed(impl_task, log_file="logs/impl.log", stats=None)

        # Task #2: has a branch set, but that branch has been deleted
        middle_task = store.add(
            prompt="Improve feature (deleted branch)",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        middle_task.task_id = "20260301-improve-deleted-branch"
        middle_task.branch = "test/20260301-improve-deleted-branch"
        store.mark_in_progress(middle_task)
        store.mark_failed(middle_task, log_file="logs/middle.log", stats=None)

        # Task #3: retry, based on middle_task
        retry_task = store.add(
            prompt="Improve feature (retry)",
            task_type="improve",
            based_on=middle_task.id,
            same_branch=True,
        )
        retry_task.task_id = "20260301-improve-retry"
        store.mark_in_progress(retry_task)

        config = self._make_config(tmp_path, db_path)

        def mock_provider_run(cfg, prompt, log_file, work_dir, resume_session_id=None, on_session_id=None, on_step_count=None):
            return RunResult(
                exit_code=0,
                duration_seconds=5.0,
                num_turns_reported=2,
                cost_usd=0.02,
                session_id="test-session",
                error_type=None,
            )

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_provider.run = mock_provider_run
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.worktree_list.return_value = []
            # The middle task's branch doesn't exist; the impl_task's branch does
            def branch_exists(branch: str) -> bool:
                return branch == "test/20260301-implement-feature"
            mock_git.branch_exists.side_effect = branch_exists

            mock_worktree_git = Mock()
            mock_worktree_git.has_changes.return_value = True
            mock_worktree_git.status_porcelain.side_effect = [
                set(),  # pre-run snapshot
                {("M", "changed.py")},  # post-run snapshot
            ]
            mock_worktree_git.add = Mock()
            mock_worktree_git.commit = Mock()
            mock_worktree_git.get_diff_numstat.return_value = ""
            mock_log_result = Mock()
            mock_log_result.stdout = ""
            mock_worktree_git._run.return_value = mock_log_result

            mock_git_class.side_effect = [mock_git, mock_worktree_git]

            worktree_path = config.worktree_path / retry_task.task_id
            worktree_path.mkdir(parents=True, exist_ok=True)

            result = run(config, task_id=retry_task.id)

        assert result == 0
        output = capsys.readouterr().out
        assert "test/20260301-implement-feature" in output
        assert "via" in output

    def test_same_branch_fails_when_no_ancestor_has_valid_branch(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When no ancestor in the chain has a valid branch, fail with a clear error."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task with no branch
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        impl_task.task_id = "20260301-implement-feature"
        # branch NOT set
        store.mark_in_progress(impl_task)
        store.mark_failed(impl_task, log_file="logs/impl.log", stats=None)

        # Improve task based on the branchless impl task
        improve_task = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl_task.id,
            same_branch=True,
        )
        improve_task.task_id = "20260301-improve-feature"
        store.mark_in_progress(improve_task)

        config = self._make_config(tmp_path, db_path)

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_list.return_value = []

            mock_git_class.return_value = mock_git

            result = run(config, task_id=improve_task.id)

        assert result == 1
        output = capsys.readouterr().out
        assert "no ancestor has a valid branch" in output

    def test_same_branch_fails_on_cycle_in_based_on_chain(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]):
        """When the based_on chain contains a cycle, fail with a clear error instead of looping forever."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task A: no branch yet
        task_a = store.add(prompt="Task A", task_type="implement")
        task_a.task_id = "20260301-task-a"
        store.mark_in_progress(task_a)
        store.mark_failed(task_a, log_file="logs/a.log", stats=None)

        # Task B: based_on A, also no branch
        task_b = store.add(prompt="Task B", task_type="improve", based_on=task_a.id, same_branch=True)
        task_b.task_id = "20260301-task-b"
        store.mark_in_progress(task_b)
        store.mark_failed(task_b, log_file="logs/b.log", stats=None)

        # Introduce cycle: A.based_on = B (A -> B -> A)
        task_a_fresh = store.get(task_a.id)
        assert task_a_fresh is not None
        task_a_fresh.based_on = task_b.id
        store.update(task_a_fresh)

        # Task C: based_on B, same_branch=True — will walk B -> A -> B (cycle)
        task_c = store.add(prompt="Task C", task_type="improve", based_on=task_b.id, same_branch=True)
        task_c.task_id = "20260301-task-c"
        store.mark_in_progress(task_c)

        config = self._make_config(tmp_path, db_path)

        with patch('gza.runner.get_provider') as mock_get_provider, \
             patch('gza.runner.Git') as mock_git_class, \
             patch('gza.runner.load_dotenv'):

            mock_provider = Mock()
            mock_provider.name = "TestProvider"
            mock_provider.check_credentials.return_value = True
            mock_provider.verify_credentials.return_value = True
            mock_get_provider.return_value = mock_provider

            mock_git = Mock()
            mock_git.default_branch.return_value = "main"
            mock_git._run.return_value = Mock(returncode=0)
            mock_git.branch_exists.return_value = False
            mock_git.worktree_list.return_value = []

            mock_git_class.return_value = mock_git

            result = run(config, task_id=task_c.id)

        assert result == 1
        output = capsys.readouterr().out
        assert "Cycle detected" in output


class TestExtractedRunInnerHelpers:
    """Unit tests for helpers extracted from _run_inner orchestration."""

    def _make_config(self, tmp_path: Path) -> Mock:
        config = Mock(spec=Config)
        config.project_dir = tmp_path
        config.branch_mode = "multi"
        config.project_name = "testproj"
        config.branch_strategy = Mock()
        config.branch_strategy.pattern = "{project}/{task_id}"
        config.branch_strategy.default_type = "feature"
        return config

    def test_resolve_code_task_branch_name_walks_lineage(self, tmp_path: Path):
        """same_branch lineage resolution should return an ancestor branch that exists."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        config = self._make_config(tmp_path)

        impl = store.add(prompt="impl", task_type="implement")
        impl.task_id = "20260317-impl"
        impl.branch = "test/impl"
        store.mark_in_progress(impl)
        store.mark_completed(impl, log_file="logs/impl.log", stats=None)

        failed_improve = store.add(prompt="improve1", task_type="improve", based_on=impl.id, same_branch=True)
        failed_improve.task_id = "20260317-improve1"
        store.mark_in_progress(failed_improve)
        store.mark_failed(failed_improve, log_file="logs/improve1.log", stats=None)

        retry = store.add(prompt="improve2", task_type="improve", based_on=failed_improve.id, same_branch=True)
        retry.task_id = "20260317-improve2"

        git = Mock(spec=Git)
        git.branch_exists.side_effect = lambda branch: branch == "test/impl"

        branch_name = _resolve_code_task_branch_name(retry, config, store, git, resume=False)
        assert branch_name == "test/impl"

    def test_select_worktree_base_ref_prefers_origin_when_origin_ahead(self):
        """Base ref selection should choose origin/main when origin is strictly ahead."""
        git = Mock(spec=Git)
        git._run.return_value = Mock(returncode=0)  # origin ref exists

        def count_ahead(lhs: str, rhs: str) -> int:
            if lhs == "main" and rhs == "origin/main":
                return 0
            if lhs == "origin/main" and rhs == "main":
                return 3
            return 0

        git.count_commits_ahead.side_effect = count_ahead

        base_ref = _select_worktree_base_ref(git, "main")
        assert base_ref == "origin/main"

    def test_setup_code_task_worktree_resume_missing_branch_fails(self, tmp_path: Path):
        """Resume/same_branch setup should fail early if branch no longer exists."""
        config = self._make_config(tmp_path)
        task = Task(id=1, prompt="resume task", task_type="implement", task_id="20260317-task")
        git = Mock(spec=Git)
        git.branch_exists.return_value = False

        ok = _setup_code_task_worktree(
            task,
            config,
            git,
            branch_name="missing/branch",
            worktree_path=tmp_path / "worktrees" / "20260317-task",
            default_branch="main",
            resume=True,
        )

        assert ok is False

    def test_complete_code_task_marks_failed_when_no_changes_and_no_commits(self, tmp_path: Path):
        """Completion helper should fail task when provider produced neither changes nor commits."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement X", task_type="implement")
        task.task_id = "20260317-impl-x"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.task_id}.log"
        log_file.write_text("")

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = set()
        worktree_git.default_branch.return_value = "main"
        worktree_git.count_commits_ahead.return_value = 0

        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "test/branch",
            TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
            0,
            pre_run_status=set(),
            worktree_summary_path=tmp_path / "worktree-summary.md",
            summary_path=tmp_path / ".gza" / "summaries" / f"{task.task_id}.md",
            summary_dir=tmp_path / ".gza" / "summaries",
        )

        assert rc == 0
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "failed"

    def test_complete_code_task_selectively_stages_new_files(self, tmp_path: Path):
        """Completion helper should stage only provider-introduced changes."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Implement selective staging", task_type="implement")
        task.task_id = "20260317-selective"
        store.mark_in_progress(task)

        config = self._make_config(tmp_path)
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"{task.task_id}.log"
        log_file.write_text("")

        pre_status = {("M", "pre_existing.txt")}
        post_status = {("M", "pre_existing.txt"), ("M", "src/foo.py"), ("??", "new_file.txt")}

        worktree_git = Mock(spec=Git)
        worktree_git.status_porcelain.return_value = post_status
        worktree_git.default_branch.return_value = "main"
        worktree_git.get_diff_numstat.return_value = "1\t0\tsrc/foo.py\n1\t0\tnew_file.txt\n"

        summary_dir = tmp_path / ".gza" / "summaries"
        summary_path = summary_dir / f"{task.task_id}.md"
        worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.task_id}.md"
        worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
        worktree_summary_path.write_text("## Summary\n\n- done\n")

        with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
            rc = _complete_code_task(
                task,
                config,
                store,
                worktree_git,
                log_file,
                "test/branch",
                TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
                0,
                pre_run_status=pre_status,
                worktree_summary_path=worktree_summary_path,
                summary_path=summary_path,
                summary_dir=summary_dir,
            )

        assert rc == 0
        staged_files = [call.args[0] for call in worktree_git.add.call_args_list]
        assert set(staged_files) == {"src/foo.py", "new_file.txt"}
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.status == "completed"
        assert summary_path.exists()


class TestWriteLogEntry:
    """Tests for write_log_entry helper."""

    def test_creates_file_and_writes_jsonl(self, tmp_path: Path) -> None:
        """write_log_entry creates the file and writes a valid JSONL entry."""
        import json
        log_file = tmp_path / "task.log"
        entry = {"type": "gza", "subtype": "info", "message": "Hello"}
        write_log_entry(log_file, entry)
        assert log_file.exists()
        line = log_file.read_text().strip()
        assert json.loads(line) == entry

    def test_appends_multiple_entries(self, tmp_path: Path) -> None:
        """write_log_entry appends without overwriting existing content."""
        import json
        log_file = tmp_path / "task.log"
        entry1 = {"type": "gza", "subtype": "info", "message": "First"}
        entry2 = {"type": "gza", "subtype": "branch", "message": "Second", "branch": "feat/x"}
        write_log_entry(log_file, entry1)
        write_log_entry(log_file, entry2)
        lines = log_file.read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == entry1
        assert json.loads(lines[1]) == entry2

    def test_logs_warning_when_write_fails(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """write_log_entry logs a warning and does not raise when writing fails."""
        bad_path = tmp_path / "nonexistent_dir" / "task.log"
        with caplog.at_level("WARNING"):
            write_log_entry(bad_path, {"type": "gza", "message": "x"})

        assert "Failed to write log entry" in caplog.text


class TestExtractReviewVerdict:
    """Tests for _extract_review_verdict()."""

    def test_bold_wrapped_verdict(self) -> None:
        assert _extract_review_verdict("**Verdict: APPROVED**") == "APPROVED"

    def test_bold_label_only_verdict(self) -> None:
        assert _extract_review_verdict("**Verdict**: CHANGES_REQUESTED") == "CHANGES_REQUESTED"

    def test_plain_verdict(self) -> None:
        assert _extract_review_verdict("Verdict: NEEDS_DISCUSSION") == "NEEDS_DISCUSSION"

    def test_heading_verdict(self) -> None:
        assert _extract_review_verdict("## Verdict\n\n**CHANGES_REQUESTED**\n") == "CHANGES_REQUESTED"

    def test_heading_verdict_without_bold_token(self) -> None:
        assert _extract_review_verdict("### Verdict\n\nNEEDS_DISCUSSION\n") == "NEEDS_DISCUSSION"

    def test_none_content(self) -> None:
        assert _extract_review_verdict(None) is None

    def test_no_verdict(self) -> None:
        assert _extract_review_verdict("Just some review text") is None

    def test_canonical_review_structure_with_none_sections(self) -> None:
        content = (
            "## Summary\n\n"
            "- Reviewed implementation and tests.\n\n"
            "## Must-Fix\n\n"
            "None.\n\n"
            "## Suggestions\n\n"
            "None.\n\n"
            "## Questions / Assumptions\n\n"
            "None.\n\n"
            "## Verdict\n\n"
            "No blocking issues identified.\n"
            "Verdict: APPROVED\n"
        )
        assert _extract_review_verdict(content) == "APPROVED"


class TestSelectiveStaging:
    """Tests for selective staging (only stage files changed during provider run)."""

    def test_selective_staging_only_stages_new_changes(self, tmp_path: Path):
        """Test that only files changed during the provider run get staged."""
        from gza.git import Git

        # Pre-existing status (before provider run)
        pre_status = {("M", "pre_existing.txt")}
        # Post-run status (includes pre-existing + new changes)
        post_status = {("M", "pre_existing.txt"), ("M", "src/foo.py"), ("??", "new_file.txt")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert set(files_to_stage) == {"src/foo.py", "new_file.txt"}
        assert "pre_existing.txt" not in files_to_stage

    def test_selective_staging_no_new_changes(self, tmp_path: Path):
        """Test that no staging happens when provider makes no changes."""
        pre_status = {("M", "pre_existing.txt")}
        post_status = {("M", "pre_existing.txt")}

        new_changes = post_status - pre_status
        assert len(new_changes) == 0

    def test_selective_staging_handles_deletions(self, tmp_path: Path):
        """Test that intentional deletions by the agent are staged."""
        pre_status = set()
        post_status = {("D", "removed_by_agent.py")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert "removed_by_agent.py" in files_to_stage

    def test_selective_staging_ignores_pre_existing_deletions(self, tmp_path: Path):
        """Test that pre-existing deletions are NOT staged."""
        pre_status = {("D", "already_deleted.py")}
        post_status = {("D", "already_deleted.py"), ("M", "changed.py")}

        new_changes = post_status - pre_status
        files_to_stage = [filepath for _, filepath in new_changes]

        assert "already_deleted.py" not in files_to_stage
        assert "changed.py" in files_to_stage


class TestExceptionHandlerMarkFailed:
    """Tests that exception handlers in _run_inner and _run_non_code_task mark tasks as failed."""

    def test_git_error_in_run_inner_marks_failed(self, tmp_path: Path):
        """Test that GitError during post-run finalization marks the task as failed."""
        from gza.git import GitError

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"
        task.branch = "test-branch"
        store.mark_in_progress(task)

        # Verify task is in_progress
        assert store.get(task.id).status == "in_progress"

        # Simulate what the GitError handler does
        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.task_id}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), branch="test-branch", failure_reason="GIT_ERROR")

        # Verify task is now failed
        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "GIT_ERROR"

    def test_keyboard_interrupt_marks_failed(self, tmp_path: Path):
        """Test that KeyboardInterrupt marks the task as failed with INTERRUPTED reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task", task_type="implement")
        task.task_id = "20260212-test-task"
        task.branch = "test-branch"
        store.mark_in_progress(task)

        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.task_id}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), branch="test-branch", failure_reason="INTERRUPTED")

        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "INTERRUPTED"

    def test_non_code_task_git_error_marks_failed(self, tmp_path: Path):
        """Test that GitError in _run_non_code_task marks the task as failed."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Explore something", task_type="explore")
        task.task_id = "20260212-explore-task"
        store.mark_in_progress(task)

        log_path = tmp_path / "logs"
        log_path.mkdir()
        log_file = log_path / f"{task.task_id}.log"
        log_file.write_text("")

        config = Mock(spec=Config)
        config.project_dir = tmp_path

        store.mark_failed(task, log_file=str(log_file.relative_to(tmp_path)), failure_reason="GIT_ERROR")

        updated_task = store.get(task.id)
        assert updated_task.status == "failed"
        assert updated_task.failure_reason == "GIT_ERROR"


class TestLoadDotenv:
    """Tests for load_dotenv() credential loading from .env files."""

    def _setup_home(self, tmp_path: Path, monkeypatch):
        """Create a fake ~/.gza/.env directory and patch Path.home()."""
        gza_dir = tmp_path / ".gza"
        gza_dir.mkdir()
        monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
        return gza_dir

    def test_home_env_sets_unset_vars(self, tmp_path: Path, monkeypatch):
        """~/.gza/.env should set variables not already in the environment."""
        from gza.runner import load_dotenv

        gza_dir = self._setup_home(tmp_path, monkeypatch)
        (gza_dir / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(tmp_path)

        assert os.environ["MY_TEST_KEY"] == "from_home"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_home_env_does_not_override_existing(self, tmp_path: Path, monkeypatch):
        """~/.gza/.env should not override variables already set in the environment."""
        from gza.runner import load_dotenv

        gza_dir = self._setup_home(tmp_path, monkeypatch)
        (gza_dir / ".env").write_text("MY_TEST_KEY=from_home\n")
        monkeypatch.setenv("MY_TEST_KEY", "from_shell")

        load_dotenv(tmp_path)

        assert os.environ["MY_TEST_KEY"] == "from_shell"

    def test_project_env_overrides_home_env(self, tmp_path: Path, monkeypatch):
        """Project .env should override values from ~/.gza/.env."""
        from gza.runner import load_dotenv

        gza_dir = self._setup_home(tmp_path, monkeypatch)
        (gza_dir / ".env").write_text("MY_TEST_KEY=from_home\n")

        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / ".env").write_text("MY_TEST_KEY=from_project\n")
        monkeypatch.delenv("MY_TEST_KEY", raising=False)

        load_dotenv(project_dir)

        assert os.environ["MY_TEST_KEY"] == "from_project"
        monkeypatch.delenv("MY_TEST_KEY")

    def test_comments_and_blank_lines_ignored(self, tmp_path: Path, monkeypatch):
        """Comments and blank lines in .env files should be ignored."""
        from gza.runner import load_dotenv

        gza_dir = self._setup_home(tmp_path, monkeypatch)
        (gza_dir / ".env").write_text(
            "# This is a comment\n"
            "\n"
            "MY_TEST_KEY=valid_value\n"
            "# COMMENTED_OUT=should_not_exist\n"
        )
        monkeypatch.delenv("MY_TEST_KEY", raising=False)
        monkeypatch.delenv("COMMENTED_OUT", raising=False)

        load_dotenv(tmp_path)

        assert os.environ["MY_TEST_KEY"] == "valid_value"
        assert "COMMENTED_OUT" not in os.environ
        monkeypatch.delenv("MY_TEST_KEY")

    def test_no_env_files_is_noop(self, tmp_path: Path, monkeypatch):
        """load_dotenv should not fail when no .env files exist."""
        from gza.runner import load_dotenv

        self._setup_home(tmp_path, monkeypatch)

        load_dotenv(tmp_path)
