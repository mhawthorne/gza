"""Tests for git operations CLI commands."""


import argparse
import io
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

from gza.advance_engine import evaluate_advance_rules
from gza.cli import cmd_advance
from gza.config import Config
from gza.review_verdict import ParsedReviewReport, ReviewFinding
from tests.helpers.cli import capture_background_worker_spawns

from .conftest import (
    make_store,
    run_gza,
    setup_config,
)


class TestAdvanceCommand:
    """Tests for 'gza advance' command."""

    def _mock_git(self, *, current_branch: str = "main", can_merge: bool = True):
        """Return a minimal Git mock for planner-only advance tests."""
        git = Mock()
        git.current_branch.return_value = current_branch
        git.can_merge.return_value = can_merge
        return git

    def _setup_git_repo(self, tmp_path: Path):
        """Initialize a git repo in tmp_path with an initial commit on main."""
        from gza.git import Git
        git = Git(tmp_path)
        git._run("init", "-b", "main")
        git._run("config", "user.name", "Test User")
        git._run("config", "user.email", "test@example.com")
        (tmp_path / "README.md").write_text("initial")
        git._run("add", "README.md")
        git._run("commit", "-m", "Initial commit")
        return git

    def _create_implement_task_with_branch(self, store, git, tmp_path, prompt="Implement feature"):
        """Create a completed implement task with a real git branch."""
        task = store.add(prompt, task_type="implement")
        branch = f"feat/task-{task.id}"

        # Create the branch with a commit
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")

        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        return task

    def test_advance_no_eligible_tasks(self, tmp_path: Path):
        """advance command reports no tasks when none are eligible."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        make_store(tmp_path)  # create empty db
        with patch("gza.cli.Git", return_value=self._mock_git()):
            result = run_gza("advance", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_actions(self, tmp_path: Path):
        """advance --dry-run shows planned actions without executing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert str(task.id) in result.stdout

    def test_advance_dry_run_excludes_same_branch_fix_child_from_roots(self, tmp_path: Path):
        """advance plans only the implementation root when a completed fix child exists on same branch."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.status = "completed"
        impl_task.completed_at = datetime.now(UTC)
        impl_task.branch = "feat/impl-root"
        impl_task.merge_status = "unmerged"
        impl_task.has_commits = True
        store.update(impl_task)

        fix_task = store.add(
            "Fix feature churn",
            task_type="fix",
            based_on=impl_task.id,
            same_branch=True,
        )
        fix_task.status = "completed"
        fix_task.completed_at = datetime.now(UTC)
        fix_task.branch = "feat/impl-root"
        fix_task.merge_status = "unmerged"
        fix_task.has_commits = True
        store.update(fix_task)

        with patch("gza.cli.Git", return_value=self._mock_git()):
            result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Would advance 1 task(s)" in result.stdout
        assert str(impl_task.id) in result.stdout
        assert str(fix_task.id) not in result.stdout

    def test_advance_merges_approved_task(self, tmp_path: Path):
        """advance merges a task whose review is APPROVED."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review task with APPROVED verdict
        review_prompt = f"Review implementation {task.id}"
        review_task = store.add(
            review_prompt,
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good!"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Merged" in result.stdout or "merged" in result.stdout

        # Verify merge status updated
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"

    def test_advance_dry_run_approved_with_newer_unresolved_comments_prefers_pending_improve(self, tmp_path: Path):
        """Approved review with newer unresolved comments should run pending improve, not merge."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement with late comment", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/approved-late-comment"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        assert task.id is not None

        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)
        assert review.id is not None

        improve = store.add(
            "Pending improve from newer comments",
            task_type="improve",
            based_on=task.id,
            depends_on=review.id,
        )
        improve.status = "pending"
        improve.created_at = datetime(2026, 1, 2, tzinfo=UTC)
        store.update(improve)

        store.add_comment(task.id, "Address this after approval.")

        with patch("gza.cli.Git", return_value=self._mock_git(current_branch="main", can_merge=True)):
            result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Run pending improve for unresolved comments newer than latest review" in result.stdout
        assert "Merge (review APPROVED)" not in result.stdout

    def test_advance_dry_run_followups_with_newer_unresolved_comments_creates_improve(self, tmp_path: Path):
        """APPROVED_WITH_FOLLOWUPS plus newer unresolved comments should prefer improve over merge_with_followups."""
        from gza import advance_engine as advance_engine_module

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement with followups and late comment", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/followups-late-comment"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        assert task.id is not None

        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(review)

        store.add_comment(task.id, "New unresolved issue after followups verdict.")

        with (
            patch("gza.cli.Git", return_value=self._mock_git(current_branch="main", can_merge=True)),
            patch.object(
                advance_engine_module,
                "get_review_report",
                return_value=ParsedReviewReport(
                    verdict="APPROVED_WITH_FOLLOWUPS",
                    findings=(
                        ReviewFinding(
                            id="F1",
                            severity="FOLLOWUP",
                            title="Hardening",
                            body="",
                            evidence=None,
                            impact=None,
                            fix_or_followup="add malformed input guard",
                            tests=None,
                        ),
                    ),
                    format_version="v2",
                ),
            ),
        ):
            result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Create improve task (unresolved comments newer than latest review)" in result.stdout
        assert "Merge (review APPROVED_WITH_FOLLOWUPS)" not in result.stdout

    def test_advance_merge_with_followups_creates_idempotent_followup_tasks(self, tmp_path: Path):
        """merge_with_followups creates one implement follow-up per finding and reuses on rerun."""
        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)
        review_task = store.add("Review", task_type="review", depends_on=task.id)
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            force=False,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            new=False,
            max_review_cycles=None,
            squash_threshold=None,
        )

        action = {
            "type": "merge_with_followups",
            "description": "Merge (review APPROVED_WITH_FOLLOWUPS)",
            "review_task": review_task,
            "followup_findings": (
                ReviewFinding(
                    id="F1",
                    severity="FOLLOWUP",
                    title="Hardening",
                    body="",
                    evidence=None,
                    impact=None,
                    fix_or_followup="add malformed input guard",
                    tests=None,
                ),
            ),
        }

        with patch("gza.cli.determine_next_action", return_value=action), patch(
            "gza.cli._merge_single_task", return_value=1
        ):
            rc1 = cmd_advance(args)
            rc2 = cmd_advance(args)

        assert rc1 == 1
        assert rc2 == 1
        followups = [
            t for t in store.get_all()
            if t.task_type == "implement"
            and t.based_on == review_task.id
            and t.prompt.startswith(f"Follow-up F1 from review {review_task.id} for task {task.id}:")
        ]
        assert len(followups) == 1

    def test_advance_approved_with_followups_without_findings_needs_discussion(self, tmp_path: Path):
        """APPROVED_WITH_FOLLOWUPS without FOLLOWUP findings must not be merge-ready."""
        from gza import advance_engine as advance_engine_module

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/no-followups"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        with (
            patch(
                "gza.cli.Git",
                return_value=self._mock_git(current_branch="main", can_merge=True),
            ),
            patch.object(
                advance_engine_module,
                "get_review_report",
                return_value=ParsedReviewReport(
                    verdict="APPROVED_WITH_FOLLOWUPS",
                    findings=(),
                    format_version="v2",
                ),
            ),
        ):
            result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "needs manual attention" in result.stdout
        assert "Merge (review APPROVED_WITH_FOLLOWUPS)" not in result.stdout

    def test_advance_targets_current_branch_for_conflict_check_and_rebase(self, tmp_path: Path):
        """advance uses the current branch (not default) for conflict detection and rebase target."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create an integration branch and diverge it from main.
        git._run("checkout", "-b", "agent-sessions")
        (tmp_path / "README.md").write_text("agent branch version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Agent branch change")

        # Create feature from main so it can merge into main cleanly.
        git._run("checkout", "main")
        git._run("checkout", "-b", "feat/target-mismatch")
        (tmp_path / "README.md").write_text("feature branch version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Feature change")

        # Return to agent-sessions so advance target is non-default.
        git._run("checkout", "agent-sessions")

        task = store.add("Conflicting on agent-sessions only", task_type="explore")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/target-mismatch"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 1
        assert "onto the local branch 'agent-sessions'" in rebases[0].prompt
        assert store.get(task.id).merge_status == "unmerged"

    def test_advance_passes_current_branch_as_merge_target(self, tmp_path: Path):
        """advance passes current branch to _merge_single_task for merge actions."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        git._run("checkout", "-b", "agent-sessions")
        git._run("checkout", "main")
        task = store.add("Explore merge target branch", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Add feature for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        git._run("checkout", "agent-sessions")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        captured_targets: list[str] = []

        def fake_merge(task_id, config, store, git, merge_args, target_branch):
            captured_targets.append(target_branch)
            return 0

        with patch("gza.cli._merge_single_task", side_effect=fake_merge):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_targets == ["agent-sessions"]

    def test_advance_merge_conflict_fallback_creates_rebase_and_cleans_state(self, tmp_path: Path):
        """A merge conflict during execution resets git state and falls back to rebase task creation."""
        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = store.add("Explore fallback behavior", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature one")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit 1 for task {task.id}")
        (tmp_path / f"feat_{task.id}.txt").write_text("feature two")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit 2 for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli.determine_next_action", return_value={"type": "merge", "description": "Merge"}):
            with patch("gza.cli._merge_single_task", return_value=1):
                with patch("gza.git.Git.can_merge", return_value=False):
                    with patch("gza.git.Git.reset_hard_head") as mock_reset:
                        with patch("gza.cli._spawn_background_worker", return_value=0):
                            rc = cmd_advance(args)

        assert rc == 0
        assert mock_reset.called
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 1
        assert "onto the local branch 'main'" in rebases[0].prompt

    def test_advance_merge_conflict_fallback_reports_rebase_worker_start_failure(self, tmp_path: Path):
        """Merge-conflict fallback must report child creation separately from rebase worker startup failure."""
        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = store.add("Explore fallback spawn failure", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            force=False,
        )

        with (
            patch("gza.cli.determine_next_action", return_value={"type": "merge", "description": "Merge"}),
            patch("gza.cli._merge_single_task", return_value=1),
            patch("gza.git.Git.can_merge", return_value=False),
            patch("gza.git.Git.reset_hard_head"),
            patch("gza.cli._spawn_background_worker", return_value=1),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 1
        assert rebases[0].id is not None
        assert rc == 1
        assert f"Created rebase task {rebases[0].id}" in output
        assert f"Failed to start rebase worker for task {rebases[0].id}" in output

    def test_advance_merge_conflict_fallback_reset_failure_is_hard_error(self, tmp_path: Path):
        """When reset_hard_head fails, advance increments error_count and skips rebase task creation."""
        from gza.cli import cmd_advance
        from gza.git import GitError
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = store.add("Explore fallback reset failure", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"feat_{task.id}.txt").write_text("feature")
        git._run("add", f"feat_{task.id}.txt")
        git._run("commit", "-m", f"Commit for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        output_lines: list[str] = []

        def capture_print(msg: str = "", **kwargs: object) -> None:
            output_lines.append(str(msg))

        with patch("gza.cli.determine_next_action", return_value={"type": "merge", "description": "Merge"}):
            with patch("gza.cli._merge_single_task", return_value=1):
                with patch("gza.git.Git.can_merge", return_value=False):
                    with patch("gza.git.Git.reset_hard_head", side_effect=GitError("reset failed")):
                        with patch("gza.cli._spawn_background_worker", return_value=0) as mock_spawn:
                            with patch("gza.cli.git_ops.console") as mock_console:
                                mock_console.print.side_effect = capture_print
                                cmd_advance(args)

        # No rebase task should be created
        rebases = [t for t in store.get_all() if t.task_type == "rebase" and t.based_on == task.id]
        assert len(rebases) == 0
        # No background worker spawned for rebase
        mock_spawn.assert_not_called()
        # Output should contain a red error message about failed cleanup
        combined = "\n".join(output_lines)
        assert "Cleanup failed" in combined or "Manual intervention" in combined

    def test_advance_skips_task_with_in_progress_rebase_child(self, tmp_path: Path):
        """advance skips a task when a rebase child is already in progress."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting2"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        # Create an in-progress rebase child
        rebase_child = store.add(
            "Rebase branch",
            task_type="rebase",
            based_on=task.id,
            same_branch=True,
        )
        rebase_child.status = "in_progress"
        store.update(rebase_child)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'skip'
        assert f"rebase {rebase_child.id} already in progress" in action['description']

    def test_advance_needs_discussion_for_failed_rebase_child(self, tmp_path: Path):
        """advance returns needs_discussion when a rebase child has failed."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a branch that conflicts with main
        branch = "feat/conflicting3"
        git._run("checkout", "-b", branch)
        (tmp_path / "README.md").write_text("feature version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Conflict commit")
        git._run("checkout", "main")
        (tmp_path / "README.md").write_text("main version")
        git._run("add", "README.md")
        git._run("commit", "-m", "Main change")

        task = store.add("Conflicting feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        # Create a failed rebase child
        rebase_child = store.add(
            "Rebase branch",
            task_type="rebase",
            based_on=task.id,
            same_branch=True,
        )
        rebase_child.status = "failed"
        store.update(rebase_child)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'needs_discussion'
        assert f"rebase {rebase_child.id} failed" in action['description']

    def test_advance_merges_non_implement_task_without_review(self, tmp_path: Path):
        """advance merges a non-implement task (e.g. explore) directly, skipping review creation."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a completed explore task with a branch but no review
        task = store.add("Explore the codebase", task_type="explore")
        branch = f"feat/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"explore_{task.id}.txt").write_text("notes")
        git._run("add", f"explore_{task.id}.txt")
        git._run("commit", "-m", f"Exploration for task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        rc = cmd_advance(args)

        assert rc == 0

        # Verify the task was merged directly without creating a review
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_creates_review_for_implement_without_review(self, tmp_path: Path):
        """advance creates a review task for a completed implement task with no review."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify a review task was created (not merged directly)
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'

    def test_advance_creates_improve_for_changes_requested(self, tmp_path: Path):
        """advance creates an improve task when review is CHANGES_REQUESTED."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Patch _spawn_background_worker to avoid actually spawning processes
        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0

        # Verify improve task was created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].task_type == "improve"

    def test_advance_improve_prompt_mentions_unresolved_comments(self, tmp_path: Path):
        """Advance-created improve prompts should mention unresolved comments when present."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)
        assert task.id is not None

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease address reviewer notes."
        store.update(review_task)

        store.add_comment(task.id, "Unresolved note from operator feedback.")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert "unresolved comments" in improve_tasks[0].prompt

    def test_advance_orchestrates_implement_review_improve_merge_in_local_repo(self, tmp_path: Path):
        """advance orchestrates implement -> review -> improve -> merge in a local fixture repo."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        impl_task = self._create_implement_task_with_branch(
            store,
            git,
            tmp_path,
            prompt="Implement feature via advance workflow",
        )
        assert impl_task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        spawned_types: list[str] = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            task = store.get(task_id)
            assert task is not None
            spawned_types.append(task.task_type)

            if task.task_type == "review":
                # First review requests changes; second review approves.
                completed_reviews = [
                    r for r in store.get_reviews_for_task(impl_task.id)
                    if r.status == "completed"
                ]
                if completed_reviews:
                    verdict = "**Verdict: APPROVED**\n\nLooks good."
                else:
                    verdict = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix nits."
                store.mark_completed(task, output_content=verdict)
                return 0

            if task.task_type == "improve":
                store.mark_completed(task, output_content="Applied requested fixes.")
                store.clear_review_state(impl_task.id)
                return 0

            raise AssertionError(f"Unexpected spawned task type: {task.task_type}")

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            # 1) implement -> create+run review (CHANGES_REQUESTED)
            assert cmd_advance(args) == 0
            reviews_after_first = store.get_reviews_for_task(impl_task.id)
            assert len(reviews_after_first) == 1
            assert reviews_after_first[0].status == "completed"
            assert "CHANGES_REQUESTED" in (reviews_after_first[0].output_content or "")
            assert store.get(impl_task.id).merge_status == "unmerged"

            # 2) changes requested -> create+run improve
            assert cmd_advance(args) == 0
            first_review = store.get_reviews_for_task(impl_task.id)[0]
            improves = store.get_improve_tasks_for(impl_task.id, first_review.id)
            assert len(improves) == 1
            assert improves[0].status == "completed"
            assert store.get(impl_task.id).review_cleared_at is not None

            # 3) improved code -> create+run re-review (APPROVED)
            assert cmd_advance(args) == 0
            reviews_after_second = store.get_reviews_for_task(impl_task.id)
            assert len(reviews_after_second) == 2
            assert "APPROVED" in (reviews_after_second[0].output_content or "")

            # 4) approved review -> merge
            assert cmd_advance(args) == 0

        updated_impl = store.get(impl_task.id)
        assert updated_impl is not None
        assert updated_impl.merge_status == "merged"
        assert git.is_merged(updated_impl.branch, "main")
        assert (tmp_path / f"feat_{impl_task.id}.txt").exists()
        assert spawned_types == ["review", "improve", "review"]

    def test_advance_single_task_id(self, tmp_path: Path):
        """advance with a specific task ID only advances that task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")

        # Give task1 an approved review so it can merge
        review = store.add(f"Review {task1.id}", task_type="review", depends_on=task1.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        # Advance only task1
        result = run_gza("advance", str(task1.id), "--auto", "--project", str(tmp_path))
        assert result.returncode == 0

        # task1 should be merged, task2 should still be unmerged
        assert store.get(task1.id).merge_status == "merged"
        assert store.get(task2.id).merge_status == "unmerged"

    def test_advance_rejects_bare_integer_id(self, tmp_path: Path):
        """advance requires a full prefixed task ID."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path, "Feature via integer ID")

        # Give the task an approved review so it can merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("advance", "1", "--auto", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "Use a full prefixed task ID" in result.stdout or "Use a full prefixed task ID" in result.stderr
        assert store.get(task.id).merge_status == "unmerged"

    def test_advance_max_limits_batch(self, tmp_path: Path):
        """advance --max N limits the number of tasks processed."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task1 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature A")
        task2 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature B")
        task3 = self._create_implement_task_with_branch(store, git, tmp_path, "Feature C")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # Only 2 tasks should have been processed (not 3, due to --max 2).
        # Since these are implement tasks with no reviews, reviews are created.
        # Tasks are ordered by completed_at DESC (newest first), so task3 and
        # task2 are processed while task1 (oldest) is left untouched.
        review_counts = [
            len(store.get_reviews_for_task(t.id))
            for t in [task1, task2, task3]
        ]
        assert sum(review_counts) == 2
        # task1 is the oldest so it falls outside the --max 2 window.
        assert review_counts[0] == 0

    def test_advance_max_still_resumes_failed_tasks(self, tmp_path: Path):
        """advance --max should not suppress resumable failed-task handling."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path, "Completed task in --max window")
        failed_task = self._create_failed_task(store, session_id="sess-max-1", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--max", "1", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume (failed: MAX_STEPS" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1

    def test_advance_dry_run_max_still_plans_failed_resume(self, tmp_path: Path):
        """advance --dry-run --max should still include resumable failed tasks in the plan."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path, "Completed task in --max window")
        failed_task = self._create_failed_task(store, session_id="sess-max-2", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--max", "1", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert str(failed_task.id) in result.stdout
        assert "Resume (failed: MAX_STEPS" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_spawns_worker_for_pending_review(self, tmp_path: Path):
        """advance spawns a worker for a pending review instead of skipping."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime(2026, 1, 2, tzinfo=UTC)
        task.branch = "feat/pending-review"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        # Create a pending review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        # review_task.status is 'pending' by default

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            force=False,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            new=False,
            max_review_cycles=None,
            squash_threshold=None,
        )
        worker_calls, fake_spawn = capture_background_worker_spawns()

        with (
            patch("gza.cli.Git", return_value=self._mock_git(current_branch="main", can_merge=True)),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
        ):
            rc = cmd_advance(args)

        assert rc == 0
        assert review_task.id is not None
        assert len(worker_calls) == 1
        assert worker_calls[0]["task_id"] == review_task.id

    def test_advance_force_propagates_to_run_review_worker(self, tmp_path: Path):
        """advance --force forwards force override when spawning run_review workers."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )

        captured_force: list[bool] = []

        def fake_spawn(worker_args, _config, task_id, **_kw):
            del task_id
            captured_force.append(bool(getattr(worker_args, "force", False)))
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_force == [True]

    def test_advance_force_propagates_to_resume_worker(self, tmp_path: Path):
        """advance --force forwards force override when spawning resume workers."""
        import argparse

        from gza.cli import cmd_advance

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_task = store.add("Resumable failure", task_type="implement")
        failed_task.status = "failed"
        failed_task.failure_reason = "MAX_STEPS"
        failed_task.session_id = "ses_resume_123"
        store.update(failed_task)

        captured_force: list[bool] = []

        def fake_spawn_resume(worker_args, _config, _task_id, **_kw):
            captured_force.append(bool(getattr(worker_args, "force", False)))
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=True,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_resume_worker", side_effect=fake_spawn_resume),
        ):
            rc = cmd_advance(args)

        assert rc == 0
        assert captured_force == [True]

    def test_advance_create_review_spawn_failure_reports_created_task_and_worker_error(self, tmp_path: Path):
        """advance should report created review tasks separately from review worker startup failures."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed implementation", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/review-spawn-failure"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli.determine_next_action", return_value={"type": "create_review", "description": "Create review"}),
            patch("gza.cli._spawn_background_worker", return_value=1),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        review_task = next(t for t in store.get_all() if t.task_type == "review")
        assert review_task.id is not None
        assert rc == 1
        assert f"Created review task {review_task.id}" in output
        assert f"Failed to start review worker for task {review_task.id}" in output
        assert f"✗ Created review task {review_task.id}" not in output

    def test_advance_create_implement_spawn_failure_reports_created_task_and_worker_error(self, tmp_path: Path):
        """advance should report created implement tasks separately from implement worker startup failures."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Completed plan", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_implement", "description": "Create implement"},
            ),
            patch("gza.cli._spawn_background_worker", return_value=1),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        impl_task = next(t for t in store.get_all() if t.task_type == "implement" and t.depends_on == plan.id)
        assert impl_task.id is not None
        assert rc == 1
        assert f"Created implement task {impl_task.id}" in output
        assert f"Failed to start implement worker for task {impl_task.id}" in output
        assert f"✗ Created implement task {impl_task.id}" not in output

    def test_advance_resume_spawn_failure_reports_created_task_and_worker_error(self, tmp_path: Path):
        """advance should report created resume tasks separately from resume worker startup failures."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)

        failed_task = store.add("Failed implementation", task_type="implement")
        failed_task.status = "failed"
        failed_task.failure_reason = "MAX_STEPS"
        failed_task.session_id = "resume-session"
        failed_task.completed_at = datetime.now(UTC)
        store.update(failed_task)
        assert failed_task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli.determine_next_action", return_value={"type": "resume", "description": "Resume task"}),
            patch("gza.cli._spawn_background_resume_worker", return_value=1),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        resume_task = next(t for t in store.get_based_on_children(failed_task.id) if t.task_type == "implement")
        assert resume_task.id is not None
        assert rc == 1
        assert f"Created resume task {resume_task.id}" in output
        assert f"Failed to start resume worker for task {resume_task.id}" in output
        assert f"✗ Created resume task {resume_task.id}" not in output

    def test_advance_needs_rebase_spawn_failure_reports_created_task_and_worker_error(self, tmp_path: Path):
        """advance should report created rebase tasks separately from rebase worker startup failures."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Needs rebase", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/rebase-spawn-failure"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        assert task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase", "description": "Needs rebase"}),
            patch("gza.cli._spawn_background_worker", return_value=1),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        rebase_task = next(t for t in store.get_based_on_children(task.id) if t.task_type == "rebase")
        assert rebase_task.id is not None
        assert rc == 1
        assert f"Created rebase task {rebase_task.id}" in output
        assert f"Failed to start rebase worker for task {rebase_task.id}" in output
        assert f"✗ Created rebase task {rebase_task.id}" not in output

    def test_advance_non_spawn_error_message_prints_only_failure_line(self, tmp_path: Path):
        """Non-spawn execution errors must not also print a green success line."""
        import argparse

        from gza.cli.advance_executor import AdvanceActionExecutionResult

        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed implementation", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/non-spawn-error"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_review", "description": "Create review"},
            ),
            patch(
                "gza.cli.execute_advance_action",
                return_value=AdvanceActionExecutionResult(
                    action_type="create_review",
                    status="error",
                    message="review creation returned no task",
                    attempted_spawn=False,
                ),
            ),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 1
        assert "✗ review creation returned no task" in output
        assert "✓ review creation returned no task" not in output

    def test_advance_improve_value_error_does_not_print_green_success(self, tmp_path: Path):
        """Improve creation ValueErrors must surface only as failures."""
        import argparse

        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path, "Improve duplicate path")
        assert task.id is not None

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli._create_improve_task", side_effect=ValueError("duplicate improve task")),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "improve", "description": "Create improve", "review_task": review_task},
            ),
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 1
        assert "✗ duplicate improve task" in output
        assert "✓ duplicate improve task" not in output

    def test_advance_iterate_create_implement_spawn_failure_uses_iterate_worker_label(self, tmp_path: Path):
        """Iterate-mode implement launch failures must mention the iterate worker path."""
        import argparse

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)

        plan = store.add("Completed plan", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch(
                "gza.cli.determine_next_action",
                return_value={"type": "create_implement", "description": "Create implement"},
            ),
            patch("gza.cli._spawn_background_iterate_worker", return_value=1),
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        impl_task = next(t for t in store.get_all() if t.task_type == "implement" and t.depends_on == plan.id)
        assert impl_task.id is not None
        assert rc == 1
        assert f"Created implement task {impl_task.id}" in output
        assert f"Failed to start iterate worker for task {impl_task.id}" in output
        assert f"Failed to start implement worker for task {impl_task.id}" not in output
        spawn_worker.assert_not_called()

    def test_advance_iterate_needs_rebase_spawn_failure_uses_iterate_worker_label(self, tmp_path: Path):
        """Iterate-mode rebase launch failures must mention the iterate worker path."""
        import argparse

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)

        task = store.add("Needs iterate rebase", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feat/iterate-rebase-failure"
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)
        assert task.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase", "description": "Needs rebase"}),
            patch("gza.cli._spawn_background_iterate_worker", return_value=1),
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 1
        assert f"Failed to start iterate worker for task {task.id}" in output
        assert f"Failed to start rebase worker for task {task.id}" not in output
        spawn_worker.assert_not_called()

    def test_advance_waits_for_in_progress_review(self, tmp_path: Path):
        """advance skips a task whose review is in_progress."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create an in_progress review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "in_progress"
        store.update(review_task)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "SKIP" in result.stdout
        assert "in_progress" in result.stdout

    def test_advance_task_not_found(self, tmp_path: Path):
        """advance with non-existent task ID returns error."""
        setup_config(tmp_path)
        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        make_store(tmp_path)  # create db
        with patch("gza.cli.Git", return_value=self._mock_git()):
            result = run_gza("advance", "testproject-999999", "--project", str(tmp_path))
        assert result.returncode == 1
        assert "not found" in result.stdout

    def test_advance_dry_run_does_not_modify_state(self, tmp_path: Path):
        """advance --dry-run does not modify task state or create tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout

        # Task should still be unmerged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_task_with_no_branch_is_skipped(self, tmp_path: Path):
        """advance skips tasks that have no branch (no commits)."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create a task with no branch
        task = store.add("Implement feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.merge_status = "unmerged"
        task.branch = None
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        with patch("gza.cli.Git", return_value=self._mock_git()):
            rc = cmd_advance(args)

        assert rc == 0
        # No review tasks should have been created
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_needs_discussion_verdict_skips(self, tmp_path: Path):
        """advance skips tasks whose review verdict needs manual attention."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review with no recognizable verdict
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "I have some thoughts but no verdict."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # Task should not have been merged or had new tasks created
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_non_implement_task_skipped_in_create_review(self, tmp_path: Path):
        """advance skips creating a review for non-implement task types."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create a plan-type task with a branch
        task = store.add("Plan something", task_type="plan")
        branch = f"plan/task-{task.id}"
        git._run("checkout", "-b", branch)
        (tmp_path / f"plan_{task.id}.txt").write_text("plan")
        git._run("add", f"plan_{task.id}.txt")
        git._run("commit", "-m", f"Plan task {task.id}")
        git._run("checkout", "main")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = branch
        task.merge_status = "unmerged"
        task.has_commits = True
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No review should have been created for a plan task
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 0

    def test_advance_active_improve_already_exists_is_skipped(self, tmp_path: Path):
        """advance skips creating a new improve task when one is already active."""
        import argparse

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a review with CHANGES_REQUESTED
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix the tests."
        store.update(review_task)

        # Create an already-pending improve task
        existing_improve = store.add(
            f"Improve {task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=task.id,
            same_branch=True,
        )
        # status is 'pending' by default

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )
        rc = cmd_advance(args)

        assert rc == 0
        # No additional improve task should be created
        improve_tasks = store.get_improve_tasks_for(task.id, review_task.id)
        assert len(improve_tasks) == 1
        assert improve_tasks[0].id == existing_improve.id

    def test_advance_already_merged_task_returns_early(self, tmp_path: Path):
        """advance with a specific already-merged task ID exits with 0 early."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Mark task as already merged
        task.merge_status = "merged"
        store.update(task)

        result = run_gza("advance", str(task.id), "--project", str(tmp_path))
        assert result.returncode == 0
        assert "already merged" in result.stdout

    def test_advance_review_cleared_at_triggers_merge(self, tmp_path: Path):
        """advance merges when review_cleared_at marks prior review as addressed (no new review)."""
        import argparse
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Set review_cleared_at on the task to a time AFTER the review completed
        # (simulates an improve task having run after the review)
        task.review_cleared_at = review_task.completed_at + timedelta(microseconds=1)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        # No new review should be created — task is merged directly after improve
        all_reviews = store.get_reviews_for_task(task.id)
        assert len(all_reviews) == 1  # only the original review
        assert store.get(task.id).merge_status == "merged"

    def test_advance_batch_limits_worker_spawning(self, tmp_path: Path):
        """advance --batch B stops after B workers have been started."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 3 implement tasks, each with a pending review (triggers run_review)
        tasks = []
        for i in range(3):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            return 0

        def fake_merge(task_id, config, store, git, merge_args, default_branch):
            task = store.get(task_id)
            assert task is not None
            store.set_merge_status(task.id, "merged")
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=2,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        # Only 2 workers should have been started, not 3
        assert len(spawn_calls) == 2
        # The third task should show a batch limit message
        assert "batch limit reached" in output
        assert f"{tasks[2].id}" in output

    def test_advance_batch_merge_does_not_count_toward_limit(self, tmp_path: Path):
        """advance --batch B: merge actions don't count toward the worker limit."""
        # Use advance_requires_review=false so unreviewed tasks merge directly
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\nadvance_requires_review: false\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 tasks that will merge (with APPROVED reviews)
        merge_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Merge {i}")
            review_task = store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            review_task.status = "completed"
            review_task.completed_at = datetime.now(UTC)
            review_task.output_content = "**Verdict: APPROVED**"
            store.update(review_task)
            merge_tasks.append(task)

        # Create 2 tasks with pending reviews (will spawn workers)
        worker_tasks = []
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Worker {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )
            worker_tasks.append(task)

        spawn_calls = []

        def fake_spawn(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            return 0

        def fake_merge(task_id, config, store, git, merge_args, default_branch):
            task = store.get(task_id)
            assert task is not None
            store.set_merge_status(task.id, "merged")
            return 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn), \
             patch("gza.cli._merge_single_task", side_effect=fake_merge):
            rc = cmd_advance(args)

        assert rc == 0
        # Both merge tasks should be merged (they don't count toward batch)
        for t in merge_tasks:
            assert store.get(t.id).merge_status == "merged"
        # Only 1 worker should have been spawned (batch=1)
        assert len(spawn_calls) == 1

    def test_advance_batch_enforced_on_failed_spawn(self, tmp_path: Path):
        """advance --batch 1 attempts only one spawn even when the first spawn fails."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # Create 2 implement tasks, each with a pending review (triggers run_review)
        for i in range(2):
            task = self._create_implement_task_with_branch(store, git, tmp_path, f"Feature {i}")
            store.add(
                f"Review {task.id}",
                task_type="review",
                depends_on=task.id,
            )

        spawn_calls = []

        def fake_spawn_first_fails(worker_args, config, task_id, **_kw):
            spawn_calls.append(task_id)
            # First call fails, second would succeed — but with batch=1 it should never be called
            return 1 if len(spawn_calls) == 1 else 0

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            no_docker=True,
        )

        with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn_first_fails):
            cmd_advance(args)

        # With batch=1, the failed spawn still counts toward the limit,
        # so only 1 spawn attempt should be made (not 2)
        assert len(spawn_calls) == 1

    def test_advance_batch_zero_returns_error(self, tmp_path: Path):
        """advance --batch 0 is rejected with an error message."""
        setup_config(tmp_path)
        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=0,
            no_docker=True,
        )
        with patch("gza.cli.Git", return_value=self._mock_git()):
            rc = cmd_advance(args)
        assert rc == 1

    def test_advance_spawn_worker_failure_increments_error_count(self, tmp_path: Path):
        """advance returns 1 when _spawn_background_worker fails for an improve task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review so advance will try to spawn an improve worker
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        # Simulate worker spawn failure
        with patch("gza.cli._spawn_background_worker", return_value=1):
            rc = cmd_advance(args)

        assert rc == 1

    def test_advance_interactive_shows_plan_and_prompts(self, tmp_path: Path):
        """advance without --auto shows plan and prompts for confirmation."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        # Simulate user confirming with 'y'
        with patch("builtins.input", return_value="y") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_called_once()
        call_args = mock_input.call_args[0][0]
        assert "Proceed" in call_args

    def test_advance_interactive_aborts_on_no(self, tmp_path: Path):
        """advance without --auto exits without executing when user answers 'n'."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action would be merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", return_value="n"):
            rc = cmd_advance(args)

        assert rc == 0
        # Task should NOT have been merged
        updated_task = store.get(task.id)
        assert updated_task.merge_status == "unmerged"

    def test_advance_interactive_eof_aborts(self, tmp_path: Path):
        """advance without --auto exits cleanly when stdin is closed (EOFError)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=False,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input", side_effect=EOFError):
            rc = cmd_advance(args)

        assert rc == 0

    def test_advance_auto_flag_skips_prompt(self, tmp_path: Path):
        """advance --auto executes without prompting."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Add approved review so action is merge
        review = store.add(f"Review {task.id}", task_type="review", depends_on=task.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        review.output_content = "**Verdict: APPROVED**"
        store.update(review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        with patch("builtins.input") as mock_input:
            with patch("gza.cli._spawn_background_worker", return_value=0):
                rc = cmd_advance(args)

        assert rc == 0
        mock_input.assert_not_called()
        assert store.get(task.id).merge_status == "merged"

    def test_advance_merges_run_before_workers(self, tmp_path: Path):
        """advance executes all merge actions before spawning any background workers.

        This test fails if the sort line in cmd_advance is removed: get_unmerged()
        returns tasks ORDER BY completed_at DESC, so task_spawn (the newer task)
        appears first. Without the sort, spawn happens before merge. The sort
        reorders so merge runs first.
        """
        import argparse
        from datetime import datetime
        from unittest.mock import patch

        from gza.cli import cmd_advance
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)

        # task_merge: APPROVED review → 'merge' action.
        # Given an EARLIER completed_at so it appears second in DB order (DESC).
        task_merge = self._create_implement_task_with_branch(store, git, tmp_path, "Feature merge")
        approved_review = store.add(
            f"Review {task_merge.id}", task_type="review", depends_on=task_merge.id
        )
        approved_review.status = "completed"
        approved_review.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        approved_review.output_content = "**Verdict: APPROVED**\n\nLooks great."
        store.update(approved_review)
        task_merge.completed_at = datetime(2026, 1, 1, tzinfo=UTC)
        store.update(task_merge)

        # task_spawn: pending review → 'run_review' action (spawns a worker).
        # Given a LATER completed_at so it appears first in DB order (DESC).
        # Without the sort, this causes spawn to execute before merge.
        task_spawn = self._create_implement_task_with_branch(store, git, tmp_path, "Feature spawn")
        store.add(f"Review {task_spawn.id}", task_type="review", depends_on=task_spawn.id)
        # Leave review status as default 'pending' — this triggers run_review action.
        task_spawn.completed_at = datetime(2026, 2, 1, tzinfo=UTC)
        store.update(task_spawn)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
        )

        call_log: list[str] = []

        def fake_merge(task_id, config, store, git, merge_args, default_branch):
            call_log.append('merge')
            return 0

        def fake_spawn(spawn_args, config, task_id=None, **_kw):
            call_log.append('spawn')
            return 0

        with patch("gza.cli._merge_single_task", side_effect=fake_merge):
            with patch("gza.cli._spawn_background_worker", side_effect=fake_spawn):
                rc = cmd_advance(args)

        assert rc == 0
        assert 'merge' in call_log, "Expected at least one merge call"
        assert 'spawn' in call_log, "Expected at least one worker spawn call"
        # All merges must complete before the first spawn
        last_merge_index = max(i for i, v in enumerate(call_log) if v == 'merge')
        first_spawn_index = min(i for i, v in enumerate(call_log) if v == 'spawn')
        assert last_merge_index < first_spawn_index, (
            f"Expected all merges before first spawn, got call order: {call_log}"
        )

    def test_advance_requires_review_true_create_true_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review when advance_requires_review=True, advance_create_reviews=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        reviews = store.get_reviews_for_task(task.id)
        assert len(reviews) == 1
        assert reviews[0].task_type == 'review'
        assert store.get(task.id).merge_status != "merged"

    def test_advance_requires_review_true_create_false_skips_unreviewed(self, tmp_path: Path):
        """advance skips unreviewed implement tasks when advance_create_reviews=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_create_reviews: false\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'skip'

    def test_advance_requires_review_false_merges_unreviewed(self, tmp_path: Path):
        """advance merges unreviewed implement tasks when advance_requires_review=False."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_requires_review: false\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        rc = cmd_advance(args)

        assert rc == 0
        updated_task = store.get(task.id)
        assert updated_task is not None
        assert updated_task.merge_status == "merged"
        assert store.get_reviews_for_task(task.id) == []

    def test_advance_review_cleared_always_merges_regardless_of_config(self, tmp_path: Path):
        """advance merges when review is cleared by improve, even with advance_requires_review=True."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_create_reviews: true\n"
            "advance_requires_review: true\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nFix things."
        store.update(review_task)

        # Mark review as cleared (simulates improve task having run)
        task.review_cleared_at = review_task.completed_at + timedelta(microseconds=1)
        store.update(task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0):
            rc = cmd_advance(args)

        assert rc == 0
        assert store.get(task.id).merge_status == "merged"

    # Planned test #5 (advance_requires_review=True, APPROVED review → merge) is covered by
    # the pre-existing test_advance_merges_approved_task, which verifies this happy path.

    def test_advance_default_config_creates_review_for_unreviewed(self, tmp_path: Path):
        """advance creates a review for unreviewed implement tasks with default config."""
        # Default config — no explicit advance_* flags
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        config = Config.load(tmp_path)
        # Defaults: advance_create_reviews=True, advance_requires_review=True
        assert config.advance_create_reviews is True
        assert config.advance_requires_review is True

        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def test_advance_failed_review_is_treated_as_unreviewed(self, tmp_path: Path):
        """Failed review tasks should not block creating a required review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        failed_review = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        failed_review.status = "failed"
        failed_review.completed_at = datetime.now(UTC)
        failed_review.output_content = "**Verdict: APPROVED**"
        store.update(failed_review)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def _create_completed_improve(self, store, impl_task, review_task):
        """Create a completed improve task for the given impl and review tasks."""
        improve = store.add(
            f"Improve {impl_task.id}",
            task_type="improve",
            depends_on=review_task.id,
            based_on=impl_task.id,
            same_branch=True,
        )
        improve.status = "completed"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        return improve

    def test_advance_skips_task_at_max_review_cycles(self, tmp_path: Path):
        """advance skips task when completed improve count >= max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 2\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improve tasks (= max_review_cycles)
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        assert config.max_review_cycles == 2

        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'max_cycles_reached'
        assert 'max review cycles' in action['description']
        assert '2' in action['description']

    def test_advance_creates_improve_when_under_cycle_limit(self, tmp_path: Path):
        """advance creates an improve task when completed cycles < max_review_cycles."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 3\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 1 completed improve (below limit of 3)
        self._create_completed_improve(store, task, review_task)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'improve'

    def test_advance_rebase_after_review_forces_new_review(self, tmp_path: Path):
        """advance creates a new review when rebase completed after latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Create a completed APPROVED review (completed first)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t0
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        # Create a completed rebase (completed AFTER the review)
        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t1
        store.update(rebase_task)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'create_review'
        assert 'rebase' in action['description'].lower()

    def test_advance_rebase_after_review_idempotent(self, tmp_path: Path):
        """advance does not create duplicate reviews after a rebase — reuses pending review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Completed review, then completed rebase after it
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t0
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t1
        store.update(rebase_task)

        # First call should want to create a review
        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'create_review'

        # Simulate the review being created (pending)
        new_review = store.add(
            f"Review {task.id} (post-rebase)",
            task_type="review",
            depends_on=task.id,
        )

        # Second call should run the pending review, not create another
        action2 = evaluate_advance_rules(config, store, git, task, "main")
        assert action2['type'] == 'run_review'
        assert str(new_review.id) in action2['description']

    def test_advance_rebase_before_review_does_not_force_new_review(self, tmp_path: Path):
        """advance does NOT force new review when rebase completed before the latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)

        # Create a completed rebase (completed first)
        rebase_task = store.add(
            f"Rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        rebase_task.status = "completed"
        rebase_task.completed_at = t0
        store.update(rebase_task)

        # Create a completed APPROVED review (completed AFTER the rebase)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t1
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        # Should proceed to merge, not force a new review
        assert action['type'] != 'create_review'

    def test_advance_no_rebases_no_effect_on_review(self, tmp_path: Path):
        """advance with no rebase tasks does not affect review flow."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a completed APPROVED review (no rebase at all)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        # Should merge, not create another review
        assert action['type'] == 'merge'

    def test_advance_multiple_rebases_only_latest_matters(self, tmp_path: Path):
        """advance checks only the latest rebase against the latest review."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 1, 1, 12, 0, 1, tzinfo=UTC)
        t2 = datetime(2026, 1, 1, 12, 0, 2, tzinfo=UTC)

        # Old rebase (before review)
        old_rebase = store.add(
            f"Old rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        old_rebase.status = "completed"
        old_rebase.completed_at = t0
        store.update(old_rebase)

        # Review (after old rebase)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = t1
        review_task.output_content = "**Verdict: APPROVED**\n\nLooks good."
        store.update(review_task)

        # New rebase (after review) — this is the one that should invalidate
        new_rebase = store.add(
            f"New rebase {task.id}",
            task_type="rebase",
            based_on=task.id,
        )
        new_rebase.status = "completed"
        new_rebase.completed_at = t2
        store.update(new_rebase)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, task, "main")
        assert action['type'] == 'create_review'

    def test_advance_needs_attention_summary_printed(self, tmp_path: Path):
        """advance prints Needs attention section for actionable skips."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 1\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        # Create a CHANGES_REQUESTED review and 1 completed improve (= max_review_cycles=1)
        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            no_docker=True,
            batch=None,
            max_review_cycles=None,
        )

        with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "Needs attention" in output
        assert f"{task.id}" in output
        assert "max review cycles" in output

    def test_advance_max_review_cycles_cli_override(self, tmp_path: Path):
        """--max-review-cycles overrides the config value."""
        # Config has default max_review_cycles=3; 2 completed improves would normally allow more
        setup_config(tmp_path)
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)

        # Create 2 completed improves
        self._create_completed_improve(store, task, review_task)
        self._create_completed_improve(store, task, review_task)

        # With default max_review_cycles=3, action would be 'improve' (2 < 3)
        config = Config.load(tmp_path)
        action_default = evaluate_advance_rules(config, store, git, task, "main")
        assert action_default['type'] == 'improve'

        # Override to 2 — now 2 completed improves == limit → max_cycles_reached
        config.max_review_cycles = 2
        action_override = evaluate_advance_rules(config, store, git, task, "main")
        assert action_override['type'] == 'max_cycles_reached'

    def test_advance_max_review_cycles_dry_run(self, tmp_path: Path):
        """advance --dry-run shows max_cycles_reached action without executing."""
        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\nmax_review_cycles: 1\n"
        )
        store = make_store(tmp_path)

        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        review_task = store.add(
            f"Review {task.id}",
            task_type="review",
            depends_on=task.id,
        )
        review_task.status = "completed"
        review_task.completed_at = datetime.now(UTC)
        review_task.output_content = "**Verdict: CHANGES_REQUESTED**\n\nPlease fix."
        store.update(review_task)
        self._create_completed_improve(store, task, review_task)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))
        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "max review cycles" in result.stdout


    def _create_failed_task(self, store, session_id="sess-abc", failure_reason="MAX_STEPS", prompt="Implement feature"):
        """Create a failed task with given failure_reason and session_id."""
        task = store.add(prompt, task_type="implement")
        task.status = "failed"
        task.failure_reason = failure_reason
        task.session_id = session_id
        task.completed_at = datetime.now(UTC)
        task.branch = f"feat/task-{task.id}"
        store.update(task)
        return task

    def test_advance_resumes_max_steps_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_STEPS failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        assert "Created resume task" in result.stdout

        # Verify a resume child task was created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        child = children[0]
        assert child.based_on == failed_task.id
        assert child.session_id == failed_task.session_id

    def test_advance_resumes_max_turns_failed_task(self, tmp_path: Path):
        """advance creates a resume child task and spawns worker for MAX_TURNS failed task."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-xyz", failure_reason="MAX_TURNS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1
        assert children[0].session_id == "sess-xyz"

    def test_advance_skips_test_failure_failed_task(self, tmp_path: Path):
        """advance does not auto-resume TEST_FAILURE failed tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-test", failure_reason="TEST_FAILURE")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_skips_failed_task_at_max_attempts(self, tmp_path: Path):
        """advance skips a failed task when chain depth >= max_resume_attempts."""
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 1\n")
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create a chain: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        store.update(first_resume)

        # max_resume_attempts=1; original is skipped (already has a child),
        # first_resume (depth=1) is skipped (at max attempts)
        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "max resume attempts" in result.stdout

        # Original should NOT get a new resume child (it already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should not have any new children (at max attempts)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 0

    def test_advance_default_resume_budget_is_one(self, tmp_path: Path):
        """advance default max_resume_attempts=1 skips failed task at depth 1."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        store.update(first_resume)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "max resume attempts (1)" in result.stdout
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 0

    def test_evaluate_advance_rules_returns_skip_at_max_resume_attempts(self, tmp_path: Path):
        """Action selection keeps max resume exhaustion on the skip contract."""
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 1\n")
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)

        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        first_resume.branch = f"feat/task-{first_resume.id}"
        store.update(first_resume)

        config = Config.load(tmp_path)
        action = evaluate_advance_rules(config, store, git, first_resume, "main")

        assert action["type"] == "skip"
        assert action["description"] == "SKIP: max resume attempts (1) reached"

    def test_advance_skips_failed_task_with_existing_resume_child(self, tmp_path: Path):
        """advance skips a failed task that already has a pending/in_progress child."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create an existing pending resume child
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "pending"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created (still just the one pre-existing)
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing child

    def test_advance_skips_failed_task_with_completed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child already completed."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Create a completed resume child (simulating a successful resume)
        child = store.add("Implement feature", task_type="implement")
        child.based_on = failed_task.id
        child.status = "completed"
        store.update(child)

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        # No new child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1  # only the pre-existing completed child

    def test_advance_skips_failed_task_with_failed_resume_child(self, tmp_path: Path):
        """advance skips a failed task whose resume child also failed (no double-resume of root)."""
        (tmp_path / "gza.yaml").write_text("project_name: test-project\ndb_path: .gza/gza.db\nmax_resume_attempts: 1\n")
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Original task #198 equivalent — failed with MAX_STEPS
        original = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        # Resume child #213 equivalent — also failed with MAX_STEPS
        child = store.add("Implement feature", task_type="implement")
        child.based_on = original.id
        child.status = "failed"
        child.failure_reason = "MAX_STEPS"
        child.session_id = "sess-abc"
        store.update(child)

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        # The original should NOT appear in the plan — only the child should
        # (and the child should be skipped due to max resume attempts)
        assert f"{original.id}" not in result.stdout
        assert "SKIP: max resume attempts" in result.stdout
        assert "No eligible tasks to advance" in result.stdout

    def test_advance_no_resume_failed_flag_skips(self, tmp_path: Path):
        """advance --no-resume-failed excludes failed tasks from processing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--no-resume-failed", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_dry_run_shows_resume_action(self, tmp_path: Path):
        """advance --dry-run shows resume action without executing."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", "--dry-run", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Would advance" in result.stdout
        assert "Resume" in result.stdout

        # No resume child should have been created
        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_specific_failed_task_id(self, tmp_path: Path):
        """advance with a specific failed resumable task ID works."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="MAX_STEPS")

        result = run_gza("advance", str(failed_task.id), "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 1

    def test_advance_specific_failed_task_id_test_failure_is_not_resumable(self, tmp_path: Path):
        """advance rejects TEST_FAILURE tasks for explicit failed task IDs."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        failed_task = self._create_failed_task(store, session_id="sess-abc", failure_reason="TEST_FAILURE")

        result = run_gza("advance", str(failed_task.id), "--auto", "--project", str(tmp_path))

        assert result.returncode == 1
        assert f"Error: Task {failed_task.id} is not completed (status: failed)" in result.stdout

        children = store.get_based_on_children(failed_task.id)
        assert len(children) == 0

    def test_advance_skips_failed_task_without_session_id(self, tmp_path: Path):
        """advance skips failed tasks without session_id (not resumable)."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Task with no session_id — not resumable
        self._create_failed_task(store, session_id=None, failure_reason="MAX_STEPS")

        result = run_gza("advance", "--auto", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "No eligible tasks" in result.stdout

    def test_advance_max_resume_attempts_flag_overrides_config(self, tmp_path: Path):
        """advance --max-resume-attempts N overrides the config value."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        self._setup_git_repo(tmp_path)

        # Create a chain of depth 1: original (MAX_STEPS) → first_resume (MAX_STEPS)
        original = self._create_failed_task(store, session_id="sess-1", failure_reason="MAX_STEPS")
        first_resume = store.add("Implement feature", task_type="implement")
        first_resume.status = "failed"
        first_resume.failure_reason = "MAX_STEPS"
        first_resume.session_id = "sess-2"
        first_resume.based_on = original.id
        first_resume.completed_at = datetime.now(UTC)
        store.update(first_resume)

        # With --max-resume-attempts 2, original is skipped (has child),
        # first_resume (depth=1 < 2) gets resumed
        result = run_gza("advance", "--auto", "--max-resume-attempts", "2", "--project", str(tmp_path))

        assert result.returncode == 0
        assert "Resume" in result.stdout
        # Original should NOT get a new child (already has first_resume)
        original_children = store.get_based_on_children(original.id)
        assert len(original_children) == 1  # only the pre-existing first_resume
        # first_resume should get a new resume child (depth=1 < max=2)
        first_resume_children = store.get_based_on_children(first_resume.id)
        assert len(first_resume_children) == 1

    def test_advance_prefers_in_progress_review_over_pending_sibling(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        pending_review = store.add(f"Review {task.id} pending", task_type="review", depends_on=task.id)
        pending_review.status = "pending"
        store.update(pending_review)

        in_progress_review = store.add(f"Review {task.id} in-progress", task_type="review", depends_on=task.id)
        in_progress_review.status = "in_progress"
        store.update(in_progress_review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=None,
            new=False,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker:
            rc = cmd_advance(args)

        assert rc == 0
        spawn_worker.assert_not_called()

    def test_advance_runs_pending_review_when_no_in_progress_review_exists(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path)

        pending_review = store.add(f"Review {task.id} pending", task_type="review", depends_on=task.id)
        pending_review.status = "pending"
        store.update(pending_review)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=None,
            new=False,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        with patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker:
            rc = cmd_advance(args)

        assert rc == 0
        spawn_worker.assert_called_once()


    def test_advance_new_batch_spawns_distinct_tasks(self, tmp_path: Path):
        """advance --new --batch N spawns a separate worker for each pending task.

        Regression: previously all N workers were spawned without explicit task IDs,
        so each one peeked at get_next_pending() and all displayed/claimed the same task.
        """
        setup_config(tmp_path)
        store = make_store(tmp_path)

        # Create 4 pending tasks
        t1 = store.add("Task one", task_type="implement")
        t2 = store.add("Task two", task_type="implement")
        t3 = store.add("Task three", task_type="implement")
        t4 = store.add("Task four", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=4,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
        )

        spawned_task_ids: list[int | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            spawned_task_ids.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
        ):
            rc = cmd_advance(args)

        assert rc == 0
        # Each of the 4 pending tasks should have been passed as an explicit task_id
        assert len(spawned_task_ids) == 4
        assert set(spawned_task_ids) == {t1.id, t2.id, t3.id, t4.id}

    def test_advance_new_batch_skips_internal_pending_tasks(self, tmp_path: Path):
        """advance --new should only start runnable pickup tasks, not internal pending rows."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal = store.add("Internal pending", task_type="internal")
        runnable = store.add("Runnable pending", task_type="implement")
        assert internal.id is not None
        assert runnable.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        started: list[str | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            started.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
        ):
            rc = cmd_advance(args)

        assert rc == 0
        assert started == [runnable.id]
        assert internal.id not in started

    def test_advance_iterate_new_batch_needs_rebase_consumes_slot(self, tmp_path: Path):
        """iterate mode accounts for needs_rebase as worker-consuming in --new --batch planning/execution."""
        import io

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)
        git = self._setup_git_repo(tmp_path)
        task = self._create_implement_task_with_branch(store, git, tmp_path, "Conflict task")
        pending = store.add("Pending task", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(task.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        with (
            patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase", "description": "rebase"}),
            patch("gza.cli._spawn_background_iterate_worker", return_value=0) as spawn_iterate,
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 0
        assert pending.id not in output
        assert "Will start 1 new pending task(s)" not in output
        spawn_iterate.assert_called_once()
        spawn_worker.assert_not_called()

    def test_advance_new_excludes_dependency_blocked_pending_from_plan_and_startup(self, tmp_path: Path):
        """Blocked pending tasks must not appear in --new startup plan or be started."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        blocker = store.add("Failed blocker")
        blocker.status = "failed"
        blocker.completed_at = datetime.now(UTC)
        store.update(blocker)
        blocked = store.add("Blocked pending task", depends_on=blocker.id)
        runnable = store.add("Runnable pending task")
        assert blocked.id is not None
        assert runnable.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        started: list[str | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            started.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
        ):
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        assert "Blocked pending task" not in output
        assert "Runnable pending task" in output
        assert started == [runnable.id]

    def test_advance_new_batch_plan_create_implement_consumes_slot_for_planning(self, tmp_path: Path):
        """create_implement must count as worker-consuming in --new planning output."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        plan = store.add("Plan for create_implement action", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        extra_pending = store.add("Extra runnable pending")
        assert extra_pending.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        started: list[str | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            started.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
        ):
                rc = cmd_advance(args)
                output = mock_stdout.getvalue()

        assert rc == 0
        assert "Will start 1 new pending task" not in output
        assert len(started) == 1
        assert started[0] != extra_pending.id
        assert plan.id is not None
        children = store.get_impl_tasks_by_depends_on_or_based_on(plan.id)
        assert any(child.task_type == "implement" and child.depends_on == plan.id for child in children)

    def test_advance_iterate_new_batch_create_implement_consumes_slot(self, tmp_path: Path):
        """iterate mode accounts for create_implement as worker-consuming in --new --batch planning/execution."""
        import io

        (tmp_path / "gza.yaml").write_text(
            "project_name: test-project\ndb_path: .gza/gza.db\n"
            "advance_mode: iterate\n"
        )
        store = make_store(tmp_path)

        plan = store.add("Plan task", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)
        pending = store.add("Pending task", task_type="implement")

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=str(plan.id),
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_iterate_worker", return_value=0) as spawn_iterate,
            patch("gza.cli._spawn_background_worker", return_value=0) as spawn_worker,
            patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            rc = cmd_advance(args)
            output = stdout.getvalue()

        assert rc == 0
        assert pending.id not in output
        assert "Will start 1 new pending task(s)" not in output
        spawn_iterate.assert_called_once()
        spawn_worker.assert_not_called()

    def test_advance_new_batch_needs_rebase_consumes_slot_for_planning(self, tmp_path: Path):
        """needs_rebase must count as worker-consuming in --new planning output."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        task = store.add("Completed unmerged task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/rebase-me"
        store.update(task)
        assert task.id is not None
        store.set_merge_status(task.id, "unmerged")

        extra_pending = store.add("Extra runnable pending")
        assert extra_pending.id is not None

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        started: list[str | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            started.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli.determine_next_action", return_value={"type": "needs_rebase", "description": "Needs rebase"}),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
            patch("sys.stdout", new_callable=io.StringIO) as mock_stdout,
        ):
            rc = cmd_advance(args)
            output = mock_stdout.getvalue()

        assert rc == 0
        assert "Will start 1 new pending task" not in output
        assert len(started) == 1
        assert started[0] != extra_pending.id

    def test_advance_new_picks_freshly_bumped_task_before_older_urgent(self, tmp_path: Path):
        """advance --new should pick a freshly bumped task before older urgent tasks."""
        setup_config(tmp_path)
        store = make_store(tmp_path)

        older_urgent = store.add("Older urgent", urgent=True)
        bumped = store.add("Bumped task")
        assert older_urgent.id is not None
        assert bumped.id is not None
        store.set_urgent(bumped.id, True)

        args = argparse.Namespace(
            project_dir=tmp_path,
            task_id=None,
            dry_run=False,
            auto=True,
            max=None,
            batch=1,
            new=True,
            no_docker=True,
            plans=False,
            unimplemented=False,
            create=False,
            no_resume_failed=False,
            max_resume_attempts=None,
            advance_type=None,
            max_review_cycles=None,
            squash_threshold=None,
            force=False,
        )

        started: list[str | None] = []

        def fake_spawn(_args, _config, task_id=None, quiet=False):
            started.append(task_id)
            return 0

        with (
            patch("gza.cli.Git", return_value=self._mock_git()),
            patch("gza.cli._spawn_background_worker", side_effect=fake_spawn),
        ):
            rc = cmd_advance(args)

        assert rc == 0
        assert started == [bumped.id]
