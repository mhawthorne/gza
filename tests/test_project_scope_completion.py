from pathlib import Path
from unittest.mock import Mock, patch

from gza.config import Config
from gza.db import SqliteTaskStore, TaskStats
from gza.git import Git
from gza.runner import (
    ProjectBoundary,
    ResolvedLocalDependency,
    _complete_code_task,
)


def _make_task_context(tmp_path: Path):
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path)
    task = store.add(prompt="Implement scope gate", task_type="implement")
    task.slug = "20260530-scope-gate"
    store.mark_in_progress(task)

    config = Config(project_dir=tmp_path, project_name="test-project", use_docker=False)
    log_file = tmp_path / "task.log"
    log_file.write_text("")
    summary_dir = tmp_path / ".gza" / "summaries"
    summary_path = summary_dir / f"{task.slug}.md"
    worktree_summary_path = tmp_path / "worktree" / ".gza" / "summaries" / f"{task.slug}.md"
    worktree_summary_path.parent.mkdir(parents=True, exist_ok=True)
    worktree_summary_path.write_text("## Summary\n\n- done\n")
    return store, task, config, log_file, summary_dir, summary_path, worktree_summary_path


def test_complete_code_task_fails_on_out_of_scope_changes(tmp_path: Path) -> None:
    store, task, config, log_file, summary_dir, summary_path, worktree_summary_path = _make_task_context(tmp_path)
    config.project_dir = tmp_path / "services" / "api"
    config.project_dir.mkdir(parents=True)
    log_file = config.project_dir / "task.log"
    log_file.write_text("")
    summary_dir = config.project_dir / ".gza" / "summaries"
    summary_path = summary_dir / f"{task.slug}.md"

    worktree_git = Mock(spec=Git)
    worktree_git.status_porcelain.return_value = {
        ("M", "services/api/app.py"),
        ("M", "services/other/outside.py"),
    }

    boundary = ProjectBoundary(repo_root=tmp_path, scope_root=Path("services/api"))

    rc = _complete_code_task(
        task,
        config,
        store,
        worktree_git,
        log_file,
        "feature/scope-gate",
        TaskStats(duration_seconds=1.0, num_steps_reported=1, cost_usd=0.01),
        0,
        pre_run_status=set(),
        worktree_summary_path=worktree_summary_path,
        summary_path=summary_path,
        summary_dir=summary_dir,
        project_boundary=boundary,
    )

    assert rc == 1
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "failed"
    assert refreshed.failure_reason == "PROJECT_SCOPE_VIOLATION"
    worktree_git.add.assert_not_called()
    worktree_git.commit.assert_not_called()


def test_complete_code_task_root_scope_is_noop(tmp_path: Path) -> None:
    store, task, config, log_file, summary_dir, summary_path, worktree_summary_path = _make_task_context(tmp_path)
    worktree_git = Mock(spec=Git)
    worktree_git.status_porcelain.return_value = {("M", "other-project/outside.py")}
    worktree_git.default_branch.return_value = "main"
    worktree_git.get_diff_numstat.return_value = "1\t0\tother-project/outside.py\n"

    boundary = ProjectBoundary(repo_root=tmp_path, scope_root=Path("."))

    with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "feature/root-scope",
            TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            0,
            pre_run_status=set(),
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_dir,
            project_boundary=boundary,
        )

    assert rc == 0
    staged_files = [call.args[0] for call in worktree_git.add.call_args_list]
    assert staged_files == ["other-project/outside.py"]


def test_complete_code_task_allows_in_repo_dependency_paths(tmp_path: Path) -> None:
    store, task, config, log_file, summary_dir, summary_path, worktree_summary_path = _make_task_context(tmp_path)
    worktree_git = Mock(spec=Git)
    worktree_git.status_porcelain.return_value = {
        ("M", "services/api/app.py"),
        ("M", "libs/shared/helpers.py"),
    }
    worktree_git.default_branch.return_value = "main"
    worktree_git.get_diff_numstat.return_value = "1\t0\tservices/api/app.py\n1\t0\tlibs/shared/helpers.py\n"

    boundary = ProjectBoundary(
        repo_root=tmp_path,
        scope_root=Path("services/api"),
        local_dependencies=(
            ResolvedLocalDependency(
                raw_path="../../libs/shared",
                resolved_path=tmp_path / "libs" / "shared",
                repo_relative_path=Path("libs/shared"),
            ),
        ),
    )

    with patch("gza.runner._squash_wip_commits"), patch("gza.runner.maybe_auto_regenerate_learnings", return_value=None):
        rc = _complete_code_task(
            task,
            config,
            store,
            worktree_git,
            log_file,
            "feature/with-dep",
            TaskStats(duration_seconds=1.0, num_steps_reported=2, cost_usd=0.02),
            0,
            pre_run_status=set(),
            worktree_summary_path=worktree_summary_path,
            summary_path=summary_path,
            summary_dir=summary_dir,
            project_boundary=boundary,
        )

    assert rc == 0
    staged_files = {call.args[0] for call in worktree_git.add.call_args_list}
    assert staged_files == {"services/api/app.py", "libs/shared/helpers.py"}
