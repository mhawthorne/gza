from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.git import Git
from gza.recovery_engine import list_failed_tasks_for_recovery
from tests.cli.conftest import make_store, setup_config

pytestmark = pytest.mark.integration


def _init_git_repo(tmp_path: Path) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "README.md").write_text("initial\n")
    git._run("add", "README.md")
    git._run("commit", "-m", "Initial commit")
    return git


def test_list_failed_tasks_for_recovery_filters_branch_already_merged_into_main(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    git = _init_git_repo(tmp_path)

    failed = store.add("Failed implementation", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-failed"
    failed.branch = "feature/landed-work"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    git._run("checkout", "-b", failed.branch)
    (tmp_path / "landed.txt").write_text("landed\n")
    git._run("add", "landed.txt")
    git._run("commit", "-m", "Add landed work")
    git._run("checkout", "main")
    git._run("merge", "--no-ff", failed.branch, "-m", "Merge landed work")

    assert list_failed_tasks_for_recovery(store) == []
