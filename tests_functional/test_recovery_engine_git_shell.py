from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import gza.recovery_engine as recovery_engine
from gza.db import Task
from gza.git import Git
from tests.cli.conftest import make_store, setup_config


class _CountingRealGit(Git):
    def __init__(self, repo_dir: Path) -> None:
        super().__init__(repo_dir)
        self.run_count = 0

    def _run(self, *args: str, **kwargs: Any) -> Any:  # type: ignore[override]
        self.run_count += 1
        return super()._run(*args, **kwargs)


def _commit_file(repo: Path, git: Git, path: str, content: str, message: str, date: str) -> str:
    file_path = repo / path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    git._run("add", path)
    dated_git = Git(repo, env={"GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date})
    dated_git._run("commit", "-m", message)
    return git.rev_parse("HEAD")


def test_real_batch_recorded_head_proofs_match_per_branch_with_skewed_history(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    setup_git = Git(repo)
    setup_git._run("init", "-b", "main")
    setup_git._run("config", "user.email", "gza@example.com")
    setup_git._run("config", "user.name", "Gza Test")
    _commit_file(repo, setup_git, "base.txt", "base\n", "base", "2026-01-01T00:00:00Z")

    setup_git._run("checkout", "-b", "recorded/missing", "main")
    _commit_file(repo, setup_git, "missing.txt", "missing\n", "missing older patch", "2030-01-01T00:00:00Z")
    missing_represented = _commit_file(
        repo,
        setup_git,
        "represented-missing.txt",
        "represented\n",
        "represented tip patch",
        "2000-01-01T00:00:00Z",
    )
    missing_head = setup_git.rev_parse("HEAD")
    setup_git._run("checkout", "main")
    setup_git._run("cherry-pick", missing_represented)
    setup_git._run("branch", "feature/missing-recorded-head")

    setup_git._run("checkout", "-b", "recorded/all", "main")
    all_older = _commit_file(
        repo,
        setup_git,
        "all-older.txt",
        "all older\n",
        "all older patch",
        "2031-01-01T00:00:00Z",
    )
    all_tip = _commit_file(repo, setup_git, "all-tip.txt", "all tip\n", "all tip patch", "2001-01-01T00:00:00Z")
    all_head = setup_git.rev_parse("HEAD")
    setup_git._run("checkout", "main")
    setup_git._run("branch", "feature/all-recorded-head")
    setup_git._run("cherry-pick", all_older)
    setup_git._run("cherry-pick", all_tip)

    setup_git._run("checkout", "-b", "recorded/merge-left", "main")
    merge_left = _commit_file(
        repo,
        setup_git,
        "merge-left.txt",
        "left\n",
        "merge left patch",
        "2032-01-01T00:00:00Z",
    )
    setup_git._run("checkout", "-b", "recorded/merge-right", "main")
    merge_right = _commit_file(
        repo,
        setup_git,
        "merge-right.txt",
        "right\n",
        "merge right patch",
        "2033-01-01T00:00:00Z",
    )
    setup_git._run("checkout", "recorded/merge-left")
    merge_git = Git(repo, env={"GIT_AUTHOR_DATE": "2002-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2002-01-01T00:00:00Z"})
    merge_git._run("merge", "--no-ff", "recorded/merge-right", "-m", "recorded merge")
    merge_head = setup_git.rev_parse("HEAD")
    setup_git._run("checkout", "main")
    setup_git._run("branch", "feature/merge-recorded-head")
    setup_git._run("cherry-pick", merge_left)
    setup_git._run("cherry-pick", merge_right)

    setup_git._run("checkout", "-b", "recorded/descendant", "main")
    _commit_file(
        repo,
        setup_git,
        "descendant-base.txt",
        "descendant base\n",
        "descendant recorded older patch",
        "2034-01-01T00:00:00Z",
    )
    descendant_recorded_head = _commit_file(
        repo,
        setup_git,
        "descendant-tip.txt",
        "descendant tip\n",
        "descendant recorded tip patch",
        "2035-01-01T00:00:00Z",
    )
    setup_git._run("checkout", "-b", "feature/descends-recorded-head", "recorded/descendant")
    _commit_file(
        repo,
        setup_git,
        "descendant-child.txt",
        "descendant child\n",
        "descendant source child patch",
        "1999-01-01T00:00:00Z",
    )
    assert setup_git.ancestor_relationships(
        ((descendant_recorded_head, "feature/descends-recorded-head"),)
    ) == {
        (descendant_recorded_head, "feature/descends-recorded-head"): True,
    }

    cases = (
        ("feature/missing-recorded-head", missing_head, ("unmerged", "recorded-head-has-net-diff")),
        ("feature/all-recorded-head", all_head, ("redundant", "no-unique-commits-with-task-commits")),
        ("feature/merge-recorded-head", merge_head, ("redundant", "no-unique-commits-with-task-commits")),
        ("feature/descends-recorded-head", descendant_recorded_head, ("unmerged", "not-equivalent")),
    )
    tasks: list[Task] = []
    requests: list[recovery_engine._FailedTaskBranchClassificationPreload] = []
    for branch, recorded_head, _expected in cases:
        task = store.add(f"Failed {branch}", task_type="review")
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "WORKER_DIED"
        task.branch = branch
        task.has_commits = True
        task.completed_at = datetime.now(UTC)
        store.update(task)
        tasks.append(task)
        requests.append(
            recovery_engine._FailedTaskBranchClassificationPreload(
                source_ref=branch,
                target_branch="main",
                recorded_head_sha=recorded_head,
                source_has_commits=True,
            )
        )

    ordinary_git = _CountingRealGit(repo)
    ordinary = [
        recovery_engine._classify_failed_task_branch_merge_state_for_target(
            git=ordinary_git,
            failed_task=task,
            target_branch="main",
            recorded_head_sha=recorded_head,
            source_has_commits=True,
            source_ref=branch,
        )
        for task, (branch, recorded_head, _expected) in zip(tasks, cases, strict=True)
    ]

    batch_git = _CountingRealGit(repo)
    batched = recovery_engine._batch_classify_failed_task_branch_merge_states(
        git=batch_git,
        requests=tuple(requests),
    )

    ordinary_pairs = [(item.state, item.reason) for item in ordinary]
    batched_pairs = [(batched[request].state, batched[request].reason) for request in requests]
    assert ordinary_pairs == [expected for _branch, _recorded_head, expected in cases]
    assert batched_pairs == ordinary_pairs
    assert batch_git.run_count <= 10
