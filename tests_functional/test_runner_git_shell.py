"""Functional tests for runner review context that require a real git repo."""

import re

from gza.db import SqliteTaskStore
from gza.runner import _build_context_from_chain
from tests_functional.git_helpers import init_basic_repo


def test_review_context_uses_real_git_shas_for_diff_audit_metadata(tmp_path) -> None:
    """Review diff metadata should resolve real SHAs from git refs."""
    git = init_basic_repo(tmp_path)
    feature_branch = "test/feature-branch"

    base_sha = git.rev_parse("HEAD")

    git._run("checkout", "-b", feature_branch)
    (tmp_path / "feature.txt").write_text("feature change\n")
    git._run("add", "feature.txt")
    git._run("commit", "-m", "feature change")
    feature_sha = git.rev_parse("HEAD")

    git._run("checkout", "main")
    (tmp_path / "main.txt").write_text("main change\n")
    git._run("add", "main.txt")
    git._run("commit", "-m", "main change")
    main_sha = git.rev_parse("HEAD")

    merge_base_sha = git.merge_base("main", feature_branch)
    assert merge_base_sha == base_sha

    store = SqliteTaskStore(tmp_path / "test.db")
    impl_task = store.add(prompt="Implement feature", task_type="implement")
    impl_task.status = "completed"
    impl_task.branch = feature_branch
    store.update(impl_task)

    review_task = store.add(
        prompt="Review implementation",
        task_type="review",
        depends_on=impl_task.id,
    )

    context = _build_context_from_chain(review_task, store, tmp_path, git)

    assert f"Implementation head: {feature_branch} ({feature_sha})" in context
    assert f"Local default branch: main ({main_sha})" in context
    assert f"Review base (merge-base): {merge_base_sha}" in context
    assert "Revision range: main...test/feature-branch" in context

    metadata_lines = {
        "Implementation head": next(
            line for line in context.splitlines() if line.startswith("Implementation head:")
        ),
        "Local default branch": next(
            line for line in context.splitlines() if line.startswith("Local default branch:")
        ),
        "Review base (merge-base)": next(
            line for line in context.splitlines() if line.startswith("Review base (merge-base):")
        ),
    }
    for line in metadata_lines.values():
        assert re.search(r"\b[0-9a-f]{40}\b", line), line
