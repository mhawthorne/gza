"""Regression checks for canonical operator docs."""

from pathlib import Path


def test_docs_task_type_use_internal_not_learn() -> None:
    """Docs should reflect internal task type in authoritative task-type lists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    learnings_content = (docs_root / "internal" / "learnings.md").read_text()

    # configuration.md should list internal in task type filters
    assert "explore`, `plan`, `implement`, `review`, `improve`, `rebase`, `internal`" in config_content

    # learnings doc should describe internal task mechanics
    assert "skip_learnings=True" in learnings_content
    assert "`gza history --type internal`" in learnings_content

    # Stale "learn" references should not appear
    for content in (config_content, learnings_content):
        assert "--type learn" not in content
        assert "A `learn` task is created" not in content


def test_configuration_docs_require_full_prefixed_ids_for_strict_commands() -> None:
    """Strict-ID command reference entries should consistently require full prefixed IDs."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `task_id` | Specific full prefixed task ID(s) to run",
        "| `task_id` | Full prefixed task ID to kill",
        "| `task_id` | Full prefixed task ID to mark as completed",
        "| `task_id` | Full prefixed task ID(s) to merge",
        "| `task_id_or_branch` | Full prefixed task ID or branch name to checkout",
        "| `task_id` | Full prefixed task ID to diff",
        "| `impl_task_id` | Full prefixed task ID (implement, improve, or review",
        "| `task_id` | Full prefixed task ID (implement, improve, or review",
        "| `plan_task_id` | Full prefixed completed plan task ID to implement",
        "| `task_id` | Specific full prefixed task ID to advance",
        "| `impl_task_id` | Full prefixed implementation task ID to cycle",
        "| `task_id` | Full prefixed task ID to refresh",
    ]

    for snippet in required_snippets:
        assert snippet in config_content
