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
