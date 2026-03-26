"""Regression checks for canonical operator docs."""

from pathlib import Path


def test_agents_task_type_docs_use_internal_not_learn() -> None:
    """AGENTS.md should reflect internal task type in authoritative task-type lists."""
    agents_path = Path(__file__).resolve().parents[1] / "AGENTS.md"
    content = agents_path.read_text()

    required_clauses = [
        "Filter by task type (explore, plan, implement, review, improve, rebase, internal)",
        "An `internal` task is created in the DB with `skip_learnings=True`",
        "visible via `gza history --type internal`",
        "This is NOT added to `explore`, `plan`, `review`, or `internal` tasks",
    ]
    for clause in required_clauses:
        assert clause in content

    stale_clauses = [
        "Filter by task type (explore, plan, implement, review, improve, learn)",
        "Filter by task type (explore, plan, implement, review, improve, internal)",
        "A `learn` task is created in the DB",
        "visible via `gza history --type learn`",
        "This is NOT added to `explore`, `plan`, `review`, or `learn` tasks",
    ]
    for clause in stale_clauses:
        assert clause not in content
