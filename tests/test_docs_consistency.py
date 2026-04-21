"""Regression checks for canonical operator docs."""

from pathlib import Path


def test_docs_task_type_use_internal_not_learn() -> None:
    """Docs should reflect internal task type in authoritative task-type lists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    learnings_content = (docs_root / "internal" / "learnings.md").read_text()

    # configuration.md should list internal in task type filters
    assert "explore`, `plan`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal`" in config_content

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
        "| `--based-on ID` | Base on previous task by full prefixed task ID",
        "| `--depends-on ID` | Set dependency on another task by full prefixed task ID",
        "| `task_id` | Full prefixed task ID to edit",
        "| `--based-on ID` | Set lineage/parent relationship using a full prefixed task ID",
        "| `--depends-on ID` | Set execution dependency using a full prefixed task ID",
        "| `task_id` | Full prefixed task ID to kill",
        "| `task_id` | Full prefixed task ID to mark as completed",
        "| `task_id` | Full prefixed task ID(s) to merge",
        "| `task_id_or_branch` | Full prefixed task ID or branch name to checkout",
        "| `task_id` | Full prefixed task ID to diff",
        "| `task_id` | Full prefixed task ID for the completed task to open as a PR",
        "| `task_id` | Full prefixed task ID to delete",
        "| `task_id` | Full prefixed task ID to show",
        "| `task_id` | Full prefixed task ID to resume",
        "| `task_id` | Full prefixed task ID to retry",
        "| `task_id` | Full prefixed task ID to rebase",
        "| `impl_task_id` | Full prefixed task ID (implement, improve, review, or fix",
        "| `--review-id ID` | Explicit full prefixed review task ID to base the improve on",
        "| `task_id` | Full prefixed task ID (implement, improve, review, or fix",
        "| `plan_task_id` | Full prefixed completed plan task ID to implement",
        "| `--depends-on ID` | Set dependency on another task by full prefixed task ID",
        "| `task_id` | Specific full prefixed task ID to advance",
        "| `impl_task_id` | Full prefixed implementation task ID to iterate",
        "| `task_id` | Full prefixed task ID to refresh",
        "`task_id` must be a full prefixed task ID (for example `gza-1234`).",
    ]

    for snippet in required_snippets:
        assert snippet in config_content
    assert "{prefix}-{base36}" not in config_content
    assert "`gza-1a2b`" not in config_content


def test_configuration_docs_cover_force_execution_flags_and_prerequisite_unmerged_guidance() -> None:
    """Operator docs should stay in sync with execution --force and failure-recovery behavior."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `--force` | Skip dependency merge precondition checks (run even if depends_on output is not yet merged) |",
        "| `--force` | Skip dependency merge precondition checks when starting the resumed task |",
        "| `--force` | Skip dependency merge precondition checks when starting the retry task |",
        "| `--force` | Skip dependency merge precondition checks when running the improve task |",
        "| `--force` | Skip dependency merge precondition checks when running the implement task |",
        "| `--force` | Skip dependency merge precondition checks when advance starts workers |",
        "| `--force` | Skip dependency merge precondition checks when iterate starts workers |",
        "`PREREQUISITE_UNMERGED`: the resolved completed dependency branch is not reachable",
    ]

    for snippet in required_snippets:
        assert snippet in config_content


def test_skills_docs_do_not_advertise_unsupported_gza_log_task_flag() -> None:
    """docs/skills.md examples should avoid invalid gza log --task invocations."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    skills_content = (docs_root / "skills.md").read_text()

    assert "gza log --task" not in skills_content
    assert "gza log gza-p --task" not in skills_content


def test_configuration_docs_include_comment_command_reference() -> None:
    """Canonical command reference should document `gza comment` and comment visibility output."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "### comment",
        "gza comment <task_id> <text> [options]",
        "| `task_id` | Full prefixed task ID to comment on",
        "When task comments exist, `gza show` also includes a `Comments:` section",
        "When tasks have comments, `gza history` includes a `comments: N` indicator",
    ]
    for snippet in required_snippets:
        assert snippet in config_content


def test_configuration_docs_describe_comments_only_improve_path() -> None:
    """Improve docs should reflect comments-only fallback when no review exists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    assert "unresolved task comments as feedback context" in config_content
    assert "review exists but unresolved comments do" in config_content
    assert "improve still runs using comments-only feedback" in config_content


def test_cli_help_and_skill_docs_use_decimal_task_id_examples() -> None:
    """CLI help and bundled skills should avoid legacy base36 task-ID examples."""
    repo_root = Path(__file__).resolve().parents[1]
    main_content = (repo_root / "src" / "gza" / "cli" / "main.py").read_text()
    config_cmds_content = (repo_root / "src" / "gza" / "cli" / "config_cmds.py").read_text()

    assert "gza-1234" in main_content
    assert "Full prefixed implementation task ID to iterate" in main_content
    assert "gza-1a2b" not in main_content
    assert "{prefix}-{decimal}" in config_cmds_content
    assert "{prefix}-{base36}" not in config_cmds_content
    assert "gza-1a2b" not in config_cmds_content

    skill_names = [
        "gza-plan-review",
        "gza-task-run",
        "gza-task-resume",
        "gza-task-improve",
        "gza-task-fix",
        "gza-task-review",
        "gza-task-info",
        "gza-task-debug",
    ]
    for skill_name in skill_names:
        content = (repo_root / "src" / "gza" / "skills" / skill_name / "SKILL.md").read_text()
        assert "gza-1234" in content
        assert "gza-1a2b" not in content
