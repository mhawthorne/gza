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


def test_agents_document_pytest_hang_triage_rules() -> None:
    """AGENTS.md should tell operators to treat silent pytest runs as suspect."""
    repo_root = Path(__file__).resolve().parents[1]
    agents_content = (repo_root / "AGENTS.md").read_text()

    required_snippets = [
        "## Pytest hangs",
        "If `uv run pytest tests/` produces no new output for about 2 minutes, kill it and bisect by file or class.",
        "CPU usage is a poor liveness signal because an infinite loop also pegs a core.",
        "the mock must also mark the spawned task complete or the loop spins forever",
        "test_iterate_failed_improve_non_attention_skip_does_not_emit_needs_attention",
    ]
    for snippet in required_snippets:
        assert snippet in agents_content


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
        "| `task_id` | Specific full prefixed task ID to advance",
        "| `impl_task_id` | Full prefixed implementation task ID to iterate",
        "| `task_id` | Full prefixed task ID to refresh",
        "| `task_id` | Full prefixed task ID(s) whose branch cohorts should be synced",
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
        "`PREREQUISITE_UNMERGED`: the resolved completed dependency is not yet marked merged",
    ]

    for snippet in required_snippets:
        assert snippet in config_content


def test_configuration_docs_describe_unimplemented_lineage_guidance() -> None:
    """advance docs should explain pending-descendant lineage selection and truthful follow-up actions."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "| `--unimplemented` | List unimplemented plan/explore source rows, preferring newer descendants within each lineage branch |",
        "| `--create` | With `--unimplemented`: queue implement tasks for the listed source rows |",
        "It may surface a newer pending",
        "keeping sibling branches as separate source rows",
        "Only completed plan rows are directly runnable with `uv run gza implement <id>`;",
        "use `uv run gza advance --unimplemented --create` to queue implement tasks",
    ]

    for snippet in required_snippets:
        assert snippet in config_content


def test_configuration_docs_describe_sync_as_broader_explicit_reconciliation_surface() -> None:
    """Canonical docs should keep `uv run gza sync` as the broader explicit branch and PR maintenance surface."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    workflow_example = (docs_root / "examples" / "plan-implement-review.md").read_text()

    required_snippets = [
        "### sync",
        "uv run gza sync [task_id ...] [options]",
        "Use `uv run gza unmerged` for the daily \"what still needs to be merged?\" check.",
        "`uv run gza sync` remains the broader explicit branch and PR reconciliation command.",
        "The only GitHub-side exception outside `uv run gza sync` is improve completion with `--review`",
        "`gza pr` does not reconcile or close stale GitHub PRs",
        "`gza merge` only performs the local git merge/rebase path",
        "`uv run gza sync <impl_id>`",
    ]
    for snippet in required_snippets:
        assert snippet in config_content or snippet in workflow_example


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


def test_configuration_docs_keep_fix_comment_and_run_inline_surfaces() -> None:
    """run-inline docs additions must not replace existing fix/comment operator docs."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    required_snippets = [
        "### run-inline",
        "gza run-inline <task_id> [options]",
        "### search",
        "gza search <term> [options]",
        "Replacing `gza incomplete`",
        "`uv run gza history --incomplete` remains available as a factual unresolved-history filter.",
        "### tv",
        "gza tv [task_id ...] [options]",
        "### comment",
        "gza comment <task_id> <text> [options]",
        "### fix",
        "gza fix <task_id> [options]",
        "| `--type TYPE` | Filter by task type: `explore`, `plan`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal` |",
    ]
    for snippet in required_snippets:
        assert snippet in config_content


def test_summary_docs_and_skill_use_dedicated_triage_surfaces() -> None:
    """`/gza-summary` docs should synthesize dedicated surfaces instead of reviving `gza incomplete`."""
    repo_root = Path(__file__).resolve().parents[1]
    skills_doc_content = (repo_root / "docs" / "skills.md").read_text()
    skill_content = (repo_root / "src" / "gza" / "skills" / "gza-summary" / "SKILL.md").read_text()

    required_snippets = [
        "uv run gza history --status failed",
        "uv run gza advance --unimplemented",
        "uv run gza unmerged",
        "uv run gza next --all",
        "/gza-summary",
        "Failed Recovery",
        "Queue State",
    ]
    for snippet in required_snippets:
        assert snippet in skills_doc_content
        assert snippet in skill_content

    assert "git merge" not in skills_doc_content
    assert "git merge" not in skill_content
    assert "factual failed-attempt history" in skills_doc_content
    assert "factual failed-task history" in skill_content
    assert "unresolved failed tasks" not in skill_content
    assert "not a canonical replacement for `gza incomplete`" in skill_content


def test_configuration_docs_describe_comments_only_improve_path() -> None:
    """Improve docs should reflect comments-only fallback when no review exists."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()

    assert "unresolved task comments as feedback context" in config_content
    assert "review exists but unresolved comments do" in config_content
    assert "improve still runs using comments-only feedback" in config_content


def test_plan_implement_review_example_uses_uv_run_gza_shell_snippets() -> None:
    """Workflow example should not mix bare gza shell snippets with uv run gza guidance."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    example_content = (docs_root / "examples" / "plan-implement-review.md").read_text()

    required_snippets = [
        "$ uv run gza add --type implement --based-on gza-1 --review \"Implement...\"",
        "$ uv run gza add --type implement --based-on gza-1 --review --pr \"Implement...\"",
    ]
    for snippet in required_snippets:
        assert snippet in example_content

    for line in example_content.splitlines():
        stripped = line.lstrip()
        assert not stripped.startswith("$ gza ")
        assert not stripped.startswith("> $ gza ")


def test_recovery_docs_use_uv_run_gza_on_touched_recovery_surfaces() -> None:
    """Touched recovery docs should keep canonical `uv run gza ...` command wording."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    failed_tasks_content = (docs_root / "examples" / "failed-tasks.md").read_text()

    advance_section = config_content.split("### advance", 1)[1].split("### iterate", 1)[0]
    iterate_section = config_content.split("### iterate", 1)[1].split("### watch", 1)[0]
    watch_section = config_content.split("### watch", 1)[1].split("### learnings", 1)[0]

    assert "uv run gza advance [task_id] [options]" in advance_section
    assert "uv run gza iterate <impl_task_id> [options]" in iterate_section
    assert "uv run gza watch [options]" in watch_section
    assert "`uv run gza watch --restart-failed --dry-run` is the recovery inspection surface" in watch_section
    assert "Plain `uv run gza watch` and `uv run gza watch --restart-failed` both use the same bounded shared recovery policy" in watch_section
    assert "use `uv run gza queue --tag TAG` to preview the same scoped pickup order" in watch_section

    assert "\ngza advance [task_id] [options]\n" not in advance_section
    assert "\ngza iterate <impl_task_id> [options]\n" not in iterate_section
    assert "\ngza watch [options]\n" not in watch_section
    assert "`gza watch --restart-failed --dry-run` is the recovery inspection surface" not in watch_section
    assert "Plain `gza watch` and `--restart-failed` both use the same bounded shared recovery policy" not in watch_section

    assert "| `uv run gza resume` | Continue from where it left off |" in failed_tasks_content
    assert "| `uv run gza retry` | Start completely fresh |" in failed_tasks_content
    assert "| `uv run gza watch --restart-failed` | Drain actionable failed tasks before pending queue work, choosing `resume` or `retry` per task |" in failed_tasks_content
    assert "`uv run gza watch --restart-failed` adds an explicit recovery phase ahead of normal pending work." in failed_tasks_content

    for line in failed_tasks_content.splitlines():
        assert not line.lstrip().startswith("$ gza ")


def test_watch_attention_docs_describe_sticky_manual_attention_behavior() -> None:
    """Watch docs should describe sticky ATTENTION reminders for manual advance outcomes."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    internal_content = (docs_root / "internal" / "advance-workflow.md").read_text()

    watch_section = config_content.split("### watch", 1)[1].split("### learnings", 1)[0]

    assert "surfaced as `ATTENTION` lines in watch output instead of one-shot deduped `SKIP` lines" in watch_section
    assert "Ordinary wait/skip states keep the existing `SKIP` dedupe behavior." in watch_section
    assert "sticky `ATTENTION` log lines instead of deduped `SKIP` lines" in internal_content
    assert "Ordinary watch skip/wait lines remain deduped across cycles." in internal_content


def test_internal_advance_workflow_task_collection_tracks_shared_recovery_policy() -> None:
    """Internal advance workflow docs should describe shared failed-task recovery collection policy."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    internal_content = (docs_root / "internal" / "advance-workflow.md").read_text()
    task_collection_section = internal_content.split("## Task Collection", 1)[1].split("## Configuration", 1)[0]

    assert "Advance collects tasks from three sources" in task_collection_section
    assert "store.list_failed_tasks_for_recovery(...)" in task_collection_section
    assert "decide_failed_task_recovery(...)" in task_collection_section
    assert "resume`, `retry`, or manual review required" in task_collection_section
    assert "failure_reason IN ('MAX_STEPS', 'MAX_TURNS')" not in task_collection_section
    assert "session_id IS NOT NULL" not in task_collection_section
    assert "**Resumable failed tasks**" not in task_collection_section


def test_docker_setup_command_docs_describe_prewarm_hook_and_race_avoidance() -> None:
    """Docker config docs should explain pre-warm semantics and why first-use lazy installs race."""
    docs_root = Path(__file__).resolve().parents[1] / "docs"
    config_content = (docs_root / "configuration.md").read_text()
    docker_content = (docs_root / "docker.md").read_text()

    required_config_snippets = [
        "### Docker Pre-Warm Hook (`docker_setup_command`)",
        "Runs synchronously inside the container before the provider CLI process starts.",
        "so setup does not race with parallel tool calls or subagents.",
        "dependency installs are often lazy on the first CLI invocation.",
        'docker_setup_command: "uv sync"',
        "poetry install --no-interaction",
        "pip install -e .",
        "npm ci",
    ]
    for snippet in required_config_snippets:
        assert snippet in config_content

    required_docker_snippets = [
        "## Pre-Warm Dependencies with `docker_setup_command`",
        "Runs inside the container before the provider CLI starts.",
        "Runs synchronously in a single process.",
        "Completes before the agent can issue tool calls.",
    ]
    for snippet in required_docker_snippets:
        assert snippet in docker_content


def test_improve_related_skills_describe_comments_as_feedback_source() -> None:
    """Bundled improve-related skills should mention unresolved task comments as a first-class
    feedback source and describe the comments-only fallback when no review exists.

    Regression: `gza-task-improve/SKILL.md` and `gza-task-add/SKILL.md` previously framed improve
    purely around review feedback, which steered operators and agents away from valid
    comments-only improve flows after the feature landed.
    """
    repo_root = Path(__file__).resolve().parents[1]

    improve_skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-improve" / "SKILL.md"
    ).read_text()
    assert "unresolved task comments" in improve_skill_content
    assert "comments-only" in improve_skill_content
    assert "resolve_comments" in improve_skill_content

    add_skill_content = (
        repo_root / "src" / "gza" / "skills" / "gza-task-add" / "SKILL.md"
    ).read_text()
    assert "unresolved task comments" in add_skill_content
    assert "comments-only improve is supported" in add_skill_content


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
        "gza-explore-summarize",
        "gza-plan-review",
        "gza-plan-improve",
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
