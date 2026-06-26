"""Prompt building module for gza tasks.

Centralizes all prompt strings into template files and provides a clean API
for assembling prompts by task type.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from gza.plan_review_verdict import SLICE_COMPLEXITIES
from gza.project_discovery import discover_repo_project_configs

if TYPE_CHECKING:
    from gza.config import Config
    from gza.db import SqliteTaskStore, Task
    from gza.git import Git

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_template(name: str) -> str:
    """Load a prompt template file by name."""
    return (_TEMPLATE_DIR / name).read_text()


def _get_optional_verify_command(config: Config, field_name: str) -> str:
    """Return a configured verify command or raise on invalid prompt inputs."""
    config_dict = getattr(config, "__dict__", {})
    if isinstance(config_dict, dict):
        value = config_dict.get(field_name, "")
    else:
        value = getattr(config, field_name, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise TypeError(f"config.{field_name} must be a string")
    return value


def _cross_project_verify_instructions(task: Task, config: Config) -> str:
    """Build additional verification guidance for cross-project code tasks."""
    tags = tuple(getattr(task, "tags", ()) or ())
    if "cross-project" not in tags:
        return ""

    lines = [
        "Cross-project verification policy:",
        "- `cross-project` widens allowed change scope; it does not change the primary execution root.",
        "- If you modify multiple project roots, run each affected project's configured verification from that project's own root before finishing.",
        "- If an affected project has no `verify_command`, explicitly note that it was skipped; do not assume that project passed.",
    ]
    for project in discover_repo_project_configs(config):
        scope = "." if project.scope_root == Path(".") else project.scope_root.as_posix()
        if project.verify_command:
            line = f"- Project `{scope}` final verify: `{project.verify_command}`"
            if project.inner_verify_command:
                line += f" (inner-loop: `{project.inner_verify_command}`)"
        else:
            line = f"- Project `{scope}` has no `verify_command`; affected changes there must be reported as skipped verification."
        lines.append(line)
    return "\n".join(lines)


def _code_task_verify_instructions(task: Task, config: Config) -> str:
    """Build the code-task verification policy block for prompts."""
    final_command = _get_optional_verify_command(config, "verify_command")
    inner_command = _get_optional_verify_command(config, "inner_verify_command")
    if not final_command:
        return _cross_project_verify_instructions(task, config)

    lines = [
        "Verification policy for this code task:",
        "- During editing, use fast verification instead of rerunning the full final suite after every change.",
    ]
    if inner_command:
        lines.append(f"- Preferred inner-loop verify command: `{inner_command}`")
    else:
        lines.append("- No inner-loop command is configured; use targeted tests/lint/type checks for the files you changed.")
    lines.extend(
        [
            f"- Required final verify command: `{final_command}`",
            "- Run the full final verify command once after your last code change.",
            "- If the final verify fails, fix the failures and rerun it. Do not rerun previously successful heavy phases unless your later edits require it.",
        ]
    )
    cross_project = _cross_project_verify_instructions(task, config)
    if cross_project:
        lines.extend(["", cross_project])
    return "\n".join(lines)


class PromptBuilder:
    """Builds prompts for gza tasks using template files.

    Provides a clean API for assembling prompts by task type. All prompt
    text is stored in template files under src/gza/prompts/templates/.
    """

    def build(
        self,
        task: Task,
        config: Config,
        store: SqliteTaskStore,
        report_path: Path | None = None,
        summary_path: Path | None = None,
        git: Git | None = None,
        review_verify_result: str | None = None,
    ) -> str:
        """Build the full prompt for a task.

        Args:
            task: The task to build a prompt for.
            config: Project configuration.
            store: Task store for looking up related tasks.
            report_path: Path where explore/plan/review output should be written.
            summary_path: Path where task/implement/improve summary should be written.
            git: Git instance for diff context in review tasks.

        Returns:
            The assembled prompt string.
        """
        base_prompt = f"Complete this task: {task.prompt}"

        # Include spec file content if specified
        if task.spec:
            spec_path = config.project_dir / task.spec
            if spec_path.exists():
                spec_content = spec_path.read_text()
                base_prompt += (
                    f"\n\n## Specification\n\n"
                    f"The following specification file ({task.spec}) provides context for this task:\n\n"
                    f"{spec_content}"
                )

        # Mention learnings file if it exists and task doesn't opt out.
        # Internal tasks are gza-owned orchestration tasks and should not
        # implicitly inherit learnings context.
        learnings_available = False
        if task.task_type != "internal" and not task.skip_learnings:
            learnings_path = config.project_dir / ".gza" / "learnings.md"
            if learnings_path.exists():
                learnings_available = True
                base_prompt += (
                    "\n\nProject learnings from previous tasks are available at"
                    " `.gza/learnings.md`. Consult it for relevant patterns and conventions."
                )

        # Point agents to internal docs if the directory exists
        docs_internal = config.project_dir / "docs" / "internal"
        if docs_internal.is_dir():
            base_prompt += (
                "\n\nArchitectural notes and internal documentation are available in"
                " `docs/internal/`. Consult relevant files when making design decisions."
            )

        # Add context from lineage/review chains for task types that derive from prior work.
        if task.based_on or task.depends_on or task.task_type in (
            "implement",
            "review",
            "plan_review",
            "plan_improve",
        ):
            from gza.runner import _build_context_from_chain
            context = _build_context_from_chain(
                task,
                store,
                config.project_dir,
                git,
                config=config,
                review_verify_result=review_verify_result,
            )
            if context:
                base_prompt += "\n\n" + context

        if task.recovery_origin == "retry" and task.based_on:
            base_prompt += (
                "\n\nRetry context:\n"
                f"- A prior attempt exists at task {task.based_on}.\n"
                f"- You may run `uv run gza log {task.based_on}` to inspect that attempt's reasoning and transcript before continuing.\n"
                "- Treat that history as optional context; this retry should still succeed from the current worktree state even if you do not consult it."
            )

        # Task type-specific instructions from templates
        if task.task_type == "explore":
            if report_path:
                base_prompt += "\n\n" + _load_template("explore.txt").format(
                    report_path=report_path
                )
        elif task.task_type == "plan":
            if report_path:
                base_prompt += "\n\n" + _load_template("plan.txt").format(
                    report_path=report_path
                )
        elif task.task_type == "plan_review":
            if report_path:
                base_prompt += "\n\n" + _load_template("plan_review.txt").format(
                    report_path=report_path,
                    plan_slice_target_timeout_minutes=config.get_plan_slice_target_timeout_minutes(),
                    max_plan_slices=(
                        config.max_plan_slices
                        if config.max_plan_slices is not None
                        else "unset"
                    ),
                    slice_complexities=", ".join(f"`{value}`" for value in sorted(SLICE_COMPLEXITIES)),
                )
        elif task.task_type == "plan_improve":
            if report_path:
                base_prompt += "\n\n" + _load_template("plan_improve.txt").format(
                    report_path=report_path
                )
        elif task.task_type == "review":
            # Check for REVIEW.md in project root for custom review guidelines
            review_md_path = config.project_dir / "REVIEW.md"
            if review_md_path.exists():
                review_guidelines = review_md_path.read_text()
                base_prompt += f"\n\n## Review Guidelines\n\n{review_guidelines}"

            if report_path:
                base_prompt += "\n\n" + _load_template("review.txt").format(
                    report_path=report_path
                )
        elif task.task_type in ("task", "implement"):
            learnings_check = (
                "- Re-read `.gza/learnings.md` for project-specific patterns that apply to this task."
                if learnings_available
                else ""
            )
            if summary_path:
                base_prompt += _load_template("task_with_summary.txt").format(
                    summary_path=summary_path,
                    learnings_check=learnings_check,
                )
            else:
                base_prompt += _load_template("task_without_summary.txt").format(
                    learnings_check=learnings_check
                )

            verify_instructions = _code_task_verify_instructions(task, config)
            if verify_instructions:
                base_prompt += f"\n\n{verify_instructions}"
        elif task.task_type == "improve":
            base_prompt += "\n\n" + _load_template("improve.txt")
            learnings_check = (
                "- Re-read `.gza/learnings.md` for project-specific patterns that apply to this task."
                if learnings_available
                else ""
            )

            if summary_path:
                base_prompt += _load_template("task_with_summary.txt").format(
                    summary_path=summary_path,
                    learnings_check=learnings_check,
                )
            else:
                base_prompt += _load_template("task_without_summary.txt").format(
                    learnings_check=learnings_check
                )

            verify_instructions = _code_task_verify_instructions(task, config)
            if verify_instructions:
                base_prompt += f"\n\n{verify_instructions}"
        elif task.task_type == "fix":
            base_prompt += "\n\n" + _load_template("fix.txt")
            learnings_check = (
                "- Re-read `.gza/learnings.md` for project-specific patterns that apply to this task."
                if learnings_available
                else ""
            )

            if summary_path:
                base_prompt += _load_template("task_with_summary.txt").format(
                    summary_path=summary_path,
                    learnings_check=learnings_check,
                )
            else:
                base_prompt += _load_template("task_without_summary.txt").format(
                    learnings_check=learnings_check
                )

            verify_instructions = _code_task_verify_instructions(task, config)
            if verify_instructions:
                base_prompt += f"\n\n{verify_instructions}"
        elif task.task_type == "rebase":
            verify_instructions = _code_task_verify_instructions(task, config)
            if verify_instructions:
                base_prompt += f"\n\n{verify_instructions}"
        elif task.task_type in ("internal", "learn"):
            if report_path:
                base_prompt += "\n\n" + _load_template("internal.txt").format(
                    report_path=report_path
                )
        else:
            base_prompt += "\n\nWhen you are done, report what you accomplished."

        return base_prompt

    def resume_prompt(
        self,
        *,
        task_id: str | None = None,
        task_slug: str | None = None,
        report_path: Path | None = None,
        resume_context: str | None = None,
    ) -> str:
        """Build the resume verification prompt.

        Used when resuming an interrupted task to prompt the agent to verify
        its todo list against the actual state of the codebase.

        For non-code tasks, callers can pass task/report metadata to reassert
        the current output artifact contract.
        """
        prompt = _load_template("resume.txt")

        if resume_context:
            prompt += f"\n\n{resume_context}"

        if report_path is None:
            return prompt

        prompt += (
            "\n\nResume output contract (current run):\n"
            f"- Current task DB id: {task_id if task_id is not None else '?'}\n"
            f"- Current task slug: {task_slug or '(unset)'}\n"
            f"- Required report path for this run: {report_path}\n"
            "- Write output to this exact report path before finishing.\n"
            "- Do not keep writing a prior task's filename from an earlier session.\n"
            "- If you wrote to an old report filename earlier, move/copy the final content to the required report path."
        )
        return prompt

    def pr_description_prompt(
        self, task_prompt: str, commit_log: str, diff_stat: str
    ) -> str:
        """Build the prompt for generating a PR title and description.

        Args:
            task_prompt: The task's prompt text.
            commit_log: Git log output for the branch commits.
            diff_stat: Git diff --stat output showing changed files.

        Returns:
            Prompt string instructing Claude to generate PR title and body.
        """
        return _load_template("pr_description.txt").format(
            task_prompt=task_prompt,
            commit_log=commit_log,
            diff_stat=diff_stat,
        )

    def improve_task_prompt(
        self,
        task_id: str,
        review_id: str | None,
        *,
        has_comments: bool = False,
    ) -> str:
        """Build the prompt for an improve task.

        Args:
            task_id: The ID of the implementation task being improved.
            review_id: The ID of the review task being addressed (if any).
            has_comments: Whether unresolved task comments exist for feedback context.

        Returns:
            Prompt string for an improve task.
        """
        if review_id is not None and has_comments:
            return (
                f"Improve implementation of task {task_id} "
                f"based on review {review_id} and unresolved comments"
            )
        if review_id is not None:
            return f"Improve implementation of task {task_id} based on review {review_id}"
        if has_comments:
            return f"Improve implementation of task {task_id} based on unresolved comments"
        return f"Improve implementation of task {task_id}"

    def fix_task_prompt(self, task_id: str, review_id: str | None = None) -> str:
        """Build the prompt for a stuck-task rescue fix task."""
        if review_id:
            return f"Rescue stuck implementation task {task_id} based on review {review_id}"
        return f"Rescue stuck implementation task {task_id}"

    def review_task_prompt(
        self, impl_task_id: str, impl_prompt: str | None = None
    ) -> str:
        """Build the prompt for a review task.

        Args:
            impl_task_id: The ID of the implementation task being reviewed.
            impl_prompt: Unused legacy parameter retained for call-site compatibility.

        Returns:
            Prompt string for a review task.
        """
        del impl_prompt  # legacy parameter kept for backward compatibility
        prompt = f"Review task {impl_task_id}"
        prompt += (
            ". Review the provided changed-files list, diffstat, and inline diff/context"
            " in the prompt. The provided diff is authoritative - do not use git commands"
            " to reconstruct, re-derive, or expand it. You may read unchanged source files"
            " when surrounding context is needed to judge correctness."
        )
        return prompt
