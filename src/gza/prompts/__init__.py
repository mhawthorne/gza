"""Prompt building module for gza tasks.

Centralizes all prompt strings into template files and provides a clean API
for assembling prompts by task type.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gza.config import Config
    from gza.db import SqliteTaskStore, Task
    from gza.git import Git

_TEMPLATE_DIR = Path(__file__).parent / "templates"


def _load_template(name: str) -> str:
    """Load a prompt template file by name."""
    return (_TEMPLATE_DIR / name).read_text()


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

        # Add context from based_on chain (walk up the chain to find plan tasks)
        if task.based_on or task.task_type in ("implement", "review"):
            from gza.runner import _build_context_from_chain
            context = _build_context_from_chain(task, store, config.project_dir, git)
            if context:
                base_prompt += "\n\n" + context

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
        elif task.task_type in ("task", "implement", "improve"):
            if summary_path:
                base_prompt += _load_template("task_with_summary.txt").format(
                    summary_path=summary_path
                )
            else:
                base_prompt += _load_template("task_without_summary.txt")
        else:
            base_prompt += "\n\nWhen you are done, report what you accomplished."

        return base_prompt

    def resume_prompt(self) -> str:
        """Build the resume verification prompt.

        Used when resuming an interrupted task to prompt the agent to verify
        its todo list against the actual state of the codebase.
        """
        return _load_template("resume.txt")

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

    def improve_task_prompt(self, review_id: int) -> str:
        """Build the prompt for an improve task.

        Args:
            review_id: The ID of the review task being addressed.

        Returns:
            Prompt string for an improve task.
        """
        return f"Improve implementation based on review #{review_id}"

    def review_task_prompt(
        self, impl_task_id: int, impl_prompt: str | None = None
    ) -> str:
        """Build the prompt for a review task.

        Args:
            impl_task_id: The ID of the implementation task being reviewed.
            impl_prompt: Optional prompt text of the implementation task for context.

        Returns:
            Prompt string for a review task.
        """
        prompt = f"Review the implementation from task #{impl_task_id}"
        if impl_prompt:
            prompt += f": {impl_prompt[:100]}"
        prompt += (
            ". The diff shows what changed, but you should use Read/Glob/Grep tools"
            " to understand the surrounding context of each changed file."
            " Read the full content of modified files to understand how changes fit"
            " into the broader codebase."
        )
        return prompt
