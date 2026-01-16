"""Task import from YAML files."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .db import SqliteTaskStore, Task


@dataclass
class ImportTask:
    """A task parsed from an import file."""
    prompt: str
    task_type: str = "task"
    group: str | None = None
    depends_on: int | None = None  # Local index (1-based)
    review: bool = False
    spec: str | None = None


@dataclass
class ImportResult:
    """Result of importing a single task."""
    task: Task | None  # None if skipped
    local_index: int
    skipped: bool = False
    skip_reason: str | None = None


@dataclass
class ValidationError:
    """A validation error in the import file."""
    message: str
    task_index: int | None = None  # None for file-level errors


def parse_import_file(file_path: Path) -> tuple[list[ImportTask], str | None, str | None, list[ValidationError]]:
    """Parse an import YAML file.

    Returns:
        (tasks, default_group, default_spec, errors)
    """
    errors: list[ValidationError] = []

    if not file_path.exists():
        errors.append(ValidationError(f"File not found: {file_path}"))
        return [], None, None, errors

    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        errors.append(ValidationError(f"Invalid YAML: {e}"))
        return [], None, None, errors

    if not isinstance(data, dict):
        errors.append(ValidationError("Import file must be a YAML mapping"))
        return [], None, None, errors

    # Extract file-level defaults
    default_group = data.get("group")
    default_spec = data.get("spec")

    # Get tasks list
    tasks_data = data.get("tasks", [])
    if not isinstance(tasks_data, list):
        errors.append(ValidationError("'tasks' must be a list"))
        return [], default_group, default_spec, errors

    if not tasks_data:
        errors.append(ValidationError("No tasks found in file"))
        return [], default_group, default_spec, errors

    tasks: list[ImportTask] = []
    for i, task_data in enumerate(tasks_data, 1):
        task, task_errors = _parse_task(task_data, i, default_group, default_spec)
        errors.extend(task_errors)
        if task:
            tasks.append(task)

    return tasks, default_group, default_spec, errors


def _parse_task(
    data: Any,
    index: int,
    default_group: str | None,
    default_spec: str | None,
) -> tuple[ImportTask | None, list[ValidationError]]:
    """Parse a single task from the import file."""
    errors: list[ValidationError] = []

    if not isinstance(data, dict):
        errors.append(ValidationError(f"Task must be a mapping", task_index=index))
        return None, errors

    # Required: prompt
    prompt = data.get("prompt")
    if not prompt:
        errors.append(ValidationError("Task missing required 'prompt' field", task_index=index))
        return None, errors

    if not isinstance(prompt, str):
        errors.append(ValidationError("'prompt' must be a string", task_index=index))
        return None, errors

    # Optional fields
    task_type = data.get("type", "task")
    if task_type not in ("task", "explore", "plan", "implement", "review"):
        errors.append(ValidationError(
            f"Invalid task type '{task_type}'. Must be: task, explore, plan, implement, review",
            task_index=index
        ))

    # Group: use task-level if present, else default, handle null override
    group = default_group
    if "group" in data:
        group = data["group"]  # Can be None to clear default

    # Spec: same logic as group
    spec = default_spec
    if "spec" in data:
        spec = data["spec"]

    # depends_on: local index (1-based)
    depends_on = data.get("depends_on")
    if depends_on is not None:
        if not isinstance(depends_on, int) or depends_on < 1:
            errors.append(ValidationError(
                f"'depends_on' must be a positive integer (1-based index)",
                task_index=index
            ))
            depends_on = None

    # review flag
    review = data.get("review", False)
    if not isinstance(review, bool):
        errors.append(ValidationError("'review' must be a boolean", task_index=index))
        review = False

    return ImportTask(
        prompt=prompt.strip(),
        task_type=task_type,
        group=group,
        depends_on=depends_on,
        review=review,
        spec=spec,
    ), errors


def validate_import(
    tasks: list[ImportTask],
    project_dir: Path,
    default_spec: str | None,
) -> list[ValidationError]:
    """Validate the parsed tasks.

    Checks:
    - Spec files exist
    - Dependency indices are valid
    - No circular dependencies
    """
    errors: list[ValidationError] = []
    num_tasks = len(tasks)

    # Check default spec exists
    if default_spec:
        spec_path = project_dir / default_spec
        if not spec_path.exists():
            errors.append(ValidationError(f"Spec file not found: {default_spec} (file-level default)"))

    # Check each task
    for i, task in enumerate(tasks, 1):
        # Check spec file exists (if different from default already checked)
        if task.spec and task.spec != default_spec:
            spec_path = project_dir / task.spec
            if not spec_path.exists():
                errors.append(ValidationError(f"Spec file not found: {task.spec}", task_index=i))

        # Check depends_on is valid index
        if task.depends_on is not None:
            if task.depends_on > num_tasks:
                errors.append(ValidationError(
                    f"Invalid depends_on: {task.depends_on} (only {num_tasks} tasks in file)",
                    task_index=i
                ))
            elif task.depends_on == i:
                errors.append(ValidationError(
                    f"Task cannot depend on itself",
                    task_index=i
                ))
            elif task.depends_on > i:
                errors.append(ValidationError(
                    f"Task depends on a later task ({task.depends_on}). Dependencies must reference earlier tasks.",
                    task_index=i
                ))

    # Check for circular dependencies (already prevented by depends_on > i check above,
    # but let's be explicit)
    # Since we only allow depends_on to reference earlier tasks, cycles are impossible

    return errors


def find_duplicate(
    store: SqliteTaskStore,
    prompt: str,
    group: str | None,
) -> Task | None:
    """Find a duplicate pending task by prompt and group."""
    # Normalize prompt for comparison
    prompt_normalized = prompt.strip()

    # Search for pending tasks with same prompt
    pending = store.get_pending()
    for task in pending:
        if task.prompt.strip() == prompt_normalized and task.group == group:
            return task

    return None


def import_tasks(
    store: SqliteTaskStore,
    tasks: list[ImportTask],
    project_dir: Path,
    dry_run: bool = False,
    force: bool = False,
) -> tuple[list[ImportResult], list[str]]:
    """Import tasks into the database.

    Args:
        store: Task store
        tasks: Parsed tasks to import
        project_dir: Project directory for resolving spec paths
        dry_run: If True, don't actually create tasks
        force: If True, skip duplicate detection

    Returns:
        (results, messages) where results contains import outcomes and messages are status lines
    """
    results: list[ImportResult] = []
    messages: list[str] = []

    # Map local index -> actual task ID
    index_to_id: dict[int, int] = {}

    for i, import_task in enumerate(tasks, 1):
        # Check for duplicates
        if not force:
            duplicate = find_duplicate(store, import_task.prompt, import_task.group)
            if duplicate:
                results.append(ImportResult(
                    task=None,
                    local_index=i,
                    skipped=True,
                    skip_reason=f"duplicate of #{duplicate.id}",
                ))
                prompt_preview = import_task.prompt[:40] + "..." if len(import_task.prompt) > 40 else import_task.prompt
                messages.append(f"  - Skipped: {prompt_preview} (duplicate of #{duplicate.id})")
                continue

        # Resolve depends_on from local index to actual ID
        depends_on_id = None
        if import_task.depends_on is not None:
            depends_on_id = index_to_id.get(import_task.depends_on)
            if depends_on_id is None and not dry_run:
                # This shouldn't happen if validation passed, but handle gracefully
                messages.append(f"  ! Warning: Task {i} depends on {import_task.depends_on} which was skipped")

        if dry_run:
            # Don't create, just report what would happen
            prompt_preview = import_task.prompt[:40] + "..." if len(import_task.prompt) > 40 else import_task.prompt
            type_label = f"[{import_task.task_type}] " if import_task.task_type != "task" else ""
            group_label = f" (group: {import_task.group})" if import_task.group else ""
            dep_label = f" (depends on #{import_task.depends_on})" if import_task.depends_on else ""
            review_label = " (review: true)" if import_task.review else ""
            messages.append(f"  {i}. {type_label}{prompt_preview}{group_label}{dep_label}{review_label}")
            results.append(ImportResult(task=None, local_index=i))
        else:
            # Create the task
            task = store.add(
                prompt=import_task.prompt,
                task_type=import_task.task_type,
                group=import_task.group,
                depends_on=depends_on_id,
                spec=import_task.spec,
                create_review=import_task.review,
            )

            # Track mapping for future dependencies
            index_to_id[i] = task.id

            results.append(ImportResult(task=task, local_index=i))
            prompt_preview = import_task.prompt[:40] + "..." if len(import_task.prompt) > 40 else import_task.prompt
            type_label = f", {import_task.task_type}" if import_task.task_type != "task" else ""
            dep_label = f", depends on #{depends_on_id}" if depends_on_id else ""
            messages.append(f"  ✓ Created: {prompt_preview} (#{task.id}{type_label}{dep_label})")

    return results, messages
