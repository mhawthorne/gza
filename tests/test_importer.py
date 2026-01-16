"""Tests for the task importer."""

from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.importer import (
    ImportTask,
    ValidationError,
    parse_import_file,
    validate_import,
    import_tasks,
    find_duplicate,
)


@pytest.fixture
def store(tmp_path: Path) -> SqliteTaskStore:
    """Create a temporary task store."""
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Create a project directory with a spec file."""
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    (spec_dir / "test.md").write_text("# Test Spec\n")
    return tmp_path


class TestParseImportFile:
    """Tests for parse_import_file."""

    def test_parse_simple_tasks(self, tmp_path: Path):
        """Parse a simple import file with tasks."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - prompt: "First task"
  - prompt: "Second task"
""")
        tasks, group, spec, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert len(tasks) == 2
        assert tasks[0].prompt == "First task"
        assert tasks[1].prompt == "Second task"
        assert group is None
        assert spec is None

    def test_parse_with_file_level_defaults(self, tmp_path: Path):
        """Parse file with group and spec defaults."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
group: my-feature
spec: specs/feature.md

tasks:
  - prompt: "Task with defaults"
  - prompt: "Task with override"
    group: other-group
""")
        tasks, group, spec, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert group == "my-feature"
        assert spec == "specs/feature.md"
        assert tasks[0].group == "my-feature"
        assert tasks[0].spec == "specs/feature.md"
        assert tasks[1].group == "other-group"
        assert tasks[1].spec == "specs/feature.md"

    def test_parse_with_null_override(self, tmp_path: Path):
        """Null can override file-level defaults."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
group: default-group

tasks:
  - prompt: "Task with group"
  - prompt: "Task without group"
    group: null
""")
        tasks, group, spec, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert tasks[0].group == "default-group"
        assert tasks[1].group is None

    def test_parse_with_dependencies(self, tmp_path: Path):
        """Parse tasks with depends_on."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - prompt: "First task"
  - prompt: "Second task"
    depends_on: 1
  - prompt: "Third task"
    depends_on: 2
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert tasks[0].depends_on is None
        assert tasks[1].depends_on == 1
        assert tasks[2].depends_on == 2

    def test_parse_with_task_types(self, tmp_path: Path):
        """Parse tasks with different types."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - prompt: "Plan task"
    type: plan
  - prompt: "Implement task"
    type: implement
  - prompt: "Review task"
    type: review
  - prompt: "Default task"
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert tasks[0].task_type == "plan"
        assert tasks[1].task_type == "implement"
        assert tasks[2].task_type == "review"
        assert tasks[3].task_type == "task"

    def test_parse_with_review_flag(self, tmp_path: Path):
        """Parse tasks with review flag."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - prompt: "Task with review"
    review: true
  - prompt: "Task without review"
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 0
        assert tasks[0].review is True
        assert tasks[1].review is False

    def test_parse_missing_file(self, tmp_path: Path):
        """Error when file doesn't exist."""
        import_file = tmp_path / "nonexistent.yaml"
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 1
        assert "not found" in errors[0].message.lower()

    def test_parse_invalid_yaml(self, tmp_path: Path):
        """Error on invalid YAML."""
        import_file = tmp_path / "invalid.yaml"
        import_file.write_text("{ invalid yaml [")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 1
        assert "yaml" in errors[0].message.lower()

    def test_parse_missing_prompt(self, tmp_path: Path):
        """Error when task missing prompt."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - type: plan
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 1
        assert "prompt" in errors[0].message.lower()
        assert errors[0].task_index == 1

    def test_parse_empty_tasks(self, tmp_path: Path):
        """Error when no tasks in file."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
group: my-group
tasks: []
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 1
        assert "no tasks" in errors[0].message.lower()

    def test_parse_invalid_type(self, tmp_path: Path):
        """Error on invalid task type."""
        import_file = tmp_path / "tasks.yaml"
        import_file.write_text("""
tasks:
  - prompt: "Bad task"
    type: invalid_type
""")
        tasks, _, _, errors = parse_import_file(import_file)

        assert len(errors) == 1
        assert "invalid task type" in errors[0].message.lower()


class TestValidateImport:
    """Tests for validate_import."""

    def test_validate_valid_tasks(self, project_dir: Path):
        """Valid tasks pass validation."""
        tasks = [
            ImportTask(prompt="First", spec="specs/test.md"),
            ImportTask(prompt="Second", depends_on=1),
        ]
        errors = validate_import(tasks, project_dir, "specs/test.md")

        assert len(errors) == 0

    def test_validate_missing_spec(self, project_dir: Path):
        """Error when spec file doesn't exist."""
        tasks = [ImportTask(prompt="Task", spec="specs/missing.md")]
        errors = validate_import(tasks, project_dir, None)

        assert len(errors) == 1
        assert "not found" in errors[0].message.lower()
        assert "missing.md" in errors[0].message

    def test_validate_missing_default_spec(self, project_dir: Path):
        """Error when default spec doesn't exist."""
        tasks = [ImportTask(prompt="Task")]
        errors = validate_import(tasks, project_dir, "specs/missing.md")

        assert len(errors) == 1
        assert "file-level default" in errors[0].message.lower()

    def test_validate_invalid_depends_on_index(self, project_dir: Path):
        """Error when depends_on references non-existent task."""
        tasks = [
            ImportTask(prompt="First"),
            ImportTask(prompt="Second", depends_on=5),
        ]
        errors = validate_import(tasks, project_dir, None)

        assert len(errors) == 1
        assert "invalid depends_on" in errors[0].message.lower()

    def test_validate_self_dependency(self, project_dir: Path):
        """Error when task depends on itself."""
        tasks = [
            ImportTask(prompt="First"),
            ImportTask(prompt="Second", depends_on=2),
        ]
        errors = validate_import(tasks, project_dir, None)

        assert len(errors) == 1
        assert "itself" in errors[0].message.lower()

    def test_validate_forward_dependency(self, project_dir: Path):
        """Error when task depends on later task."""
        tasks = [
            ImportTask(prompt="First", depends_on=2),
            ImportTask(prompt="Second"),
        ]
        errors = validate_import(tasks, project_dir, None)

        assert len(errors) == 1
        assert "later task" in errors[0].message.lower()


class TestFindDuplicate:
    """Tests for find_duplicate."""

    def test_find_no_duplicate(self, store: SqliteTaskStore):
        """No duplicate when store is empty."""
        result = find_duplicate(store, "New task", None)
        assert result is None

    def test_find_duplicate_by_prompt(self, store: SqliteTaskStore):
        """Find duplicate by matching prompt."""
        task = store.add("Existing task", group="my-group")

        result = find_duplicate(store, "Existing task", "my-group")
        assert result is not None
        assert result.id == task.id

    def test_no_duplicate_different_group(self, store: SqliteTaskStore):
        """No duplicate when group differs."""
        store.add("Existing task", group="group-a")

        result = find_duplicate(store, "Existing task", "group-b")
        assert result is None

    def test_no_duplicate_completed_task(self, store: SqliteTaskStore):
        """No duplicate when existing task is completed."""
        task = store.add("Existing task", group="my-group")
        task.status = "completed"
        store.update(task)

        result = find_duplicate(store, "Existing task", "my-group")
        assert result is None

    def test_duplicate_ignores_whitespace(self, store: SqliteTaskStore):
        """Duplicate detection normalizes whitespace."""
        store.add("Existing task  ", group="my-group")

        result = find_duplicate(store, "  Existing task", "my-group")
        assert result is not None


class TestImportTasks:
    """Tests for import_tasks."""

    def test_import_simple_tasks(self, store: SqliteTaskStore, project_dir: Path):
        """Import simple tasks without dependencies."""
        tasks = [
            ImportTask(prompt="First task"),
            ImportTask(prompt="Second task"),
        ]

        results, messages = import_tasks(store, tasks, project_dir)

        assert len(results) == 2
        assert all(not r.skipped for r in results)
        assert results[0].task.id is not None
        assert results[1].task.id is not None

        # Verify in database
        all_tasks = store.get_pending()
        assert len(all_tasks) == 2

    def test_import_with_dependencies(self, store: SqliteTaskStore, project_dir: Path):
        """Import tasks with dependency resolution."""
        tasks = [
            ImportTask(prompt="First task"),
            ImportTask(prompt="Second task", depends_on=1),
            ImportTask(prompt="Third task", depends_on=2),
        ]

        results, messages = import_tasks(store, tasks, project_dir)

        # Check dependency IDs are resolved
        task1_id = results[0].task.id
        task2_id = results[1].task.id

        db_task2 = store.get(task2_id)
        db_task3 = store.get(results[2].task.id)

        assert db_task2.depends_on == task1_id
        assert db_task3.depends_on == task2_id

    def test_import_with_group(self, store: SqliteTaskStore, project_dir: Path):
        """Import tasks with group."""
        tasks = [
            ImportTask(prompt="Task", group="my-feature"),
        ]

        results, _ = import_tasks(store, tasks, project_dir)

        db_task = store.get(results[0].task.id)
        assert db_task.group == "my-feature"

    def test_import_dry_run(self, store: SqliteTaskStore, project_dir: Path):
        """Dry run doesn't create tasks."""
        tasks = [
            ImportTask(prompt="First task"),
            ImportTask(prompt="Second task"),
        ]

        results, messages = import_tasks(store, tasks, project_dir, dry_run=True)

        assert len(results) == 2
        assert all(r.task is None for r in results)

        # Verify nothing in database
        all_tasks = store.get_pending()
        assert len(all_tasks) == 0

    def test_import_skip_duplicates(self, store: SqliteTaskStore, project_dir: Path):
        """Skip duplicate tasks by default."""
        # Create existing task
        store.add("Existing task", group="my-group")

        tasks = [
            ImportTask(prompt="Existing task", group="my-group"),
            ImportTask(prompt="New task", group="my-group"),
        ]

        results, messages = import_tasks(store, tasks, project_dir)

        assert results[0].skipped is True
        assert results[1].skipped is False

        # Only one new task created
        all_tasks = store.get_pending()
        assert len(all_tasks) == 2  # 1 existing + 1 new

    def test_import_force_duplicates(self, store: SqliteTaskStore, project_dir: Path):
        """Force flag creates duplicates."""
        store.add("Existing task", group="my-group")

        tasks = [
            ImportTask(prompt="Existing task", group="my-group"),
        ]

        results, messages = import_tasks(store, tasks, project_dir, force=True)

        assert results[0].skipped is False

        # Duplicate created
        all_tasks = store.get_pending()
        assert len(all_tasks) == 2

    def test_import_with_all_fields(self, store: SqliteTaskStore, project_dir: Path):
        """Import preserves all task fields."""
        (project_dir / "specs").mkdir(exist_ok=True)
        (project_dir / "specs" / "test.md").write_text("# Test")

        tasks = [
            ImportTask(
                prompt="Full task",
                task_type="implement",
                group="my-feature",
                spec="specs/test.md",
                review=True,
            ),
        ]

        results, _ = import_tasks(store, tasks, project_dir)

        db_task = store.get(results[0].task.id)
        assert db_task.prompt == "Full task"
        assert db_task.task_type == "implement"
        assert db_task.group == "my-feature"
        assert db_task.spec == "specs/test.md"
        assert db_task.create_review is True


class TestGetNextPendingWithDependencies:
    """Tests for get_next_pending respecting dependencies."""

    def test_skips_blocked_task(self, store: SqliteTaskStore):
        """get_next_pending skips tasks with incomplete dependencies."""
        task1 = store.add("First task")
        task2 = store.add("Second task", depends_on=task1.id)

        # Task 1 is pending, task 2 depends on it
        next_task = store.get_next_pending()
        assert next_task.id == task1.id

    def test_returns_unblocked_after_completion(self, store: SqliteTaskStore):
        """get_next_pending returns task after dependency completes."""
        task1 = store.add("First task")
        task2 = store.add("Second task", depends_on=task1.id)

        # Complete task 1
        task1.status = "completed"
        store.update(task1)

        # Now task 2 should be next
        next_task = store.get_next_pending()
        assert next_task.id == task2.id

    def test_returns_none_when_all_blocked(self, store: SqliteTaskStore):
        """get_next_pending returns None when all tasks are blocked."""
        task1 = store.add("First task")
        task1.status = "in_progress"
        store.update(task1)

        task2 = store.add("Second task", depends_on=task1.id)

        # Task 1 is in_progress (not completed), task 2 is blocked
        next_task = store.get_next_pending()
        assert next_task is None
