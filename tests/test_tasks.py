"""Tests for task model and YAML serialization."""

import tempfile
from pathlib import Path

import yaml

from gza.tasks import Task, YamlTaskStore, LiteralString


class TestTaskSerialization:
    """Tests for Task serialization to YAML."""

    def test_short_description_uses_regular_string(self):
        """Short descriptions (<= 50 chars) use regular YAML strings."""
        task = Task(id=None, prompt="Short task")
        task_dict = task.to_dict()

        # Should not be a LiteralString
        assert not isinstance(task_dict["description"], LiteralString)
        assert task_dict["description"] == "Short task"

    def test_long_description_uses_literal_string(self):
        """Long descriptions (> 50 chars) use literal block scalar syntax."""
        long_desc = "This is a very long task description that exceeds the fifty character limit"
        task = Task(id=None, prompt=long_desc)
        task_dict = task.to_dict()

        # Should be a LiteralString
        assert isinstance(task_dict["description"], LiteralString)
        assert str(task_dict["description"]) == long_desc

    def test_multiline_description_uses_literal_string(self):
        """Multi-line descriptions use literal block scalar syntax."""
        multiline_desc = "First line\nSecond line\nThird line"
        task = Task(id=None, prompt=multiline_desc)
        task_dict = task.to_dict()

        # Should be a LiteralString
        assert isinstance(task_dict["description"], LiteralString)
        assert str(task_dict["description"]) == multiline_desc

    def test_yaml_output_format_short(self):
        """Short descriptions are serialized as regular YAML strings."""
        task = Task(id=None, prompt="Short task", status="pending")
        task_dict = task.to_dict()
        yaml_output = yaml.dump({"task": task_dict}, default_flow_style=False)

        # Should not have literal block scalar indicator
        assert "|-" not in yaml_output
        assert "description: Short task" in yaml_output

    def test_yaml_output_format_long(self):
        """Long descriptions are serialized with literal block scalar syntax."""
        long_desc = "This is a very long task description that exceeds the fifty character limit"
        task = Task(id=None, prompt=long_desc, status="pending")
        task_dict = task.to_dict()
        yaml_output = yaml.dump({"task": task_dict}, default_flow_style=False)

        # Should have literal block scalar indicator
        assert "|-" in yaml_output
        assert long_desc in yaml_output

    def test_yaml_output_format_multiline(self):
        """Multi-line descriptions are serialized with literal block scalar syntax."""
        multiline_desc = "First line\nSecond line\nThird line"
        task = Task(id=None, prompt=multiline_desc, status="pending")
        task_dict = task.to_dict()
        yaml_output = yaml.dump({"task": task_dict}, default_flow_style=False)

        # Should have literal block scalar indicator
        assert "|-" in yaml_output
        # Each line should be preserved
        assert "First line" in yaml_output
        assert "Second line" in yaml_output
        assert "Third line" in yaml_output


class TestYamlTaskStore:
    """Tests for YamlTaskStore save/load functionality."""

    def test_save_and_load_preserves_short_descriptions(self):
        """Short task descriptions are preserved through save/load cycle."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_file = Path(f.name)

        try:
            store = YamlTaskStore(temp_file)
            store._tasks = [Task(id=None, prompt="Short task")]
            store._save()

            # Load it back
            store2 = YamlTaskStore(temp_file)
            assert len(store2._tasks) == 1
            assert store2._tasks[0].prompt == "Short task"
        finally:
            temp_file.unlink()

    def test_save_and_load_preserves_long_descriptions(self):
        """Long task descriptions are preserved through save/load cycle."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_file = Path(f.name)

        try:
            long_desc = "This is a very long task description that exceeds the fifty character limit and should be formatted properly"
            store = YamlTaskStore(temp_file)
            store._tasks = [Task(id=None, prompt=long_desc)]
            store._save()

            # Load it back
            store2 = YamlTaskStore(temp_file)
            assert len(store2._tasks) == 1
            assert store2._tasks[0].prompt == long_desc
        finally:
            temp_file.unlink()

    def test_save_and_load_preserves_multiline_descriptions(self):
        """Multi-line task descriptions are preserved through save/load cycle."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_file = Path(f.name)

        try:
            multiline_desc = "First line\n\nSecond paragraph\nThird line"
            store = YamlTaskStore(temp_file)
            store._tasks = [Task(id=None, prompt=multiline_desc)]
            store._save()

            # Load it back
            store2 = YamlTaskStore(temp_file)
            assert len(store2._tasks) == 1
            assert store2._tasks[0].prompt == multiline_desc
        finally:
            temp_file.unlink()

    def test_save_uses_literal_block_for_long_tasks(self):
        """Saved YAML uses literal block scalar syntax for long descriptions."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_file = Path(f.name)

        try:
            long_desc = "This is a very long task description that exceeds the fifty character limit"
            store = YamlTaskStore(temp_file)
            store._tasks = [Task(id=None, prompt=long_desc)]
            store._save()

            # Read raw YAML content
            yaml_content = temp_file.read_text()

            # Should use literal block scalar syntax
            assert "|-" in yaml_content
            assert long_desc in yaml_content
        finally:
            temp_file.unlink()

    def test_save_uses_literal_block_for_multiline_tasks(self):
        """Saved YAML uses literal block scalar syntax for multi-line descriptions."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_file = Path(f.name)

        try:
            multiline_desc = "Line 1\nLine 2\nLine 3"
            store = YamlTaskStore(temp_file)
            store._tasks = [Task(id=None, prompt=multiline_desc)]
            store._save()

            # Read raw YAML content
            yaml_content = temp_file.read_text()

            # Should use literal block scalar syntax
            assert "|-" in yaml_content
            # Each line should be on its own line in the YAML
            assert "Line 1" in yaml_content
            assert "Line 2" in yaml_content
            assert "Line 3" in yaml_content
        finally:
            temp_file.unlink()
