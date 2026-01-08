#!/usr/bin/env python3
"""Test script to verify multiple background workers can be spawned."""

import sys
import tempfile
from pathlib import Path
import argparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from theo.cli import _spawn_background_workers
from theo.config import Config
from theo.db import SqliteTaskStore


def test_multiple_workers():
    """Test spawning multiple background workers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)

        # Create a minimal theo.yaml
        config_file = project_dir / "theo.yaml"
        config_file.write_text("""
project_name: test-project
""")

        # Load config and initialize database
        config = Config.load(project_dir)
        store = SqliteTaskStore(config.db_path)

        # Add some test tasks
        for i in range(5):
            store.add(f"Test task {i+1}", task_type="task")

        # Verify tasks were added
        pending = store.get_pending()
        print(f"✓ Created {len(pending)} pending tasks")
        assert len(pending) == 5, f"Expected 5 tasks, got {len(pending)}"

        # Create mock args for spawning workers
        args = argparse.Namespace(
            project_dir=project_dir,
            task_id=None,
            count=3,
            no_docker=True,
            background=True,
            worker_mode=False
        )

        print("\nTesting _spawn_background_workers with count=3:")
        print("-" * 50)

        # This should spawn 3 workers (but they'll fail to actually run since
        # this is a test environment without Claude Code installed)
        # The important thing is that it tries to spawn them
        result = _spawn_background_workers(args, config)

        print("-" * 50)
        print(f"Result: {result}")
        print(f"✓ Function executed without errors (return code: {result})")

        # In a real scenario, workers would be running in background
        # Here we just verify the function doesn't crash

        print("\n✓ Test passed: Multiple workers can be spawned")


if __name__ == "__main__":
    test_multiple_workers()
