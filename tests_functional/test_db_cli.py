"""Subprocess CLI regression tests for DB import and migration flows."""

import os
import sqlite3
from pathlib import Path

from gza.db import SqliteTaskStore
from tests.helpers.cli import run_gza_subprocess
from tests.test_db import _make_v24_db, _make_v35_db_with_legacy_key_shapes


class TestSharedDbImportCli:
    def test_local_default_with_legacy_local_db_does_not_require_import(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text("project_name: gated\n", encoding="utf-8")

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="gza")
        legacy_task = legacy_store.add("legacy pending")

        result = run_gza_subprocess("next", "--project", str(project_dir), cwd=project_dir)
        assert result.returncode == 0, result.stderr
        assert legacy_task.id in result.stdout
        assert "Legacy local DB detected" not in result.stderr

    def test_shared_opt_in_with_legacy_local_db_requires_explicit_import(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: gated\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="gza")
        legacy_store.add("legacy pending")

        home_dir = tmp_path / "home"
        home_dir.mkdir(parents=True, exist_ok=True)
        result = run_gza_subprocess(
            "next",
            "--project",
            str(project_dir),
            cwd=project_dir,
            env={**os.environ, "HOME": str(home_dir)},
        )
        assert result.returncode == 1
        assert "Legacy local DB detected" in result.stderr
        assert "--import-local-db" in result.stderr

    def test_import_local_db_is_idempotent_and_conflicts_fail_loudly(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: gated\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="gated")
        legacy_task = legacy_store.add("legacy pending")
        assert legacy_task.id is not None

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr
        assert "Imported legacy local DB into shared DB." in first.stdout

        second = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert second.returncode == 0, second.stderr
        assert "already imported" in second.stdout.lower()

        config = Config.load(project_dir)
        shared_store = SqliteTaskStore.from_config(config)
        shared_tasks = shared_store.get_all()
        assert len(shared_tasks) == 1
        assert shared_tasks[0].id == legacy_task.id

        conn = sqlite3.connect(local_db)
        conn.execute("UPDATE tasks SET prompt = ? WHERE id = ?", ("conflicting prompt", legacy_task.id))
        conn.commit()
        conn.close()

        conflict = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert conflict.returncode == 1
        assert "Conflicting task IDs already exist" in conflict.stderr

    def test_import_local_db_conflicts_on_non_key_field_drift(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: gated\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="gated")
        legacy_task = legacy_store.add("legacy pending")
        assert legacy_task.id is not None

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr

        conn = sqlite3.connect(local_db)
        conn.execute("UPDATE tasks SET merge_status = ? WHERE id = ?", ("merged", legacy_task.id))
        conn.commit()
        conn.close()

        conflict = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert conflict.returncode == 1
        assert "Conflicting task IDs already exist" in conflict.stderr

    def test_import_local_db_conflicts_on_run_steps_payload_drift(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportstep01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )
        marker_path = project_dir / ".gza" / "shared-db-import.json"

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        task = legacy_store.add("legacy task")
        legacy_store.emit_step(task.id, "local message", provider="codex")

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr
        assert marker_path.exists()

        with sqlite3.connect(local_db) as conn:
            conn.execute(
                """
                UPDATE run_steps
                SET message_text = ?
                WHERE run_id = ? AND step_index = ?
                """,
                ("conflicting local message", task.id, 1),
            )
        marker_path.unlink()
        assert not marker_path.exists()

        conflict = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert conflict.returncode == 1
        assert "Conflicting run_steps rows already exist" in conflict.stderr
        assert not marker_path.exists()

        config = Config.load(project_dir)
        shared_store = SqliteTaskStore(shared_db, prefix=config.project_prefix, project_id=config.project_id)
        imported_steps = shared_store.get_run_steps(task.id)
        assert len(imported_steps) == 1
        assert imported_steps[0].message_text == "local message"

    def test_import_local_db_conflicts_on_run_substeps_payload_drift(self, tmp_path: Path) -> None:
        from gza.config import Config
        from gza.db import StepRef

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportsubstep01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )
        marker_path = project_dir / ".gza" / "shared-db-import.json"

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        task = legacy_store.add("legacy task")
        step = legacy_store.emit_step(task.id, "local message", provider="codex")
        legacy_store.emit_substep(step, "tool_call", {"ok": True}, source="assistant")

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr
        assert marker_path.exists()

        with sqlite3.connect(local_db) as conn:
            conn.execute(
                """
                UPDATE run_substeps
                SET payload_json = ?
                WHERE run_id = ? AND substep_index = ?
                """,
                ('{"ok": false}', task.id, 1),
            )
        marker_path.unlink()
        assert not marker_path.exists()

        conflict = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert conflict.returncode == 1
        assert "Conflicting run_substeps rows already exist" in conflict.stderr
        assert not marker_path.exists()

        config = Config.load(project_dir)
        shared_store = SqliteTaskStore(shared_db, prefix=config.project_prefix, project_id=config.project_id)
        imported_steps = shared_store.get_run_steps(task.id)
        assert len(imported_steps) == 1
        imported_substeps = shared_store.get_run_substeps(
            StepRef(
                id=imported_steps[0].id,
                run_id=imported_steps[0].run_id,
                step_index=imported_steps[0].step_index,
                step_id=imported_steps[0].step_id,
            )
        )
        assert len(imported_substeps) == 1
        assert imported_substeps[0].payload == {"ok": True}

    def test_import_local_db_stays_idempotent_when_marker_metadata_drifts(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportmarker01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr
        assert "Imported legacy local DB into shared DB." in first.stdout

        file_stat = local_db.stat()
        os.utime(local_db, ns=(file_stat.st_atime_ns, file_stat.st_mtime_ns + 1_000_000_000))

        second = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert second.returncode == 0, second.stderr
        assert "already imported" in second.stdout.lower()

    def test_import_local_db_dry_run_does_not_create_missing_shared_db(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared-missing" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportdryrun01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        assert not shared_db.exists()
        assert not shared_db.parent.exists()

        result = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--dry-run",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert result.returncode == 0, result.stderr
        assert "Dry-run: legacy local DB import preview" in result.stdout
        assert not shared_db.exists()
        assert not shared_db.parent.exists()

    def test_import_local_db_dry_run_does_not_mutate_existing_projects_rows(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportdryrun02\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        seeded = SqliteTaskStore(shared_db, prefix="other", project_id="otherproject01")
        seeded.add("seed task")

        with sqlite3.connect(shared_db) as conn:
            conn.row_factory = sqlite3.Row
            before_count = int(conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0])
            before_other_last_seen = conn.execute(
                "SELECT last_seen_at FROM projects WHERE id = ?",
                ("otherproject01",),
            ).fetchone()["last_seen_at"]

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--dry-run",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert result.returncode == 0, result.stderr
        assert "Dry-run: legacy local DB import preview" in result.stdout

        with sqlite3.connect(shared_db) as conn:
            conn.row_factory = sqlite3.Row
            after_count = int(conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0])
            after_other_last_seen = conn.execute(
                "SELECT last_seen_at FROM projects WHERE id = ?",
                ("otherproject01",),
            ).fetchone()["last_seen_at"]
            target_project_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM projects WHERE id = ?",
                    ("demoimportdryrun02",),
                ).fetchone()[0]
            )

        assert after_count == before_count
        assert after_other_last_seen == before_other_last_seen
        assert target_project_count == 0

    def test_import_local_db_dry_run_errors_cleanly_when_shared_db_uninitialized(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        shared_db.parent.mkdir(parents=True, exist_ok=True)
        shared_db.touch()
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportdryrun03\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--dry-run",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert result.returncode == 1
        assert "Error: Shared DB at" in result.stderr
        assert "not initialized or readable" in result.stderr
        assert "Traceback" not in result.stderr

    def test_missing_project_id_is_persisted_once_via_migration_flow(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        config_path = project_dir / "gza.yaml"
        config_path.write_text(
            "project_name: demo\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert first.returncode == 0, first.stderr
        text_after_first = config_path.read_text(encoding="utf-8")
        project_id_lines = [line for line in text_after_first.splitlines() if line.startswith("project_id:")]
        assert len(project_id_lines) == 1
        persisted_project_id = project_id_lines[0].split(":", 1)[1].strip()
        assert persisted_project_id

        second = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert second.returncode == 0, second.stderr
        text_after_second = config_path.read_text(encoding="utf-8")
        assert [line for line in text_after_second.splitlines() if line.startswith("project_id:")] == project_id_lines

        moved_dir = tmp_path / "project-moved"
        project_dir.rename(moved_dir)
        moved_config = Config.load(moved_dir)
        assert moved_config.project_id == persisted_project_id

    def test_import_local_db_cancel_does_not_persist_project_id(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        config_path = project_dir / "gza.yaml"
        original_config = (
            "project_name: demo\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n"
        )
        config_path.write_text(original_config, encoding="utf-8")

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        result = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--project",
            str(project_dir),
            cwd=project_dir,
            stdin_input="n\n",
        )
        assert result.returncode == 1
        assert "Import cancelled." in result.stdout
        assert "Imported legacy local DB into shared DB." not in (result.stdout + result.stderr)
        assert config_path.read_text(encoding="utf-8") == original_config

    def test_import_local_db_confirmed_yes_persists_project_id_once(self, tmp_path: Path) -> None:
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        config_path = project_dir / "gza.yaml"
        config_path.write_text(
            "project_name: demo\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        first = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--project",
            str(project_dir),
            cwd=project_dir,
            stdin_input="y\n",
        )
        assert first.returncode == 0, first.stderr
        assert "Persisted project_id" in first.stdout
        assert "Imported legacy local DB into shared DB." in first.stdout

        second = run_gza_subprocess(
            "migrate",
            "--import-local-db",
            "--yes",
            "--project",
            str(project_dir),
            cwd=project_dir,
        )
        assert second.returncode == 0, second.stderr
        lines = [line for line in config_path.read_text(encoding="utf-8").splitlines() if line.startswith("project_id:")]
        assert len(lines) == 1

    def test_cli_next_on_read_only_v35_db_surfaces_controlled_error(self, tmp_path: Path) -> None:
        """CLI read commands against read-only v35 DB should fail without traceback."""
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "db_path: .gza/gza.db\n",
            encoding="utf-8",
        )
        db_path = project_dir / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _make_v35_db_with_legacy_key_shapes(db_path)

        db_path.chmod(0o444)
        try:
            result = run_gza_subprocess("next", "--project", str(project_dir), cwd=project_dir)
        finally:
            db_path.chmod(0o644)

        assert result.returncode == 1
        assert "query-only mode does not run automatic migrations" in result.stderr
        assert "Traceback" not in result.stderr
        assert "Traceback" not in result.stdout

    def test_cli_show_on_manual_migration_db_surfaces_manual_migration_error(self, tmp_path: Path) -> None:
        """Query-only CLI paths must still surface manual migration requirements explicitly."""
        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "db_path: .gza/gza.db\n",
            encoding="utf-8",
        )
        db_path = project_dir / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _make_v24_db(db_path)

        result = run_gza_subprocess("show", "demo-1", "--project", str(project_dir), cwd=project_dir)

        assert result.returncode == 1
        assert "Database requires manual migration(s): v25" in result.stderr
        assert "Run 'gza migrate' to upgrade the database." in result.stderr
        assert "Traceback" not in result.stderr
