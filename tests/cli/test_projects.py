"""CLI coverage for explicit project registry repair commands."""

import sqlite3
import shutil
import shlex
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from gza.config import Config
from gza.db import SCHEMA_VERSION, SchemaIntegrityError, SqliteTaskStore
from gza.db import _QUERY_ONLY_REQUIRED_TASK_COLUMNS
from gza.cli import config_cmds

from .conftest import invoke_gza


def _write_project_config(
    project_dir: Path,
    *,
    project_name: str,
    project_id: str,
    db_path: Path,
    project_prefix: str = "gza",
) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    (project_dir / "gza.yaml").write_text(
        "\n".join(
            [
                f"project_name: {project_name}",
                f"project_id: {project_id}",
                f"project_prefix: {project_prefix}",
                f"db_path: {db_path}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _registry_row(db_path: Path, project_id: str) -> tuple[str, str] | None:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            "SELECT root_path, config_path FROM projects WHERE id = ?",
            (project_id,),
        ).fetchone()


def _db_snapshot(db_path: Path) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    snapshot: dict[str, Any] = {}
    with sqlite3.connect(db_path) as conn:
        tables = [
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        ]
        for table in tables:
            rows = conn.execute(f'SELECT * FROM "{table}"').fetchall()
            snapshot[table] = sorted(tuple(row) for row in rows)
    return snapshot


def _journal_mode(db_path: Path) -> str:
    with sqlite3.connect(db_path) as conn:
        return str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()


def _sidecar_snapshot(db_path: Path) -> dict[str, int]:
    sidecars: dict[str, int] = {}
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{db_path}{suffix}")
        if sidecar.exists():
            sidecars[suffix] = sidecar.stat().st_size
    return sidecars


def _unlink_sidecars(db_path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        Path(f"{db_path}{suffix}").unlink(missing_ok=True)


def _assert_db_and_sidecars_absent(db_path: Path) -> None:
    assert not db_path.exists()
    assert _sidecar_snapshot(db_path) == {}


def _assert_no_traceback(result: Any) -> None:
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr


def _install_replacement_db(db_path: Path, *, original_db: Path, replacement_db: Path) -> None:
    db_path.rename(original_db)
    _unlink_sidecars(db_path)
    shutil.copy2(replacement_db, db_path)


def _prepare_replacement_db_for_pragma_guard(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA journal_mode=DELETE")
    _unlink_sidecars(db_path)


def _write_future_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION + 1,))


def _write_current_registry_db(db_path: Path, *, project_id: str) -> None:
    project_dir = db_path.parent / f"{project_id}-project"
    _write_project_config(project_dir, project_name=project_id.title(), project_id=project_id, db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))


def _changed_tables(before: dict[str, Any], after: dict[str, Any]) -> set[str]:
    return {table for table in set(before) | set(after) if before.get(table) != after.get(table)}


def _insert_registry_row(
    db_path: Path,
    *,
    project_id: str,
    root_path: str,
    config_path: str,
    project_name: str = "Alias",
    project_prefix: str = "gza",
) -> None:
    now = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO projects (
                id, root_path, config_path, project_name, project_prefix,
                db_layout_version, created_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (project_id, root_path, config_path, project_name, project_prefix, 1, now, now),
        )
        conn.commit()


def _write_auto_migration_pending_registry_db(
    db_path: Path,
    *,
    project_id: str,
    root_path: str = "",
    config_path: str = "",
    alias_project_id: str | None = None,
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC).isoformat()
    task_columns_sql = ", ".join(f'"{column}" TEXT' for column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS)
    task_values = {column: None for column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS}
    task_values.update(
        {
            "project_id": "",
            "id": "gza-1",
            "prompt": "legacy task",
            "status": "pending",
            "task_type": "implement",
            "created_at": now,
            "group": "legacy",
        }
    )
    quoted_task_columns = ", ".join(f'"{column}"' for column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS)
    task_placeholders = ", ".join("?" for _column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION - 1,))
        conn.execute(
            """
            CREATE TABLE projects (
                id TEXT PRIMARY KEY,
                root_path TEXT NOT NULL,
                config_path TEXT NOT NULL,
                project_name TEXT NOT NULL,
                project_prefix TEXT NOT NULL,
                db_layout_version INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO projects (
                id, root_path, config_path, project_name, project_prefix,
                db_layout_version, created_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (project_id, root_path, config_path, "Project", "gza", SCHEMA_VERSION - 1, now, now),
        )
        if alias_project_id is not None:
            conn.execute(
                """
                INSERT INTO projects (
                    id, root_path, config_path, project_name, project_prefix,
                    db_layout_version, created_at, last_seen_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (alias_project_id, root_path, config_path, "Alias", "gza", SCHEMA_VERSION - 1, now, now),
            )
        conn.execute(f"CREATE TABLE tasks ({task_columns_sql})")
        conn.execute(
            f"INSERT INTO tasks ({quoted_task_columns}) VALUES ({task_placeholders})",
            tuple(task_values[column] for column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS),
        )
        conn.execute("CREATE TABLE task_tags (project_id TEXT, task_id TEXT, tag TEXT)")
        conn.execute("INSERT INTO task_tags(project_id, task_id, tag) VALUES ('', 'gza-1', 'legacy')")


def test_projects_register_records_first_project_paths(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 0
    assert "Project project" in result.stdout
    assert _registry_row(db_path, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_refuses_relocation_without_replace(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    _write_project_config(old_dir, project_name="Moved", project_id="moved", db_path=db_path)
    _write_project_config(new_dir, project_name="Moved", project_id="moved", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(old_dir))

    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(new_dir))

    assert result.returncode == 1
    assert "Use --replace" in result.stdout
    assert _db_snapshot(db_path) == before
    assert _registry_row(db_path, "moved") == (
        str(old_dir.resolve()),
        str((old_dir / "gza.yaml").resolve()),
    )


def test_projects_register_replaces_relocated_project_with_explicit_flag(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    _write_project_config(old_dir, project_name="Moved", project_id="moved", db_path=db_path)
    _write_project_config(new_dir, project_name="Moved", project_id="moved", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(old_dir))

    before = _db_snapshot(db_path)
    assert before is not None

    result = invoke_gza("projects", "register", "--project", str(new_dir), "--replace")

    assert result.returncode == 0
    after = _db_snapshot(db_path)
    assert after is not None
    assert _changed_tables(before, after) == {"projects"}
    assert _registry_row(db_path, "moved") == (
        str(new_dir.resolve()),
        str((new_dir / "gza.yaml").resolve()),
    )


def test_projects_diagnose_reports_invalid_and_duplicate_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    _insert_registry_row(db_path, project_id="emptyroot", root_path="", config_path="")
    _insert_registry_row(
        db_path,
        project_id="missingroot",
        root_path=str(tmp_path / "missing"),
        config_path=str(tmp_path / "missing" / "gza.yaml"),
    )
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(anchor_dir.resolve()),
        config_path=str((anchor_dir / "gza.yaml").resolve()),
    )

    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "diagnose", "--project", str(anchor_dir))

    assert result.returncode == 0
    assert _db_snapshot(db_path) == before
    assert "emptyroot: empty_root_path" in result.stdout
    assert "missingroot: missing_root" in result.stdout
    assert "alias: " in result.stdout
    assert "duplicate" in result.stdout


def test_projects_diagnose_preserves_alias_mismatch_while_reporting_duplicate_path(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(anchor_dir.resolve()),
        config_path=str((anchor_dir / "gza.yaml").resolve()),
    )

    result = invoke_gza("projects", "diagnose", "--project", str(anchor_dir))

    assert result.returncode == 0
    assert "anchor: ok" in result.stdout
    assert "alias: project_id_mismatch" in result.stdout
    assert "duplicate registry path pair also appears on another row" in result.stdout


def test_projects_diagnose_preserves_distinct_row_mismatches_while_reporting_duplicate_path(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="real", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM projects WHERE id = 'real'")
    for project_id in ("deadone", "deadtwo"):
        _insert_registry_row(
            db_path,
            project_id=project_id,
            root_path=str(project_dir.resolve()),
            config_path=str((project_dir / "gza.yaml").resolve()),
        )

    result = invoke_gza("projects", "diagnose", "--project", str(project_dir))

    assert result.returncode == 0
    assert "deadone: project_id_mismatch" in result.stdout
    assert "deadtwo: project_id_mismatch" in result.stdout
    assert result.stdout.count("duplicate registry path pair also appears on another row") == 2


def test_projects_deactivate_blanks_named_alias_without_deleting_row(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(anchor_dir.resolve()),
        config_path=str((anchor_dir / "gza.yaml").resolve()),
    )

    before = _db_snapshot(db_path)
    assert before is not None

    result = invoke_gza("projects", "deactivate", "alias", "--project", str(anchor_dir))

    assert result.returncode == 0
    after = _db_snapshot(db_path)
    assert after is not None
    assert _changed_tables(before, after) == {"projects"}
    assert _registry_row(db_path, "alias") == ("", "")


def test_projects_register_refuses_auto_migration_pending_db_without_backfill(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    _write_auto_migration_pending_registry_db(db_path, project_id="project")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Registry mutation requires database schema" in result.stdout
    assert "uv run gza migrate" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_deactivate_refuses_auto_migration_pending_db_without_backfill(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    _write_auto_migration_pending_registry_db(
        db_path,
        project_id="project",
        alias_project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Registry mutation requires database schema" in result.stdout
    assert "uv run gza migrate" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_diagnose_absent_db_is_non_destructive(tmp_path: Path) -> None:
    db_path = tmp_path / "missing" / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)

    result = invoke_gza("projects", "diagnose", "--project", str(project_dir))

    assert result.returncode == 0
    assert "No project registry rows" in result.stdout
    assert not db_path.exists()


def test_projects_diagnose_existing_db_deleted_after_query_store_fails_cleanly_without_recreating(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    original = SqliteTaskStore.list_project_registry_entries

    def delete_before_listing(self: SqliteTaskStore) -> Any:
        db_path.unlink()
        return original(self)

    with patch.object(SqliteTaskStore, "list_project_registry_entries", delete_before_listing):
        result = invoke_gza("projects", "diagnose", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Error:" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr
    assert not db_path.exists()


def test_projects_deactivate_unknown_row_does_not_initialize_absent_db(tmp_path: Path) -> None:
    db_path = tmp_path / "missing" / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)

    result = invoke_gza("projects", "deactivate", "unknown", "--project", str(project_dir))

    assert result.returncode == 1
    assert "project registry row not found" in result.stdout
    assert not db_path.exists()


def test_projects_deactivate_unknown_row_preserves_existing_db(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "deactivate", "unknown", "--project", str(project_dir))

    assert result.returncode == 1
    assert "project registry row not found" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_deactivate_current_without_force_preserves_empty_current_row(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE projects SET root_path = '', config_path = '' WHERE id = 'project'")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "deactivate", "project", "--project", str(project_dir))

    assert result.returncode == 1
    assert "currently selected project" in result.stdout
    assert "uv run gza projects diagnose" in result.stdout
    assert f"uv run gza projects register --project {shlex.quote(str(project_dir.resolve()))} --replace" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_register_default_refuses_config_drift_after_precheck(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    before = _db_snapshot(db_path)
    original = config_cmds._precheck_register_conflict

    def drift_after_precheck(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        _write_project_config(
            project_dir,
            project_name="Project",
            project_id="project",
            project_prefix="other",
            db_path=db_path,
        )

    with patch.object(config_cmds, "_precheck_register_conflict", side_effect=drift_after_precheck):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "project_prefix changed" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_register_path_refuses_target_db_redirect_after_precheck(tmp_path: Path) -> None:
    anchor_db = tmp_path / "shared.db"
    redirected_db = tmp_path / "redirected.db"
    anchor_dir = tmp_path / "anchor"
    target_dir = tmp_path / "target"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=anchor_db)
    _write_project_config(target_dir, project_name="Target", project_id="target", db_path=anchor_db)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    before = _db_snapshot(anchor_db)
    original = config_cmds._precheck_register_conflict

    def drift_after_precheck(*args: Any, **kwargs: Any) -> None:
        original(*args, **kwargs)
        _write_project_config(target_dir, project_name="Target", project_id="target", db_path=redirected_db)

    with patch.object(config_cmds, "_precheck_register_conflict", side_effect=drift_after_precheck):
        result = invoke_gza("projects", "register", "--project", str(anchor_dir), "--path", str(target_dir))

    assert result.returncode == 1
    assert "DB path changed" in result.stdout
    assert _db_snapshot(anchor_db) == before
    assert not redirected_db.exists()


def test_projects_register_refuses_future_db_replacement_before_writable_open(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    original = config_cmds._registry_mutation_store

    def replace_before_open(config: Config, *, allow_bootstrap: bool = True) -> SqliteTaskStore:
        db_path.unlink()
        _write_future_db(db_path)
        return original(config, allow_bootstrap=allow_bootstrap)

    with patch.object(config_cmds, "_registry_mutation_store", side_effect=replace_before_open):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "newer than supported" in result.stdout
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone() == (SCHEMA_VERSION + 1,)


def test_projects_register_refuses_old_schema_db_that_appears_before_first_activation(tmp_path: Path) -> None:
    db_path = tmp_path / "missing" / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    original = config_cmds._registry_mutation_store

    def create_old_db_before_open(config: Config, *, allow_bootstrap: bool = True) -> SqliteTaskStore:
        assert allow_bootstrap is True
        _write_auto_migration_pending_registry_db(db_path, project_id="project")
        return original(config, allow_bootstrap=allow_bootstrap)

    with patch.object(config_cmds, "_registry_mutation_store", side_effect=create_old_db_before_open):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "appeared before mutation" in result.stdout
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone() == (SCHEMA_VERSION - 1,)
        assert conn.execute("SELECT root_path, config_path FROM projects WHERE id = 'project'").fetchone() == ("", "")
        assert conn.execute("SELECT project_id FROM tasks WHERE id = 'gza-1'").fetchone() == ("",)
        assert conn.execute("SELECT project_id FROM task_tags WHERE task_id = 'gza-1'").fetchone() == ("",)


def test_projects_register_refuses_missing_existing_db_before_writable_open(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    original = config_cmds._registry_mutation_store

    def remove_before_open(config: Config, *, allow_bootstrap: bool = True) -> SqliteTaskStore:
        db_path.unlink()
        return original(config, allow_bootstrap=allow_bootstrap)

    with patch.object(config_cmds, "_registry_mutation_store", side_effect=remove_before_open):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "disappeared before mutation" in result.stdout
    assert not db_path.exists()


def test_projects_register_existing_db_deleted_after_activation_refuses_without_recreating(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    original = SqliteTaskStore.register_project_paths_for_identity

    def delete_before_mutation(self: SqliteTaskStore, *args: Any, **kwargs: Any) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.unlink()
        return original(self, *args, **kwargs)

    with patch.object(SqliteTaskStore, "register_project_paths_for_identity", delete_before_mutation):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Error:" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr
    assert not db_path.exists()


def test_projects_deactivate_refuses_current_project_id_drift_before_activation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="current", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(db_path, project_id="alias", root_path="", config_path="")
    before = _db_snapshot(db_path)
    original = SqliteTaskStore.get_project_registry_entry

    def drift_after_lookup(self: SqliteTaskStore, project_id: str) -> Any:
        row = original(self, project_id)
        if project_id == "alias":
            _write_project_config(project_dir, project_name="Project", project_id="alias", db_path=db_path)
        return row

    with patch.object(SqliteTaskStore, "get_project_registry_entry", drift_after_lookup):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "project_id changed" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_deactivate_refuses_db_redirect_before_activation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    redirected_db = tmp_path / "redirected.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(db_path, project_id="alias", root_path="", config_path="")
    before = _db_snapshot(db_path)
    original = SqliteTaskStore.get_project_registry_entry

    def drift_after_lookup(self: SqliteTaskStore, project_id: str) -> Any:
        row = original(self, project_id)
        if project_id == "alias":
            _write_project_config(project_dir, project_name="Project", project_id="project", db_path=redirected_db)
        return row

    with patch.object(SqliteTaskStore, "get_project_registry_entry", drift_after_lookup):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "DB path changed" in result.stdout
    assert _db_snapshot(db_path) == before
    assert not redirected_db.exists()


def test_projects_deactivate_refuses_db_replacement_before_writable_open(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(db_path, project_id="alias", root_path="", config_path="")
    original = config_cmds._registry_mutation_store

    def replace_before_open(config: Config, *, allow_bootstrap: bool = True) -> SqliteTaskStore:
        db_path.unlink()
        _write_future_db(db_path)
        return original(config, allow_bootstrap=allow_bootstrap)

    with patch.object(config_cmds, "_registry_mutation_store", side_effect=replace_before_open):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "newer than supported" in result.stdout
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone() == (SCHEMA_VERSION + 1,)


def test_projects_deactivate_existing_db_deleted_after_activation_refuses_without_recreating(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(db_path, project_id="alias", root_path="", config_path="")
    original = SqliteTaskStore.deactivate_project_registry_row

    def delete_before_mutation(self: SqliteTaskStore, project_id: str) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.unlink()
        return original(self, project_id)

    with patch.object(SqliteTaskStore, "deactivate_project_registry_row", delete_before_mutation):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Error:" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr
    assert not db_path.exists()


def test_projects_deactivate_refuses_future_db_replacement_after_activation_without_mutating_either_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.deactivate_project_registry_row

    def replace_before_mutation(self: SqliteTaskStore, project_id: str) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.rename(original_db)
        _write_future_db(db_path)
        return original(self, project_id)

    with patch.object(SqliteTaskStore, "deactivate_project_registry_row", replace_before_mutation):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert _db_snapshot(original_db) == before_original
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone() == (SCHEMA_VERSION + 1,)


def test_projects_deactivate_refuses_current_db_replacement_after_activation_without_mutating_either_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    _write_current_registry_db(replacement_db, project_id="replacement")
    _insert_registry_row(
        replacement_db,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    _prepare_replacement_db_for_pragma_guard(replacement_db)
    replacement_before = _db_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.deactivate_project_registry_row

    def replace_before_mutation(self: SqliteTaskStore, project_id: str) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.rename(original_db)
        shutil.copy2(replacement_db, db_path)
        return original(self, project_id)

    with patch.object(SqliteTaskStore, "deactivate_project_registry_row", replace_before_mutation):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _registry_row(db_path, "alias") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_deactivate_refuses_current_db_replacement_before_writable_connection_without_pragmas(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(db_path, project_id="alias", root_path="", config_path="")
    _write_current_registry_db(replacement_db, project_id="replacement")
    _insert_registry_row(
        replacement_db,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    _prepare_replacement_db_for_pragma_guard(replacement_db)
    replacement_before = _db_snapshot(replacement_db)
    replacement_journal_before = _journal_mode(replacement_db)
    replacement_sidecars_before = _sidecar_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.deactivate_project_registry_row

    def replace_before_writable_connection(self: SqliteTaskStore, project_id: str) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        _install_replacement_db(db_path, original_db=original_db, replacement_db=replacement_db)
        return original(self, project_id)

    with patch.object(SqliteTaskStore, "deactivate_project_registry_row", replace_before_writable_connection):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert "Project registry row deactivated" not in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _journal_mode(db_path) == replacement_journal_before
    assert _sidecar_snapshot(db_path) == replacement_sidecars_before
    assert _registry_row(db_path, "alias") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_deactivate_refuses_current_db_replacement_after_transaction_check_without_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _insert_registry_row(
        db_path,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    _write_current_registry_db(replacement_db, project_id="replacement")
    _insert_registry_row(
        replacement_db,
        project_id="alias",
        root_path=str(project_dir.resolve()),
        config_path=str((project_dir / "gza.yaml").resolve()),
    )
    replacement_before = _db_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original_validate = SqliteTaskStore._validate_registry_mutation_transaction
    calls = 0

    def replace_after_transaction_check(self: SqliteTaskStore, conn: sqlite3.Connection) -> None:
        nonlocal calls
        original_validate(self, conn)
        if self._open_mode == "registry_mutation_existing":
            calls += 1
            if calls == 1:
                _install_replacement_db(db_path, original_db=original_db, replacement_db=replacement_db)

    with patch.object(
        SqliteTaskStore,
        "_validate_registry_mutation_transaction",
        replace_after_transaction_check,
    ):
        result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert "Project registry row deactivated" not in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _registry_row(original_db, "alias") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )
    assert _registry_row(db_path, "alias") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_refuses_future_db_replacement_after_activation_without_mutating_either_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.register_project_paths_for_identity

    def replace_before_mutation(self: SqliteTaskStore, *args: Any, **kwargs: Any) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.rename(original_db)
        _write_future_db(db_path)
        return original(self, *args, **kwargs)

    with patch.object(SqliteTaskStore, "register_project_paths_for_identity", replace_before_mutation):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert _db_snapshot(original_db) == before_original
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT version FROM schema_version").fetchone() == (SCHEMA_VERSION + 1,)


def test_projects_register_refuses_current_db_replacement_after_activation_without_mutating_either_db(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _write_current_registry_db(replacement_db, project_id="replacement")
    _prepare_replacement_db_for_pragma_guard(replacement_db)
    replacement_before = _db_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.register_project_paths_for_identity

    def replace_before_mutation(self: SqliteTaskStore, *args: Any, **kwargs: Any) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        db_path.rename(original_db)
        shutil.copy2(replacement_db, db_path)
        return original(self, *args, **kwargs)

    with patch.object(SqliteTaskStore, "register_project_paths_for_identity", replace_before_mutation):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _registry_row(db_path, "project") is None


def test_projects_register_refuses_current_db_replacement_before_writable_connection_without_pragmas(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _write_current_registry_db(replacement_db, project_id="replacement")
    _prepare_replacement_db_for_pragma_guard(replacement_db)
    replacement_before = _db_snapshot(replacement_db)
    replacement_journal_before = _journal_mode(replacement_db)
    replacement_sidecars_before = _sidecar_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original = SqliteTaskStore.register_project_paths_for_identity

    def replace_before_writable_connection(self: SqliteTaskStore, *args: Any, **kwargs: Any) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        _install_replacement_db(db_path, original_db=original_db, replacement_db=replacement_db)
        return original(self, *args, **kwargs)

    with patch.object(SqliteTaskStore, "register_project_paths_for_identity", replace_before_writable_connection):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert "Project project" not in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _journal_mode(db_path) == replacement_journal_before
    assert _sidecar_snapshot(db_path) == replacement_sidecars_before
    assert _registry_row(db_path, "project") is None


def test_projects_register_refuses_current_db_replacement_after_transaction_check_without_success(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    original_db = tmp_path / "original.db"
    replacement_db = tmp_path / "replacement.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    _write_current_registry_db(replacement_db, project_id="replacement")
    replacement_before = _db_snapshot(replacement_db)
    before_original = _db_snapshot(db_path)
    original_validate = SqliteTaskStore._validate_registry_mutation_transaction
    calls = 0

    def replace_after_transaction_check(self: SqliteTaskStore, conn: sqlite3.Connection) -> None:
        nonlocal calls
        original_validate(self, conn)
        if self._open_mode == "registry_mutation_existing":
            calls += 1
            if calls == 1:
                _install_replacement_db(db_path, original_db=original_db, replacement_db=replacement_db)

    with patch.object(
        SqliteTaskStore,
        "_validate_registry_mutation_transaction",
        replace_after_transaction_check,
    ):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "changed after validation" in result.stdout
    assert "Project project" not in result.stdout
    assert _db_snapshot(original_db) == before_original
    assert _db_snapshot(db_path) == replacement_before
    assert _registry_row(db_path, "project") is None


def test_projects_register_failure_after_activation_does_not_backfill_task_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    store = SqliteTaskStore.from_config(Config.load(project_dir))
    task = store.add("legacy grouped task")
    assert task.id is not None
    with sqlite3.connect(db_path) as conn:
        conn.execute('UPDATE tasks SET "group" = ? WHERE id = ?', ("legacy", task.id))
        conn.execute("DELETE FROM task_tags WHERE task_id = ?", (task.id,))
    before = _db_snapshot(db_path)

    def fail_after_activation(self: SqliteTaskStore, *args: Any, **kwargs: Any) -> Any:
        assert self._open_mode == "registry_mutation_existing"
        raise sqlite3.OperationalError("simulated registry failure")

    with patch.object(SqliteTaskStore, "register_project_paths_for_identity", fail_after_activation):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "simulated registry failure" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_commands_report_malformed_yaml_without_traceback_or_db_mutation(tmp_path: Path) -> None:
    malformed_dir = tmp_path / "malformed"
    malformed_dir.mkdir()
    (malformed_dir / "gza.yaml").write_text("project_name: [unterminated\n", encoding="utf-8")
    for args in (
        ("projects", "diagnose", "--project", str(malformed_dir)),
        ("projects", "deactivate", "project", "--project", str(malformed_dir)),
        ("projects", "register", "--project", str(malformed_dir)),
    ):
        result = invoke_gza(*args)
        assert result.returncode == 1
        assert "Error:" in result.stdout
        assert "Traceback" not in result.stdout
        assert "Traceback" not in result.stderr
    assert not (malformed_dir / ".gza" / "gza.db").exists()


def test_projects_register_path_reports_malformed_target_yaml_without_db_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    target_dir = tmp_path / "target"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    target_dir.mkdir()
    (target_dir / "gza.yaml").write_text("project_name: [unterminated\n", encoding="utf-8")
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(anchor_dir), "--path", str(target_dir))

    assert result.returncode == 1
    assert "Error:" in result.stdout
    assert "Traceback" not in result.stdout
    assert "Traceback" not in result.stderr
    assert _db_snapshot(db_path) == before
    assert not (target_dir / ".gza" / "gza.db").exists()


def _make_symlink_loop(tmp_path: Path, name: str) -> Path:
    loop = tmp_path / name
    try:
        loop.symlink_to(loop)
    except OSError as exc:
        pytest.skip(f"symlinks are not supported here: {exc}")
    return loop


@pytest.mark.parametrize(
    "args",
    [
        ("projects", "register"),
        ("projects", "diagnose"),
        ("projects", "deactivate", "project"),
    ],
)
def test_projects_commands_reject_symlink_loop_project_without_traceback_or_db_mutation(
    tmp_path: Path,
    args: tuple[str, ...],
) -> None:
    loop = _make_symlink_loop(tmp_path, "loop-project")

    result = invoke_gza(*args, "--project", str(loop))

    assert result.returncode == 1
    combined = result.stdout + result.stderr
    assert "invalid or inaccessible" in combined
    _assert_no_traceback(result)
    assert not (tmp_path / ".gza" / "gza.db").exists()


def test_projects_register_rejects_symlink_loop_target_path_without_traceback_or_registry_mutation(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    target_loop = _make_symlink_loop(tmp_path, "loop-target")
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(anchor_dir), "--path", str(target_loop))

    assert result.returncode == 1
    assert "invalid or inaccessible" in result.stdout
    _assert_no_traceback(result)
    assert _db_snapshot(db_path) == before


def test_projects_deactivate_current_with_force_is_rejected_without_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    before = _db_snapshot(db_path)
    assert before is not None

    result = invoke_gza("projects", "deactivate", "project", "--project", str(project_dir), "--force")

    assert result.returncode == 1
    assert "currently selected project" in result.stdout
    assert "uv run gza projects diagnose" in result.stdout
    assert f"uv run gza projects register --project {shlex.quote(str(project_dir.resolve()))} --replace" in result.stdout
    assert _db_snapshot(db_path) == before
    assert _registry_row(db_path, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


@pytest.mark.parametrize(
    "path_name",
    [
        "project with spaces",
        "project with 'quote' and $semi;",
    ],
)
def test_projects_deactivate_current_refusal_quotes_register_repair_command(
    tmp_path: Path,
    path_name: str,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / path_name
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(project_dir))
    expected_command = f"uv run gza projects register --project {shlex.quote(str(project_dir.resolve()))} --replace"

    result = invoke_gza("projects", "deactivate", "project", "--project", str(project_dir))

    assert result.returncode == 1
    assert expected_command in result.stdout
    assert _registry_row(db_path, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_anchor_path_writes_target_to_anchor_registry(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    anchor_dir = tmp_path / "anchor"
    target_dir = tmp_path / "anchor" / "server"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=db_path)
    _write_project_config(target_dir, project_name="Server", project_id="server", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    before = _db_snapshot(db_path)
    assert before is not None

    result = invoke_gza(
        "projects",
        "register",
        "--project",
        str(anchor_dir),
        "--path",
        str(target_dir),
        "--project-id",
        "server",
    )

    assert result.returncode == 0
    after = _db_snapshot(db_path)
    assert after is not None
    assert _changed_tables(before, after) == {"projects"}
    assert _registry_row(db_path, "server") == (
        str(target_dir.resolve()),
        str((target_dir / "gza.yaml").resolve()),
    )


def test_projects_register_anchor_path_rejects_unrelated_target_db_without_mutation(tmp_path: Path) -> None:
    anchor_db_path = tmp_path / "anchor.db"
    target_db_path = tmp_path / "target.db"
    anchor_dir = tmp_path / "anchor"
    target_dir = tmp_path / "target"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=anchor_db_path)
    _write_project_config(target_dir, project_name="Target", project_id="target", db_path=target_db_path)
    SqliteTaskStore.from_config(Config.load(anchor_dir))
    before = _db_snapshot(anchor_db_path)

    result = invoke_gza(
        "projects",
        "register",
        "--project",
        str(anchor_dir),
        "--path",
        str(target_dir),
    )

    assert result.returncode == 1
    assert "does not match the selected registry anchor" in result.stdout
    assert _db_snapshot(anchor_db_path) == before
    assert not target_db_path.exists()


def test_projects_register_anchor_path_rejects_unrelated_target_db_without_creating_either_db(tmp_path: Path) -> None:
    anchor_db_path = tmp_path / "missing-anchor" / "anchor.db"
    target_db_path = tmp_path / "missing-target" / "target.db"
    anchor_dir = tmp_path / "anchor"
    target_dir = tmp_path / "target"
    _write_project_config(anchor_dir, project_name="Anchor", project_id="anchor", db_path=anchor_db_path)
    _write_project_config(target_dir, project_name="Target", project_id="target", db_path=target_db_path)

    result = invoke_gza(
        "projects",
        "register",
        "--project",
        str(anchor_dir),
        "--path",
        str(target_dir),
    )

    assert result.returncode == 1
    assert "does not match the selected registry anchor" in result.stdout
    assert not anchor_db_path.exists()
    assert not target_db_path.exists()


def test_projects_register_rejects_linked_worktree_without_replacing_prior_row(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    canonical_dir = tmp_path / "canonical"
    linked_dir = tmp_path / "linked"
    linked_git_dir = tmp_path / "canonical" / ".git" / "worktrees" / "linked"
    _write_project_config(canonical_dir, project_name="Project", project_id="project", db_path=db_path)
    SqliteTaskStore.from_config(Config.load(canonical_dir))
    linked_git_dir.mkdir(parents=True)
    (linked_git_dir / "commondir").write_text("../..", encoding="utf-8")
    linked_dir.mkdir()
    (linked_dir / ".git").write_text(f"gitdir: {linked_git_dir}\n", encoding="utf-8")
    _write_project_config(linked_dir, project_name="Project", project_id="project", db_path=db_path)
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(linked_dir), "--replace")

    assert result.returncode == 1
    assert "not a canonical checkout" in result.stdout
    assert _db_snapshot(db_path) == before
    assert _registry_row(db_path, "project") == (
        str(canonical_dir.resolve()),
        str((canonical_dir / "gza.yaml").resolve()),
    )


def test_projects_register_rejects_linked_worktree_first_registration_without_creating_db(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    linked_dir = tmp_path / "linked"
    linked_git_dir = tmp_path / "main" / ".git" / "worktrees" / "linked"
    linked_git_dir.mkdir(parents=True)
    (linked_git_dir / "commondir").write_text("../..", encoding="utf-8")
    linked_dir.mkdir()
    (linked_dir / ".git").write_text(f"gitdir: {linked_git_dir}\n", encoding="utf-8")
    _write_project_config(linked_dir, project_name="Project", project_id="project", db_path=db_path)

    result = invoke_gza("projects", "register", "--project", str(linked_dir))

    assert result.returncode == 1
    assert "not a canonical checkout" in result.stdout
    _assert_db_and_sidecars_absent(db_path)


def test_projects_register_resolves_relative_db_against_target_project(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    relative_db = Path(".gza") / "shared.db"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=relative_db)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 0
    assert _registry_row(project_dir / relative_db, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_succeeds_for_canonical_git_checkout(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    (project_dir / ".git").mkdir()

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 0
    assert _registry_row(db_path, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_failure_after_exclusive_create_removes_owned_db_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    original_ensure_db = SqliteTaskStore._ensure_db

    def fail_after_exclusive_create(self: SqliteTaskStore, *, allow_bootstrap: bool = True) -> None:
        if self._open_mode == "registry_mutation":
            raise SchemaIntegrityError("injected registry bootstrap failure")
        original_ensure_db(self, allow_bootstrap=allow_bootstrap)

    with patch.object(SqliteTaskStore, "_ensure_db", fail_after_exclusive_create):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "injected registry bootstrap failure" in result.stdout
    _assert_db_and_sidecars_absent(db_path)

    retry = invoke_gza("projects", "register", "--project", str(project_dir))

    assert retry.returncode == 0
    assert _registry_row(db_path, "project") == (
        str(project_dir.resolve()),
        str((project_dir / "gza.yaml").resolve()),
    )


def test_projects_register_failed_bootstrap_preserves_preexisting_public_sidecar_sentinels(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    sentinels = {
        "-wal": b"sentinel wal",
        "-shm": b"sentinel shm",
        "-journal": b"sentinel journal",
    }
    for suffix, content in sentinels.items():
        Path(f"{db_path}{suffix}").write_bytes(content)
    original_ensure_db = SqliteTaskStore._ensure_db

    def fail_after_schema_setup(self: SqliteTaskStore, *, allow_bootstrap: bool = True) -> None:
        original_ensure_db(self, allow_bootstrap=allow_bootstrap)
        if self._open_mode == "registry_mutation":
            raise SchemaIntegrityError("injected registry bootstrap failure after schema setup")

    with patch.object(SqliteTaskStore, "_ensure_db", fail_after_schema_setup):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "after schema setup" in result.stdout
    assert not db_path.exists()
    for suffix, content in sentinels.items():
        assert Path(f"{db_path}{suffix}").read_bytes() == content


def test_projects_register_publish_race_preserves_replacement_db_after_private_validation(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    replacement_db = tmp_path / "replacement.db"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    _write_current_registry_db(replacement_db, project_id="replacement")
    replacement_snapshot = _db_snapshot(replacement_db)

    def publish_replacement_then_refuse(src: str | Path, dst: str | Path) -> None:
        assert Path(dst) == db_path
        shutil.copy2(replacement_db, db_path)
        raise FileExistsError

    with patch("gza.db.os.link", side_effect=publish_replacement_then_refuse):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Registry DB appeared before mutation" in result.stdout
    assert _db_snapshot(db_path) == replacement_snapshot


def test_projects_register_publish_failure_cleanup_preserves_replacement_sidecars(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"
    project_dir = tmp_path / "project"
    replacement_db = tmp_path / "replacement.db"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    _write_current_registry_db(replacement_db, project_id="replacement")
    replacement_sidecars = {
        "-wal": b"replacement wal",
        "-shm": b"replacement shm",
        "-journal": b"replacement journal",
    }
    original_cleanup = SqliteTaskStore._cleanup_private_registry_bootstrap

    def fail_publish(src: str | Path, dst: str | Path) -> None:
        assert Path(dst) == db_path
        shutil.copy2(replacement_db, db_path)
        raise FileExistsError

    def install_sidecars_during_private_cleanup(self: SqliteTaskStore, temp_path: Path) -> None:
        for suffix, content in replacement_sidecars.items():
            Path(f"{db_path}{suffix}").write_bytes(content)
        original_cleanup(self, temp_path)

    with (
        patch("gza.db.os.link", side_effect=fail_publish),
        patch.object(SqliteTaskStore, "_cleanup_private_registry_bootstrap", install_sidecars_during_private_cleanup),
    ):
        result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "Registry DB appeared before mutation" in result.stdout
    for suffix, content in replacement_sidecars.items():
        assert Path(f"{db_path}{suffix}").read_bytes() == content


def test_projects_register_rejects_broken_db_ancestor_before_registry_mutation(tmp_path: Path) -> None:
    broken_parent = tmp_path / "broken-parent"
    broken_parent.symlink_to(tmp_path / "missing-target")
    db_path = broken_parent / "shared.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "invalid or inaccessible" in result.stdout
    assert not db_path.exists()


def test_projects_register_rejects_future_schema_before_registry_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "future.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION + 1,))
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "newer than supported" in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_register_rejects_manual_migration_db_before_registry_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "manual.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (24)")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "requires manual migration" in result.stdout
    assert "uv run gza migrate" in result.stdout
    assert "Run 'gza migrate" not in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_diagnose_rejects_manual_migration_db_with_repo_root_command(tmp_path: Path) -> None:
    db_path = tmp_path / "manual.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (24)")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "diagnose", "--project", str(project_dir))

    assert result.returncode == 1
    assert "requires manual migration" in result.stdout
    assert "uv run gza migrate" in result.stdout
    assert "Run 'gza migrate" not in result.stdout
    assert _db_snapshot(db_path) == before


def test_projects_deactivate_rejects_manual_migration_db_with_repo_root_command(tmp_path: Path) -> None:
    db_path = tmp_path / "manual.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (24)")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "deactivate", "alias", "--project", str(project_dir))

    assert result.returncode == 1
    assert "requires manual migration" in result.stdout
    assert "uv run gza migrate" in result.stdout
    assert "Run 'gza migrate" not in result.stdout
    assert _db_snapshot(db_path) == before


@pytest.mark.parametrize(
    "command_args",
    [
        ("projects", "register"),
        ("projects", "diagnose"),
        ("projects", "deactivate", "alias"),
    ],
)
def test_projects_manual_migration_refusal_keeps_explicit_target_project_in_command(
    tmp_path: Path,
    command_args: tuple[str, ...],
) -> None:
    cwd_project = tmp_path / "project-a"
    target_project = tmp_path / "project b with spaces"
    cwd_db = tmp_path / "a.db"
    target_db = tmp_path / "manual.db"
    _write_project_config(cwd_project, project_name="A", project_id="a", db_path=cwd_db)
    _write_project_config(target_project, project_name="B", project_id="b", db_path=target_db)
    SqliteTaskStore.from_config(Config.load(cwd_project))
    with sqlite3.connect(target_db) as conn:
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version (version) VALUES (24)")
    before_target = _db_snapshot(target_db)
    expected = f"uv run gza migrate --project {shlex.quote(str(target_project.resolve()))}"
    wrong_anchor = f"uv run gza migrate --project {shlex.quote(str(cwd_project.resolve()))}"

    result = invoke_gza(*command_args, "--project", str(target_project), cwd=cwd_project)

    assert result.returncode == 1
    assert "requires manual migration" in result.stdout
    assert expected in result.stdout
    assert wrong_anchor not in result.stdout
    assert "uv run gza migrate' from the project root" not in result.stdout
    assert _db_snapshot(target_db) == before_target


@pytest.mark.parametrize(
    "command_args",
    [
        ("projects", "register"),
        ("projects", "diagnose"),
        ("projects", "deactivate", "alias"),
    ],
)
def test_projects_current_schema_refusal_keeps_explicit_target_project_in_command(
    tmp_path: Path,
    command_args: tuple[str, ...],
) -> None:
    cwd_project = tmp_path / "project-a"
    target_project = tmp_path / "project b with spaces"
    cwd_db = tmp_path / "a.db"
    target_db = tmp_path / "old.db"
    _write_project_config(cwd_project, project_name="A", project_id="a", db_path=cwd_db)
    _write_project_config(target_project, project_name="B", project_id="b", db_path=target_db)
    SqliteTaskStore.from_config(Config.load(cwd_project))
    _write_auto_migration_pending_registry_db(
        target_db,
        project_id="b",
        root_path=str(target_project.resolve()),
        config_path=str((target_project / "gza.yaml").resolve()),
        alias_project_id="alias",
    )
    with sqlite3.connect(target_db) as conn:
        conn.execute("UPDATE schema_version SET version = 1")
    before_target = _db_snapshot(target_db)
    expected = f"uv run gza migrate --project {shlex.quote(str(target_project.resolve()))}"
    wrong_anchor = f"uv run gza migrate --project {shlex.quote(str(cwd_project.resolve()))}"

    result = invoke_gza(*command_args, "--project", str(target_project), cwd=cwd_project)

    assert result.returncode == 1
    assert expected in result.stdout
    assert wrong_anchor not in result.stdout
    assert "uv run gza migrate' from the project root" not in result.stdout
    assert _db_snapshot(target_db) == before_target


def test_projects_register_rejects_incompatible_db_before_registry_mutation(tmp_path: Path) -> None:
    db_path = tmp_path / "incompatible.db"
    project_dir = tmp_path / "project"
    _write_project_config(project_dir, project_name="Project", project_id="project", db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE unrelated (value TEXT)")
    before = _db_snapshot(db_path)

    result = invoke_gza("projects", "register", "--project", str(project_dir))

    assert result.returncode == 1
    assert "missing schema_version" in result.stdout
    assert _db_snapshot(db_path) == before
