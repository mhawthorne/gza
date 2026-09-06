"""Functional coverage for disposable verify DB snapshot process access."""

import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from gza.providers import DockerConfig
from gza.providers.base import build_docker_cmd
from gza.runner import disposable_verify_db_snapshot_env
from gza.runtime_context import RuntimeExecutionContext


def _write_items_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO items (name) VALUES ('live')")
        conn.commit()
    finally:
        conn.close()


def _items(db_path: Path) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return [row[0] for row in conn.execute("SELECT name FROM items ORDER BY id")]
    finally:
        conn.close()


def _metadata(paths: tuple[Path, ...]) -> dict[Path, tuple[int, int]]:
    return {path: (path.stat().st_gid, stat.S_IMODE(path.stat().st_mode)) for path in paths}


def _ancestors_until_tmp_root(path: Path) -> tuple[Path, ...]:
    tmp_root = Path(os.environ.get("TMPDIR", "/tmp")).resolve()
    current = path.resolve()
    ancestors: list[Path] = []
    while True:
        if current == tmp_root or current.parent == current:
            return tuple(ancestors)
        ancestors.append(current)
        current = current.parent


def _gid_other_than(*excluded: int) -> int:
    excluded_set = set(excluded)
    for gid in (65534, 65533, 65532, 65531, 65530):
        if gid not in excluded_set:
            return gid
    raise AssertionError("could not choose a distinct test gid")


@pytest.mark.functional
def test_docker_verify_snapshot_is_writable_by_different_uid_process_and_cleaned(
    tmp_path: Path,
) -> None:
    if os.name != "posix" or os.geteuid() != 0:
        pytest.skip("requires root privileges to execute the regression under a different UID/GID")

    host_uid = os.getuid()
    host_gid = os.getgid()
    cwd_gid = _gid_other_than(host_gid)
    gza_gid = _gid_other_than(host_gid, cwd_gid)
    tmp_gid = _gid_other_than(host_gid, cwd_gid, gza_gid)
    child_primary_gid = _gid_other_than(host_gid, cwd_gid, gza_gid, tmp_gid)
    alternate_uid = 65534 if host_uid != 65534 else 65533

    db_path = tmp_path / "repo" / ".gza" / "gza.db"
    _write_items_db(db_path)

    untouched_ancestors = _ancestors_until_tmp_root(tmp_path)
    ancestor_metadata = _metadata(untouched_ancestors)
    dedicated_root = Path(tempfile.mkdtemp(prefix="gza-verify-snapshot-"))
    try:
        os.chown(dedicated_root, host_uid, cwd_gid)
        dedicated_root.chmod(0o711)
        verify_cwd = dedicated_root / "worktree"
        (verify_cwd / ".gza" / "tmp").mkdir(parents=True)
        os.chown(verify_cwd, host_uid, cwd_gid)
        os.chown(verify_cwd / ".gza", host_uid, gza_gid)
        os.chown(verify_cwd / ".gza" / "tmp", host_uid, tmp_gid)
        verify_cwd.chmod(0o700)
        (verify_cwd / ".gza").chmod(0o750)
        (verify_cwd / ".gza" / "tmp").chmod(0o2700)
        original_snapshot_parent_metadata = _metadata((verify_cwd, verify_cwd / ".gza", verify_cwd / ".gza" / "tmp"))

        runtime_context = RuntimeExecutionContext(
            cwd=tmp_path / "repo",
            env={"GZA_DB_PATH": str(db_path)},
            project_id="project",
            db_path=db_path,
        )
        config = SimpleNamespace(use_docker=True, docker_workdir="/workspace")

        supplemental_groups: tuple[int, ...] = ()

        def use_alternate_identity() -> None:
            os.setgroups(list(supplemental_groups))
            os.setgid(child_primary_gid)
            os.setuid(alternate_uid)

        with disposable_verify_db_snapshot_env(runtime_context, cwd=verify_cwd, config=config) as snapshot:
            snapshot_path = snapshot.host_path
            snapshot_dir = snapshot_path.parent
            supplemental_groups = snapshot.docker_group_ids
            assert {cwd_gid, gza_gid, tmp_gid}.issubset(supplemental_groups)
            assert host_gid not in supplemental_groups
            assert snapshot.env["GZA_DOCKER_GROUP_ADD"] == ",".join(str(gid) for gid in supplemental_groups)
            assert snapshot_path.stat().st_gid == tmp_gid
            docker_config = DockerConfig(
                image_name="test-image",
                npm_package="@test/cli",
                cli_command="testcli",
                config_dir=None,
                env_vars=[],
            )
            docker_cmd = build_docker_cmd(
                docker_config,
                verify_cwd,
                timeout_minutes=10,
                host_env=snapshot.env,
            )
            docker_groups = tuple(
                int(docker_cmd[index + 1])
                for index, value in enumerate(docker_cmd)
                if value == "--group-add"
            )
            assert docker_groups == supplemental_groups
            for suffix in ("-wal", "-shm", "-journal"):
                assert not Path(f"{snapshot_path}{suffix}").exists()

            script = (
                "import json, os, pathlib, sqlite3\n"
                "db_path = os.environ['SNAPSHOT_DB']\n"
                "assert os.getuid() != int(os.environ['HOST_UID'])\n"
                "assert os.getgid() != int(os.environ['HOST_GID'])\n"
                "assert os.getgid() != int(os.environ['SNAPSHOT_GID'])\n"
                "assert os.getgroups() == [int(gid) for gid in os.environ['EXPECTED_GROUPS'].split(',')]\n"
                "path = pathlib.Path(db_path)\n"
                "assert path.exists()\n"
                "conn = sqlite3.connect(db_path)\n"
                "journal_mode = conn.execute('PRAGMA journal_mode=WAL').fetchone()[0]\n"
                "assert journal_mode.lower() == 'wal'\n"
                "conn.execute(\"INSERT INTO items (name) VALUES ('snapshot-only')\")\n"
                "conn.commit()\n"
                "sidecars = {}\n"
                "for suffix in ('-wal', '-shm'):\n"
                "    sidecar = pathlib.Path(db_path + suffix)\n"
                "    sidecar_stat = sidecar.stat()\n"
                "    assert sidecar_stat.st_uid == os.getuid()\n"
                "    assert sidecar_stat.st_gid in [int(gid) for gid in os.environ['EXPECTED_GROUPS'].split(',')]\n"
                "    sidecars[suffix] = {'uid': sidecar_stat.st_uid, 'gid': sidecar_stat.st_gid}\n"
                "conn.close()\n"
                "print(json.dumps({'journal_mode': journal_mode, 'sidecars': sidecars}))\n"
            )
            env = {
                **os.environ,
                "SNAPSHOT_DB": str(snapshot_path),
                "HOST_UID": str(host_uid),
                "HOST_GID": str(host_gid),
                "SNAPSHOT_GID": str(tmp_gid),
                "EXPECTED_GROUPS": ",".join(str(gid) for gid in supplemental_groups),
            }
            result = subprocess.run(
                [sys.executable, "-c", script],
                env=env,
                text=True,
                capture_output=True,
                timeout=10,
                preexec_fn=use_alternate_identity,
            )

            assert result.returncode == 0, result.stderr
            child_report = json.loads(result.stdout)
            assert child_report["journal_mode"].lower() == "wal"
            assert child_report["sidecars"]["-wal"]["uid"] == alternate_uid
            assert child_report["sidecars"]["-shm"]["uid"] == alternate_uid
            assert _items(snapshot_path) == ["live", "snapshot-only"]
            assert _items(db_path) == ["live"]

        assert not snapshot_path.exists()
        assert not snapshot_dir.exists()
        for suffix in ("-wal", "-shm", "-journal"):
            assert not Path(f"{snapshot_path}{suffix}").exists()
        assert _items(db_path) == ["live"]
        assert _metadata((verify_cwd, verify_cwd / ".gza", verify_cwd / ".gza" / "tmp")) == original_snapshot_parent_metadata
        assert _metadata(untouched_ancestors) == ancestor_metadata
    finally:
        shutil.rmtree(dedicated_root, ignore_errors=True)
