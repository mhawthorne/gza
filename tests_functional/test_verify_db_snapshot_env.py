"""Functional coverage for disposable verify DB snapshot process access."""

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

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


def _make_group_traversable_from_tmp_root(path: Path, gid: int) -> None:
    tmp_root = Path(os.environ.get("TMPDIR", "/tmp")).resolve()
    current = path.resolve()
    while True:
        if current == tmp_root or current.parent == current:
            return
        os.chown(current, -1, gid)
        current.chmod(current.stat().st_mode | 0o010)
        current = current.parent


def _gid_other_than(*excluded: int) -> int:
    excluded_set = set(excluded)
    for gid in (65534, 65533, 65532):
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
    setgid_parent_gid = _gid_other_than(host_gid)
    child_primary_gid = _gid_other_than(host_gid, setgid_parent_gid)
    alternate_uid = 65534 if host_uid != 65534 else 65533

    db_path = tmp_path / "repo" / ".gza" / "gza.db"
    _write_items_db(db_path)

    setgid_parent = tmp_path / "shared-parent"
    setgid_parent.mkdir()
    os.chown(setgid_parent, host_uid, setgid_parent_gid)
    setgid_parent.chmod(0o2770)
    verify_cwd = setgid_parent / "worktree"
    verify_cwd.mkdir()
    assert verify_cwd.stat().st_gid == setgid_parent_gid

    runtime_context = RuntimeExecutionContext(
        cwd=tmp_path / "repo",
        env={"GZA_DB_PATH": str(db_path)},
        project_id="project",
        db_path=db_path,
    )
    config = SimpleNamespace(use_docker=True, docker_workdir="/workspace")
    _make_group_traversable_from_tmp_root(setgid_parent, setgid_parent_gid)

    supplemental_groups: tuple[int, ...] = ()

    def use_alternate_identity() -> None:
        os.setgroups(list(supplemental_groups))
        os.setgid(child_primary_gid)
        os.setuid(alternate_uid)

    with disposable_verify_db_snapshot_env(runtime_context, cwd=verify_cwd, config=config) as snapshot:
        snapshot_path = snapshot.host_path
        snapshot_dir = snapshot_path.parent
        supplemental_groups = snapshot.docker_group_ids
        assert setgid_parent_gid in supplemental_groups
        assert host_gid not in supplemental_groups
        assert snapshot.env["GZA_DOCKER_GROUP_ADD"] == ",".join(str(gid) for gid in supplemental_groups)
        assert snapshot_path.stat().st_gid == setgid_parent_gid
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
            "    sidecars[suffix] = {'uid': sidecar_stat.st_uid, 'gid': sidecar_stat.st_gid}\n"
            "conn.close()\n"
            "print(json.dumps({'journal_mode': journal_mode, 'sidecars': sidecars}))\n"
        )
        env = {
            **os.environ,
            "SNAPSHOT_DB": str(snapshot_path),
            "HOST_UID": str(host_uid),
            "HOST_GID": str(host_gid),
            "SNAPSHOT_GID": str(setgid_parent_gid),
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
