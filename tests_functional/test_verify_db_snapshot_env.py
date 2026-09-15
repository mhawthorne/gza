"""Functional coverage for disposable verify DB snapshot process access."""

import contextlib
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
from gza.providers.base import build_docker_cmd, is_docker_running
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


def _require_python_docker_image(tmp_path: Path) -> str:
    image_name = os.environ.get("GZA_FUNCTIONAL_DOCKER_IMAGE", "python:3.12-slim")
    if shutil.which("docker") is None:
        pytest.skip("requires Docker CLI")
    if not is_docker_running(host_cwd=tmp_path):
        pytest.skip("requires running Docker daemon")
    inspect = subprocess.run(
        ["docker", "image", "inspect", image_name],
        text=True,
        capture_output=True,
        timeout=10,
    )
    if inspect.returncode != 0:
        pytest.skip(f"requires locally available Python Docker image: {image_name}")
    return image_name


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
            assert supplemental_groups == (tmp_gid,)
            assert host_gid not in supplemental_groups
            assert snapshot.env["GZA_DOCKER_GROUP_ADD"] == ",".join(str(gid) for gid in supplemental_groups)
            assert snapshot.docker_volumes == (snapshot.env["GZA_DOCKER_VERIFY_DB_VOLUME"],)
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
            docker_mounts = [
                docker_cmd[index + 1]
                for index, value in enumerate(docker_cmd)
                if value == "-v"
            ]
            docker_env = [
                docker_cmd[index + 1]
                for index, value in enumerate(docker_cmd)
                if value == "-e"
            ]
            docker_groups = tuple(
                int(docker_cmd[index + 1])
                for index, value in enumerate(docker_cmd)
                if value == "--group-add"
            )
            assert snapshot.env["GZA_DOCKER_VERIFY_DB_VOLUME"] in docker_mounts
            assert f"GZA_DB_PATH={snapshot.env['GZA_DOCKER_VERIFY_DB_PATH']}" in docker_env
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


@pytest.mark.functional
def test_overlapping_docker_verify_snapshots_write_isolated_wal_data_and_cleanup(
    tmp_path: Path,
) -> None:
    image_name = _require_python_docker_image(tmp_path)
    db_path = tmp_path / "repo" / ".gza" / "gza.db"
    _write_items_db(db_path)
    verify_cwd = tmp_path / "worktree"
    verify_tmp = verify_cwd / ".gza" / "tmp"
    sync_dir = verify_cwd / "sync"
    verify_tmp.mkdir(parents=True)
    sync_dir.mkdir()
    for path, mode in (
        (verify_cwd, 0o700),
        (verify_cwd / ".gza", 0o701),
        (verify_tmp, 0o705),
    ):
        path.chmod(mode)
    preexisting_paths = (verify_cwd, verify_cwd / ".gza", verify_tmp)
    preexisting_metadata = _metadata(preexisting_paths)

    runtime_context = RuntimeExecutionContext(
        cwd=tmp_path / "repo",
        env={"GZA_DB_PATH": str(db_path)},
        project_id="project",
        db_path=db_path,
    )
    config = SimpleNamespace(use_docker=True, docker_workdir="/workspace")
    docker_config = DockerConfig(
        image_name=image_name,
        npm_package="@test/cli",
        cli_command="testcli",
        config_dir=None,
        env_vars=[],
    )
    script = (
        "import json, os, pathlib, sqlite3, time\n"
        "db_path = os.environ['GZA_DB_PATH']\n"
        "run_id = os.environ['RUN_ID']\n"
        "sync_dir = pathlib.Path(os.environ['SYNC_DIR'])\n"
        "conn = sqlite3.connect(db_path)\n"
        "journal_mode = conn.execute('PRAGMA journal_mode=WAL').fetchone()[0]\n"
        "assert journal_mode.lower() == 'wal'\n"
        "conn.execute('INSERT INTO items (name) VALUES (?)', (run_id,))\n"
        "conn.commit()\n"
        "sidecars = {suffix: pathlib.Path(db_path + suffix).exists() for suffix in ('-wal', '-shm')}\n"
        "(sync_dir / f'{run_id}.ready').write_text('ready')\n"
        "deadline = time.monotonic() + 20\n"
        "while not all((sync_dir / f'{name}.ready').exists() for name in ('first', 'second')):\n"
        "    if time.monotonic() > deadline:\n"
        "        raise TimeoutError('peer container did not overlap')\n"
        "    time.sleep(0.05)\n"
        "rows = [row[0] for row in conn.execute('SELECT name FROM items ORDER BY id')]\n"
        "print(json.dumps({'run_id': run_id, 'journal_mode': journal_mode, 'sidecars': sidecars, 'rows': rows}))\n"
        "conn.close()\n"
    )

    processes: list[subprocess.Popen[str]] = []
    with contextlib.ExitStack() as stack:
        first = stack.enter_context(disposable_verify_db_snapshot_env(runtime_context, cwd=verify_cwd, config=config))
        second = stack.enter_context(disposable_verify_db_snapshot_env(runtime_context, cwd=verify_cwd, config=config))
        first_dir = first.host_path.parent
        second_dir = second.host_path.parent
        try:
            assert first.host_path.parent != second.host_path.parent
            assert first.docker_volumes != second.docker_volumes
            assert first.env["GZA_DOCKER_VERIFY_DB_PATH"] != second.env["GZA_DOCKER_VERIFY_DB_PATH"]
            assert _metadata(preexisting_paths) == preexisting_metadata

            for run_id, snapshot in (("first", first), ("second", second)):
                cmd = build_docker_cmd(
                    docker_config,
                    verify_cwd,
                    timeout_minutes=1,
                    docker_env=[f"RUN_ID={run_id}", "SYNC_DIR=/workspace/sync"],
                    host_env=snapshot.env,
                )
                cmd.extend(["python", "-c", script])
                processes.append(
                    subprocess.Popen(
                        cmd,
                        cwd=verify_cwd,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        stdin=subprocess.DEVNULL,
                    )
                )

            reports = []
            for process in processes:
                stdout, stderr = process.communicate(timeout=30)
                assert process.returncode == 0, stderr
                reports.append(json.loads(stdout))

            assert {report["run_id"] for report in reports} == {"first", "second"}
            for report in reports:
                assert report["journal_mode"].lower() == "wal"
                assert report["sidecars"] == {"-wal": True, "-shm": True}
                assert report["rows"] == ["live", report["run_id"]]

            assert _items(first.host_path) == ["live", "first"]
            assert _items(second.host_path) == ["live", "second"]
            assert _items(db_path) == ["live"]
            assert _metadata(preexisting_paths) == preexisting_metadata
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

    assert not first.host_path.exists()
    assert not second.host_path.exists()
    assert not first_dir.exists()
    assert not second_dir.exists()
    assert _items(db_path) == ["live"]
    assert _metadata(preexisting_paths) == preexisting_metadata
