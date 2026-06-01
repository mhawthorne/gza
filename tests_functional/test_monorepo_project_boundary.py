import os
import subprocess
from pathlib import Path

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore, task_id_numeric_key
from gza.git import Git
from tests_functional.helpers.cli import run_gza_subprocess


def _init_repo(repo_root: Path) -> None:
    subprocess.run(["git", "init", "-b", "main"], cwd=repo_root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=repo_root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo_root, check=True, capture_output=True, text=True)


def _write_fake_codex(bin_dir: Path) -> None:
    fake_codex = bin_dir / "codex"
    fake_codex.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import sys\n"
        "from pathlib import Path\n"
        "\n"
        "if '--version' in sys.argv:\n"
        "    print('codex 0.0-test')\n"
        "    raise SystemExit(0)\n"
        "\n"
        "cwd = Path.cwd().resolve()\n"
        "repo_root = next((path for path in (cwd, *cwd.parents) if (path / '.git').exists()), cwd)\n"
        "marker_path = Path(__import__('os').environ['FAKE_CODEX_CWD_MARKER'])\n"
        "marker_path.write_text(str(cwd))\n"
        "prompt = sys.stdin.read()\n"
        "if 'WRITE_OUTSIDE' in prompt:\n"
        "    target = repo_root / 'services' / 'bar' / 'outside.txt'\n"
        "elif 'WRITE_DEP' in prompt:\n"
        "    target = repo_root / 'libs' / 'shared' / 'shared.txt'\n"
        "else:\n"
        "    target = repo_root / 'services' / 'foo' / 'inside.txt'\n"
        "target.parent.mkdir(parents=True, exist_ok=True)\n"
        "target.write_text(prompt.strip() + '\\n')\n"
        "print(json.dumps({'type': 'thread.started', 'thread_id': 'test-session'}))\n"
    )
    fake_codex.chmod(0o755)


def _setup_monorepo(tmp_path: Path, *, enforce_project_scope: bool = True) -> tuple[Path, Path]:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "foo"
    sibling_dir = repo_root / "services" / "bar"
    shared_dir = repo_root / "libs" / "shared"
    project_dir.mkdir(parents=True)
    sibling_dir.mkdir(parents=True)
    shared_dir.mkdir(parents=True)
    _init_repo(repo_root)

    (project_dir / "app.py").write_text("print('foo')\n")
    (sibling_dir / "app.py").write_text("print('bar')\n")
    (shared_dir / "util.py").write_text("print('shared')\n")
    (project_dir / "uv.lock").write_text(
        "[[package]]\n"
        'name = "shared"\n'
        'source = { directory = "../../libs/shared" }\n'
    )
    (project_dir / "gza.yaml").write_text(
        "project_name: foo\n"
        "project_prefix: foo\n"
        "provider: codex\n"
        "use_docker: false\n"
        f"enforce_project_scope: {'true' if enforce_project_scope else 'false'}\n"
        "worktree_dir: .gza-test-worktrees\n"
        "db_path: .gza/gza.db\n"
    )
    (sibling_dir / "gza.yaml").write_text("project_name: bar\nproject_prefix: bar\n")

    subprocess.run(["git", "add", "."], cwd=repo_root, check=True, capture_output=True, text=True)
    subprocess.run(["git", "commit", "-m", "initial"], cwd=repo_root, check=True, capture_output=True, text=True)
    return repo_root, project_dir


def _test_env(bin_dir: Path, marker_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["CODEX_API_KEY"] = "test-key"
    env["FAKE_CODEX_CWD_MARKER"] = str(marker_path)
    return env


def _store_for(project_dir: Path) -> SqliteTaskStore:
    config = Config.load(project_dir)
    return SqliteTaskStore(config.db_path, prefix=config.project_prefix)


@pytest.mark.functional
def test_monorepo_project_boundary_flow(tmp_path: Path) -> None:
    repo_root, project_dir = _setup_monorepo(tmp_path)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_codex(bin_dir)
    marker_path = tmp_path / ".fake-codex-cwd"
    env = _test_env(bin_dir, marker_path)
    store = _store_for(project_dir)

    blocked = store.add("WRITE_OUTSIDE", task_type="implement")
    blocked_result = run_gza_subprocess(
        "work",
        str(blocked.id),
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        env=env,
        timeout=60,
    )

    assert blocked_result.returncode == 0, blocked_result.stderr
    blocked_task = store.get(blocked.id)
    assert blocked_task is not None
    assert blocked_task.status == "failed"
    assert blocked_task.failure_reason == "PROJECT_SCOPE_VIOLATION"
    assert "services/bar/outside.txt" in (blocked_result.stdout + blocked_result.stderr)
    assert marker_path.read_text().endswith("/services/foo")
    git = Git(project_dir)
    assert blocked_task.branch is not None
    assert git.count_commits_ahead(blocked_task.branch, "main") == 0

    allowed = store.add("WRITE_DEP", task_type="implement")
    allowed_result = run_gza_subprocess(
        "work",
        str(allowed.id),
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        env=env,
        timeout=60,
    )

    assert allowed_result.returncode == 0, allowed_result.stderr
    allowed_task = store.get(allowed.id)
    assert allowed_task is not None
    assert allowed_task.status == "completed"
    assert allowed_task.branch is not None
    assert git.count_commits_ahead(allowed_task.branch, "main") == 1

    edit_result = run_gza_subprocess(
        "edit",
        str(blocked.id),
        "--add-tag",
        "cross-project",
        "--project",
        str(project_dir),
        cwd=project_dir,
        env=env,
        timeout=60,
    )
    assert edit_result.returncode == 0, edit_result.stderr

    retry_result = run_gza_subprocess(
        "retry",
        str(blocked.id),
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        env=env,
        timeout=60,
    )
    assert retry_result.returncode == 0, retry_result.stderr

    retried = max(_store_for(project_dir).get_all(), key=lambda task: task_id_numeric_key(task.id))
    assert retried.id != blocked.id
    assert retried.status == "completed"
    assert retried.branch is not None
    assert git.count_commits_ahead(retried.branch, "main") == 1
    assert marker_path.read_text().endswith("/services/foo")


@pytest.mark.functional
def test_monorepo_project_boundary_can_be_disabled(tmp_path: Path) -> None:
    _repo_root, project_dir = _setup_monorepo(tmp_path, enforce_project_scope=False)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_fake_codex(bin_dir)
    env = _test_env(bin_dir, tmp_path / ".fake-codex-cwd")
    store = _store_for(project_dir)

    task = store.add("WRITE_OUTSIDE", task_type="implement")
    result = run_gza_subprocess(
        "work",
        str(task.id),
        "--no-docker",
        "--project",
        str(project_dir),
        cwd=project_dir,
        env=env,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    refreshed = store.get(task.id)
    assert refreshed is not None
    assert refreshed.status == "completed"
