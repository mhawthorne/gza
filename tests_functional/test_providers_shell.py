"""Shell-command regression tests for provider entrypoints and setup cwd behavior."""

import os
import shlex
import subprocess
from pathlib import Path

from gza.providers.base import GZA_GIT_GUARD_SETUP_COMMAND


class TestDockerEntrypointFunctional:
    def test_entrypoint_runs_setup_command_before_cli_handoff(self, tmp_path):
        """Entrypoint must complete docker setup before invoking the provider CLI command."""
        entrypoint = tmp_path / "entrypoint.sh"
        entrypoint.write_text(
            "#!/bin/bash\n"
            "set -e\n"
            'if [ -n "$GZA_DOCKER_SETUP_COMMAND" ]; then\n'
            '    eval "$GZA_DOCKER_SETUP_COMMAND"\n'
            "fi\n"
            'exec "$@"\n'
        )
        entrypoint.chmod(0o755)

        marker = tmp_path / "setup.done"
        order_log = tmp_path / "order.log"
        env = os.environ.copy()
        env["SETUP_MARKER"] = str(marker)
        env["ORDER_LOG"] = str(order_log)
        env["GZA_DOCKER_SETUP_COMMAND"] = 'touch "$SETUP_MARKER"; printf "setup\\n" >> "$ORDER_LOG"'

        cli_cmd = f'test -f {shlex.quote(str(marker))}; printf "cli\\n" >> {shlex.quote(str(order_log))}'
        result = subprocess.run(
            [str(entrypoint), "bash", "-c", cli_cmd],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        assert marker.exists()
        assert order_log.read_text().splitlines() == ["setup", "cli"]

    def test_entrypoint_prewarm_makes_subsequent_uv_run_noop_for_sync(self, tmp_path):
        """Pre-warm uv sync should avoid lazy install/sync during later uv run calls."""
        entrypoint = tmp_path / "entrypoint.sh"
        entrypoint.write_text(
            "#!/bin/bash\n"
            "set -e\n"
            'if [ -n "$GZA_DOCKER_SETUP_COMMAND" ]; then\n'
            '    eval "$GZA_DOCKER_SETUP_COMMAND"\n'
            "fi\n"
            'exec "$@"\n'
        )
        entrypoint.chmod(0o755)

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        fake_uv = bin_dir / "uv"
        fake_uv.write_text(
            "#!/bin/bash\n"
            "set -e\n"
            'cmd="${1:-}"\n'
            "if [ $# -gt 0 ]; then\n"
            "    shift\n"
            "fi\n"
            'case "$cmd" in\n'
            "sync)\n"
            '    printf "sync\\n" >> "$UV_STATE_FILE"\n'
            '    : > "$UV_PREWARM_STAMP"\n'
            "    ;;\n"
            "run)\n"
            '    if [ ! -f "$UV_PREWARM_STAMP" ]; then\n'
            '        printf "lazy-sync\\n" >> "$UV_STATE_FILE"\n'
            '        : > "$UV_PREWARM_STAMP"\n'
            "    fi\n"
            '    printf "run\\n" >> "$UV_STATE_FILE"\n'
            '    "$@"\n'
            "    ;;\n"
            "*)\n"
            '    printf "unknown:%s\\n" "$cmd" >> "$UV_STATE_FILE"\n'
            "    ;;\n"
            "esac\n"
        )
        fake_uv.chmod(0o755)

        state_file = tmp_path / "uv-state.log"
        prewarm_stamp = tmp_path / "prewarm.stamp"
        env = os.environ.copy()
        env["UV_STATE_FILE"] = str(state_file)
        env["UV_PREWARM_STAMP"] = str(prewarm_stamp)
        env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
        env["GZA_DOCKER_SETUP_COMMAND"] = "uv sync"

        result = subprocess.run(
            [str(entrypoint), "bash", "-c", "uv run true; uv run true"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        state_lines = state_file.read_text().splitlines()
        assert state_lines == ["sync", "run", "run"]
        assert "lazy-sync" not in state_lines


class TestSetupCommandCwdFunctional:
    def test_uv_sync_uses_scoped_project_cwd_for_venv_creation(self, tmp_path: Path) -> None:
        """Host-side `uv sync` should create `.venv` under the current scoped project cwd."""
        workspace = tmp_path / "workspace"
        project_dir = workspace / "services" / "foo"
        project_dir.mkdir(parents=True)

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        fake_uv = bin_dir / "uv"
        fake_uv.write_text(
            "#!/bin/bash\n"
            "set -e\n"
            'cmd="${1:-}"\n'
            'if [ "$cmd" = "sync" ]; then\n'
            '    mkdir -p .venv\n'
            '    pwd > "$UV_SYNC_CWD_FILE"\n'
            "    exit 0\n"
            "fi\n"
            'echo "unexpected command: $cmd" >&2\n'
            "exit 1\n"
        )
        fake_uv.chmod(0o755)

        sync_cwd_file = tmp_path / "uv-sync-cwd.txt"
        env = os.environ.copy()
        env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
        env["UV_SYNC_CWD_FILE"] = str(sync_cwd_file)

        result = subprocess.run(
            ["bash", "-c", "uv sync"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 0, result.stderr
        assert sync_cwd_file.read_text().strip() == str(project_dir)
        assert (project_dir / ".venv").is_dir()
        assert not (workspace / ".venv").exists()


class TestGitGuardFunctional:
    def _run_guarded_git(
        self,
        tmp_path: Path,
        *,
        cwd: Path,
        args: str,
        extra_env: dict[str, str] | None = None,
        container_gitdir: Path | None = None,
        container_common_gitdir: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        workspace = tmp_path / "workspace"
        workspace.mkdir(exist_ok=True)
        outside = tmp_path / "outside"
        outside.mkdir(exist_ok=True)
        prepared_gitdir = container_gitdir or (tmp_path / "mounted-gitdir")
        prepared_common_gitdir = container_common_gitdir or (tmp_path / "mounted-common")
        prepared_gitdir.mkdir(exist_ok=True)
        prepared_common_gitdir.mkdir(exist_ok=True)
        stub_git = tmp_path / "real-git.sh"
        stub_git.write_text(
            "#!/bin/bash\n"
            "set -e\n"
            'printf "%s\\n" "$PWD" > "$GZA_STUB_GIT_CWD_FILE"\n'
            'printf "%s\\n" "$0 $*" > "$GZA_STUB_GIT_ARGS_FILE"\n'
        )
        stub_git.chmod(0o755)

        env = os.environ.copy()
        env["GZA_WORKTREE_ROOT"] = str(workspace)
        env["GZA_CONTAINER_GITDIR"] = str(prepared_gitdir)
        env["GZA_CONTAINER_COMMON_GITDIR"] = str(prepared_common_gitdir)
        env["GZA_REAL_GIT"] = str(stub_git)
        env["GZA_STUB_GIT_CWD_FILE"] = str(tmp_path / "real-git.cwd")
        env["GZA_STUB_GIT_ARGS_FILE"] = str(tmp_path / "real-git.args")
        if extra_env:
            env.update(extra_env)

        script = (
            f"{GZA_GIT_GUARD_SETUP_COMMAND}\n"
            f"cd {shlex.quote(str(cwd))}\n"
            f"/tmp/gza-shims/git {args}\n"
        )
        return subprocess.run(
            ["bash", "-lc", script],
            capture_output=True,
            text=True,
            env=env,
        )

    def test_rejects_mutating_checkout_from_outside_workspace(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(tmp_path, cwd=tmp_path / "outside", args="checkout main")

        assert result.returncode == 128
        assert "refusing mutating git 'checkout'" in result.stderr

    def test_rejects_mutating_checkout_with_separate_form_dash_c(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "outside",
            args="-c user.name=test checkout main",
        )

        assert result.returncode == 128
        assert "refusing mutating git 'checkout'" in result.stderr

    def test_rejects_explicit_git_dir_outside_prepared_metadata(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "outside",
            args=f"--git-dir={shlex.quote('/tmp/other/.git')} --work-tree={shlex.quote(str(tmp_path / 'workspace'))} checkout main",
        )

        assert result.returncode == 128
        assert "refusing explicit --git-dir outside prepared task metadata" in result.stderr

    def test_rejects_mutating_checkout_with_git_env_targeting_common_gitdir(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "workspace",
            args="checkout main",
            extra_env={
                "GIT_DIR": str(tmp_path / "mounted-common"),
                "GIT_WORK_TREE": str(tmp_path / "workspace"),
            },
        )

        assert result.returncode == 128
        assert "refusing GIT_DIR outside prepared task metadata" in result.stderr

    def test_rejects_mutating_checkout_from_common_gitdir_with_worktree_only(self, tmp_path: Path) -> None:
        common_gitdir = tmp_path / "mounted-common"
        result = self._run_guarded_git(
            tmp_path,
            cwd=common_gitdir,
            args=f"--work-tree={shlex.quote(str(tmp_path / 'workspace'))} checkout main",
            container_common_gitdir=common_gitdir,
        )

        assert result.returncode == 128
        assert "from mounted git metadata without the prepared gitdir/worktree pair" in result.stderr
        assert not (tmp_path / "real-git.args").exists()

    def test_allows_mutating_checkout_with_prepared_git_env(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "workspace",
            args="checkout main",
            extra_env={
                "GIT_DIR": str(tmp_path / "mounted-gitdir"),
                "GIT_WORK_TREE": str(tmp_path / "workspace"),
                "GIT_COMMON_DIR": str(tmp_path / "mounted-common"),
            },
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "real-git.args").read_text().strip().endswith(" checkout main")

    def test_allows_mutating_checkout_from_common_gitdir_with_prepared_cli_pair(self, tmp_path: Path) -> None:
        gitdir = tmp_path / "mounted-gitdir"
        common_gitdir = tmp_path / "mounted-common"
        result = self._run_guarded_git(
            tmp_path,
            cwd=common_gitdir,
            args=(
                f"--git-dir={shlex.quote(str(gitdir))} "
                f"--work-tree={shlex.quote(str(tmp_path / 'workspace'))} "
                "checkout main"
            ),
            container_gitdir=gitdir,
            container_common_gitdir=common_gitdir,
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "real-git.cwd").read_text().strip() == str(common_gitdir)
        assert "--git-dir=" in (tmp_path / "real-git.args").read_text()

    def test_rejects_alias_expansion_outside_workspace(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "outside",
            args="-c alias.hijack='checkout main' hijack",
        )

        assert result.returncode == 128
        assert "refusing unknown git command 'hijack'" in result.stderr
        assert not (tmp_path / "real-git.args").exists()

    def test_rejects_unknown_command_outside_workspace(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(tmp_path, cwd=tmp_path / "outside", args="totally-unknown")

        assert result.returncode == 128
        assert "refusing unknown git command 'totally-unknown'" in result.stderr
        assert not (tmp_path / "real-git.args").exists()

    def test_allows_read_only_command_with_explicit_workspace_targeting(self, tmp_path: Path) -> None:
        workspace = tmp_path / "workspace"
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "outside",
            args=f"-C {shlex.quote(str(workspace))} status --short",
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "real-git.args").read_text().strip().endswith(f" -C {workspace} status --short")

    def test_allows_mutating_command_from_workspace(self, tmp_path: Path) -> None:
        result = self._run_guarded_git(
            tmp_path,
            cwd=tmp_path / "workspace",
            args="-c user.name=test checkout main",
        )

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "real-git.cwd").read_text().strip() == str(tmp_path / "workspace")
        assert (tmp_path / "real-git.args").read_text().strip().endswith(" -c user.name=test checkout main")
