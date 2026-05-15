"""Shell-command regression tests for provider entrypoints."""

import os
import shlex
import subprocess


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
