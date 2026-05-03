"""Tests for AI code generation providers."""

import io
import json
import os
import re
import shlex
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from gza.colors import TaskStreamColors, build_rich_theme
from gza.config import ClaudeConfig, Config, ConfigError
from gza.providers import (
    ClaudeProvider,
    CodexProvider,
    DockerConfig,
    GeminiProvider,
    get_provider,
)
from gza.providers.base import (
    DOCKERFILE_TEMPLATE,
    GZA_SHIM_SETUP_COMMAND,
    _extract_startup_log_line,
    _format_command_for_log,
    _get_image_created_time,
    build_docker_cmd,
    ensure_docker_image,
    is_docker_running,
    verify_docker_credentials,
)
from gza.providers.codex import build_headless_exec_args
from gza.providers.gemini import calculate_cost
from gza.providers.output_formatter import (
    StreamOutputFormatter,
    format_runtime,
    format_token_count,
    truncate_text,
)


class TestGetProvider:
    """Tests for provider selection."""

    def test_returns_claude_by_default(self, tmp_path):
        """Default provider should be Claude."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="claude",
        )
        provider = get_provider(config)
        assert isinstance(provider, ClaudeProvider)
        assert provider.name == "Claude"

    def test_returns_gemini_when_configured(self, tmp_path):
        """Should return Gemini provider when configured."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="gemini",
        )
        provider = get_provider(config)
        assert isinstance(provider, GeminiProvider)
        assert provider.name == "Gemini"

    def test_returns_codex_when_configured(self, tmp_path):
        """Should return Codex provider when configured."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="codex",
        )
        provider = get_provider(config)
        assert isinstance(provider, CodexProvider)
        assert provider.name == "Codex"

    def test_raises_for_unknown_provider(self, tmp_path):
        """Should raise ValueError for unknown provider."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="unknown",
        )
        with pytest.raises(ValueError, match="Unknown provider: unknown"):
            get_provider(config)


class TestDockerConfig:
    """Tests for Docker configuration."""

    def test_claude_docker_config(self, tmp_path):
        """Claude should have correct Docker config."""
        from gza.providers.claude import _get_docker_config

        config = _get_docker_config("my-project-gza")

        assert config.image_name == "my-project-gza"
        assert config.npm_package == "@anthropic-ai/claude-code"
        assert config.cli_command == "claude"
        assert config.config_dir == ".claude"
        assert "ANTHROPIC_API_KEY" in config.env_vars

    def test_gemini_docker_config(self, tmp_path):
        """Gemini should have correct Docker config."""
        from gza.providers.gemini import _get_docker_config

        config = _get_docker_config("my-project-gza-gemini")

        assert config.image_name == "my-project-gza-gemini"
        assert config.npm_package == "@google/gemini-cli"
        assert config.cli_command == "gemini"
        assert config.config_dir is None  # Uses API key auth, no need to mount ~/.gemini
        assert "GEMINI_API_KEY" in config.env_vars
        assert "GOOGLE_API_KEY" in config.env_vars
        assert "GOOGLE_APPLICATION_CREDENTIALS" in config.env_vars

    def test_claude_provider_uses_per_provider_image_tag(self, tmp_path):
        """ClaudeProvider should append '-claude' to docker_image for its image tag."""
        from unittest.mock import MagicMock, patch

        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        config = Config(project_dir=tmp_path, project_name="myproj", provider="claude", use_docker=True)
        config.docker_image = "myproj-gza"

        with patch("gza.providers.claude._get_docker_config") as mock_get_cfg, \
             patch("gza.providers.claude.ensure_docker_image", return_value=True), \
             patch("gza.providers.claude.build_docker_cmd", return_value=["docker"]), \
             patch.object(provider, "_run_with_output_parsing", return_value=MagicMock(exit_code=0)):
            mock_get_cfg.return_value = MagicMock(image_name="myproj-gza-claude", cli_command="claude", env_vars=[])
            provider._run_docker(config, "prompt", tmp_path / "log.txt", tmp_path)

        mock_get_cfg.assert_called_once_with("myproj-gza-claude")

    def test_codex_provider_uses_per_provider_image_tag(self, tmp_path):
        """CodexProvider should append '-codex' to docker_image for its image tag."""
        from unittest.mock import MagicMock, patch

        from gza.config import Config
        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        config = Config(project_dir=tmp_path, project_name="myproj", provider="codex", use_docker=True)
        config.docker_image = "myproj-gza"

        with patch("gza.providers.codex._get_docker_config") as mock_get_cfg, \
             patch("gza.providers.codex.ensure_docker_image", return_value=True), \
             patch("gza.providers.codex.build_docker_cmd", return_value=["docker"]), \
             patch.object(provider, "_run_with_output_parsing", return_value=MagicMock(exit_code=0)):
            mock_get_cfg.return_value = MagicMock(image_name="myproj-gza-codex", cli_command="codex", env_vars=[])
            provider._run_docker(config, "prompt", tmp_path / "log.txt", tmp_path)

        mock_get_cfg.assert_called_once_with("myproj-gza-codex")

    def test_gemini_provider_uses_per_provider_image_tag(self, tmp_path):
        """GeminiProvider should append '-gemini' to docker_image for its image tag."""
        from unittest.mock import MagicMock, patch

        from gza.config import Config
        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        config = Config(project_dir=tmp_path, project_name="myproj", provider="gemini", use_docker=True)
        config.docker_image = "myproj-gza"

        with patch("gza.providers.gemini._get_docker_config") as mock_get_cfg, \
             patch("gza.providers.gemini.ensure_docker_image", return_value=True), \
             patch("gza.providers.gemini.build_docker_cmd", return_value=["docker", "run", "--rm", "myproj-gza-gemini"]), \
             patch.object(provider, "_run_with_output_parsing", return_value=MagicMock(exit_code=0)):
            mock_get_cfg.return_value = MagicMock(image_name="myproj-gza-gemini", cli_command="gemini", env_vars=[])
            provider._run_docker(config, "prompt", tmp_path / "log.txt", tmp_path)

        mock_get_cfg.assert_called_once_with("myproj-gza-gemini")


class TestSharedHeadlessCommands:
    """Tests for shared provider CLI command builders."""

    def test_claude_noninteractive_command_matches_headless_contract(self, tmp_path):
        """Claude should expose one shared non-interactive argv contract."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="claude",
            timeout_minutes=7,
            max_steps=23,
            model="claude-sonnet-4",
        )

        cmd = ClaudeProvider.build_noninteractive_command(config, tmp_path)

        assert cmd == [
            "timeout",
            "7m",
            "claude",
            "-p",
            "-",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            "claude-sonnet-4",
            "--allowedTools",
            "Read",
            "Write",
            "Edit",
            "Glob",
            "Grep",
            "Bash",
            "--max-turns",
            "23",
        ]

    def test_codex_noninteractive_command_matches_headless_contract(self, tmp_path):
        """Codex should expose one shared non-interactive argv contract."""
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="codex",
            timeout_minutes=9,
            max_steps=31,
            model="gpt-5.3-codex",
            reasoning_effort="high",
        )

        cmd = CodexProvider.build_noninteractive_command(config, tmp_path)

        assert cmd == [
            "timeout",
            "9m",
            "codex",
            "-c",
            "check_for_update_on_startup=false",
            "exec",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "-C",
            str(tmp_path),
            "-",
            "-m",
            "gpt-5.3-codex",
            "-c",
            "model_reasoning_effort=high",
        ]


class TestProviderCommandLogging:
    """Tests for provider command logging output."""

    def test_format_command_redacts_sensitive_values(self):
        """Should hide secret values in command log output."""
        cmd = [
            "docker", "run", "-e", "OPENAI_API_KEY=abc123", "-e",
            "GZA_DOCKER_SETUP_COMMAND=export TOKEN=foo", "gza-gza", "codex", "exec",
        ]

        rendered = _format_command_for_log(cmd)

        assert "OPENAI_API_KEY=***" in rendered
        assert "GZA_DOCKER_SETUP_COMMAND=***" in rendered
        assert "abc123" not in rendered
        assert "TOKEN=foo" not in rendered

    def test_format_command_keeps_non_sensitive_values(self):
        """Should preserve normal command arguments."""
        cmd = ["timeout", "10m", "docker", "run", "-e", "PATH=/usr/bin", "image", "claude", "-p"]

        rendered = _format_command_for_log(cmd)

        assert "PATH=/usr/bin" in rendered
        assert "docker run" in rendered

    def test_extract_startup_line_skips_json(self):
        """Should suppress JSON event lines from startup echo."""
        assert _extract_startup_log_line('{"type":"event","message":"hello"}') is None

    def test_extract_startup_line_truncates_long_text(self):
        """Should truncate very long startup output lines."""
        line = "x" * 220
        extracted = _extract_startup_log_line(line)
        assert extracted is not None
        assert extracted.endswith("...")
        assert len(extracted) == 180

    def test_run_with_logging_uses_devnull_when_no_stdin(self, tmp_path):
        """Should not inherit terminal stdin when no stdin_input is provided."""
        from gza.providers.base import Provider, RunResult

        class DummyProvider(Provider):
            @property
            def name(self) -> str:
                return "dummy"

            def check_credentials(self) -> bool:
                return True

            def verify_credentials(self, config: Config, log_file: Path | None = None) -> bool:
                return True

            def run(self, config: Config, prompt: str, log_file: Path, work_dir: Path, resume_session_id: str | None = None) -> RunResult:
                return RunResult(exit_code=0)

        provider = DummyProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter([])
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider.run_with_logging(
                cmd=["echo", "hello"],
                log_file=log_file,
                timeout_minutes=1,
                stdin_input=None,
            )

        assert mock_popen.call_args.kwargs["stdin"] == subprocess.DEVNULL


class TestSharedStreamOutputFormatter:
    """Tests for shared provider output formatting utilities."""

    def test_runtime_and_token_format_helpers(self):
        """Should format runtime and token counts consistently."""
        assert format_runtime(9) == "9s"
        assert format_runtime(65) == "1m 5s"
        assert format_token_count(500) == "500 tokens"
        assert format_token_count(12_000) == "12k tokens"
        assert truncate_text("abcdefghijklmnopqrstuvwxyz", 8) == "abcde..."

    def test_step_header_is_colorized(self):
        """Step headers should include ANSI color sequences."""
        output = io.StringIO()
        console = Console(file=output, force_terminal=True, color_system="truecolor", theme=build_rich_theme())
        formatter = StreamOutputFormatter(
            console=console,
            styles=TaskStreamColors(step_header="bold red"),
        )

        formatter.print_step_header(2, 1500, 0.1234, 65)

        rendered = output.getvalue()
        plain = re.sub(r"\x1b\[[0-9;]*m", "", rendered)
        assert "| Step 2 | 1k tokens | $0.12 | 1m 5s |" in plain
        assert "\x1b[" in rendered

    def test_step_header_disables_auto_highlighter(self):
        """Step headers must not be recolored by Rich's repr.* highlighter.

        Regression: ``repr.number`` / ``repr.str`` rules in the active Rich
        theme used to apply to digits and currency fragments mid-line, leaving
        the header partially recolored. ``highlight=False`` on the step-header
        print suppresses the auto-highlighter for this line only; other
        formatter output (agent messages, tool events) still runs through it.
        """
        from rich.theme import Theme as RichTheme

        # Rich theme with a loud, distinctive color on ``repr.number``. If the
        # auto-highlighter runs, the digits in the step header will be emitted
        # in this color and it'll show up in the rendered ANSI bytes.
        rich_theme = RichTheme({"repr.number": "bright_red"})
        output = io.StringIO()
        console = Console(
            file=output,
            force_terminal=True,
            color_system="truecolor",
            theme=rich_theme,
        )
        formatter = StreamOutputFormatter(
            console=console,
            styles=TaskStreamColors(step_header="bold blue"),
        )

        formatter.print_step_header(2, 1500, 0.1234, 65)

        rendered = output.getvalue()

        # bright_red = ANSI 91 (or a truecolor equivalent for bright red).
        # The cleanest signal is that the step header line renders as a single
        # contiguous style run — i.e. no style switches between characters of
        # "| Step 2 | 1k tokens | $0.12 | 1m 5s |". Grab the style runs and
        # confirm only one distinct non-reset style appears.
        style_runs = re.findall(r"\x1b\[[0-9;]*m", rendered)
        non_reset_runs = {run for run in style_runs if run not in ("\x1b[0m",)}
        assert len(non_reset_runs) == 1, (
            f"step header should render in a single style run, got {non_reset_runs!r} "
            f"in {rendered!r}"
        )

    def test_non_header_lines_still_use_highlighter(self):
        """Only step headers disable the highlighter; other lines keep it.

        This guards against someone copy-pasting ``highlight=False`` onto every
        formatter method. Agent messages, tool events, errors, and todos should
        still go through the normal Rich path so the active theme's repr.*
        styles can apply to numbers/paths/URLs in those lines.
        """

        # The signature of ``print_step_header`` should be the only place in
        # StreamOutputFormatter that explicitly passes highlight=False — other
        # methods rely on Console's default behavior.
        import gza.providers.output_formatter as mod
        src = Path(mod.__file__).read_text()
        # There should be exactly one ``highlight=False`` in the formatter.
        assert src.count("highlight=False") == 1, (
            "Expected exactly one highlight=False (in print_step_header); "
            "other formatter methods should retain the auto-highlighter."
        )

    def test_turn_header_is_alias_for_step_header(self):
        """print_turn_header should produce the same output as print_step_header."""
        output1 = io.StringIO()
        output2 = io.StringIO()
        console1 = Console(file=output1, force_terminal=True, color_system="truecolor", theme=build_rich_theme())
        console2 = Console(file=output2, force_terminal=True, color_system="truecolor", theme=build_rich_theme())
        styles = TaskStreamColors(step_header="bold red")
        formatter1 = StreamOutputFormatter(console=console1, styles=styles)
        formatter2 = StreamOutputFormatter(console=console2, styles=styles)

        formatter1.print_step_header(3, 1500, 0.05, 10)
        formatter2.print_turn_header(3, 1500, 0.05, 10)

        plain1 = re.sub(r"\x1b\[[0-9;]*m", "", output1.getvalue())
        plain2 = re.sub(r"\x1b\[[0-9;]*m", "", output2.getvalue())
        assert "| Step 3 |" in plain1
        assert plain1 == plain2

    def test_key_event_lines_are_colorized(self):
        """Tool, assistant, and error lines should all be colorized."""
        output = io.StringIO()
        console = Console(file=output, force_terminal=True, color_system="truecolor", theme=build_rich_theme())
        formatter = StreamOutputFormatter(
            console=console,
            styles=TaskStreamColors(
                tool_use="bold yellow",
                assistant_text="bold green",
                error="bold red",
            ),
        )

        formatter.print_tool_event("Bash", "ls -la")
        formatter.print_agent_message("Working on it")
        formatter.print_error("Error: failed")

        rendered = output.getvalue()
        assert "→ Bash ls -la" in rendered
        assert "Working on it" in rendered
        assert "Error: failed" in rendered
        assert "\x1b[" in rendered


class TestBuildDockerCmd:
    """Tests for Docker command building."""

    def test_basic_command_structure(self, tmp_path):
        """Should build correct basic command structure."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)

        assert cmd[0] == "timeout"
        assert cmd[1] == "10m"
        assert "docker" in cmd
        assert "run" in cmd
        assert "--rm" in cmd
        assert cmd[-1] == "test-image"

    def test_mounts_git_dir_for_worktree(self, tmp_path):
        """Should mount host .git dir when work_dir is a git worktree."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        # Simulate a git worktree: .git is a file pointing to a gitdir
        fake_git_dir = tmp_path / "repo" / ".git" / "worktrees" / "my-task"
        fake_git_dir.mkdir(parents=True)
        main_git_dir = tmp_path / "repo" / ".git"

        worktree_dir = tmp_path / "worktree"
        worktree_dir.mkdir()
        (worktree_dir / ".git").write_text(f"gitdir: {fake_git_dir}\n")

        cmd = build_docker_cmd(docker_config, worktree_dir, timeout_minutes=10)
        assert f"{main_git_dir}:{main_git_dir}" in " ".join(cmd)

    def test_no_git_mount_for_regular_repo(self, tmp_path):
        """Should not add extra mount when .git is a directory (regular repo)."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        # .git is a directory, not a worktree
        (tmp_path / ".git").mkdir()

        cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)
        # Only the workspace mount and config mount should have -v
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        mount_args = [cmd[i + 1] for i in v_indices]
        assert all("/workspace" in m or ".testconfig" in m for m in mount_args)

    def test_mounts_workspace(self, tmp_path):
        """Should mount workspace directory."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)

        # Find the workspace mount
        mount_idx = cmd.index("-v")
        mount_arg = cmd[mount_idx + 1]
        assert mount_arg == f"{tmp_path}:/workspace"

    def test_mounts_workspace_venv_tmpfs(self, tmp_path):
        """Should shadow /workspace/.venv with a writable tmpfs mount."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        volume_mounts = [cmd[i + 1] for i in v_indices]
        tmpfs_idx = cmd.index("--tmpfs")
        assert cmd[tmpfs_idx + 1] == "/workspace/.venv:rw,exec,mode=1777"
        assert volume_mounts[0] == f"{tmp_path}:/workspace"

    def test_mounts_config_dir(self, tmp_path):
        """Should mount provider config directory."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".myconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)

        # Find the config mount after the workspace mount.
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        assert len(v_indices) >= 2
        config_mount = cmd[v_indices[1] + 1]
        assert ".myconfig" in config_mount
        assert "/home/gza/.myconfig" in config_mount

    def test_passes_env_vars_when_set(self, tmp_path):
        """Should pass environment variables when they are set."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=["MY_API_KEY", "OTHER_KEY"],
        )

        with patch.dict(os.environ, {"MY_API_KEY": "secret123"}):
            cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)

        # Should have -e MY_API_KEY but not -e OTHER_KEY
        e_indices = [i for i, x in enumerate(cmd) if x == "-e"]
        env_vars_passed = [cmd[i + 1] for i in e_indices]
        assert "MY_API_KEY" in env_vars_passed
        assert "OTHER_KEY" not in env_vars_passed

    def test_skips_env_vars_when_not_set(self, tmp_path):
        """Should not pass unset provider env vars."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=["UNSET_VAR"],
        )

        # Ensure the var is not set
        with patch.dict(os.environ, {}, clear=True):
            # Need to preserve PATH etc for the test to work
            cmd = build_docker_cmd(docker_config, tmp_path, timeout_minutes=10)

        e_indices = [i for i, x in enumerate(cmd) if x == "-e"]
        env_values = [cmd[i + 1] for i in e_indices]
        assert "UNSET_VAR" not in env_values

    def test_mounts_custom_volumes(self, tmp_path):
        """Should mount custom docker volumes."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        custom_volumes = [
            "/host/datasets:/datasets:ro",
            "/host/models:/models",
        ]

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_volumes=custom_volumes
        )

        # Verify custom volumes are present
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        volume_mounts = [cmd[i + 1] for i in v_indices]

        assert "/host/datasets:/datasets:ro" in volume_mounts
        assert "/host/models:/models" in volume_mounts

    def test_custom_volumes_added_after_standard_mounts(self, tmp_path):
        """Custom volumes should be added after workspace and config mounts."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        custom_volumes = ["/custom:/custom"]

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_volumes=custom_volumes
        )

        # Find all -v flags and their mounts
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        volume_mounts = [cmd[i + 1] for i in v_indices]

        # Workspace and config should come before custom mounts.
        assert len(volume_mounts) >= 3
        assert volume_mounts[0] == f"{tmp_path}:/workspace"
        assert ".testconfig" in volume_mounts[1]
        assert "/custom:/custom" in volume_mounts
        tmpfs_idx = cmd.index("--tmpfs")
        assert cmd[tmpfs_idx + 1] == "/workspace/.venv:rw,exec,mode=1777"

    def test_custom_volumes_with_none(self, tmp_path):
        """Should handle docker_volumes=None gracefully."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_volumes=None
        )

        # Should only have workspace and config bind mounts.
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        assert len(v_indices) == 2
        tmpfs_idx = cmd.index("--tmpfs")
        assert cmd[tmpfs_idx + 1] == "/workspace/.venv:rw,exec,mode=1777"

    def test_custom_volumes_with_empty_list(self, tmp_path):
        """Should handle docker_volumes=[] gracefully."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_volumes=[]
        )

        # Should only have workspace and config bind mounts.
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        assert len(v_indices) == 2
        tmpfs_idx = cmd.index("--tmpfs")
        assert cmd[tmpfs_idx + 1] == "/workspace/.venv:rw,exec,mode=1777"

    def test_passes_setup_command_as_env_var(self, tmp_path):
        """Should pass GZA_DOCKER_SETUP_COMMAND when docker_setup_command is set."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_setup_command="uv sync --project /workspace",
        )

        e_indices = [i for i, x in enumerate(cmd) if x == "-e"]
        env_values = [cmd[i + 1] for i in e_indices]
        setup_value = next(v for v in env_values if v.startswith("GZA_DOCKER_SETUP_COMMAND="))
        setup_cmd = setup_value.split("=", 1)[1]
        assert "uv sync --project /workspace" in setup_cmd
        assert "mkdir -p /tmp/gza-shims" in setup_cmd

    def test_no_setup_command_env_var_when_empty(self, tmp_path):
        """Should still pass default shim setup when docker_setup_command is empty."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_setup_command="",
        )

        e_indices = [i for i, x in enumerate(cmd) if x == "-e"]
        env_values = [cmd[i + 1] for i in e_indices]
        setup_value = next(v for v in env_values if v.startswith("GZA_DOCKER_SETUP_COMMAND="))
        setup_cmd = setup_value.split("=", 1)[1]
        assert setup_cmd == GZA_SHIM_SETUP_COMMAND.strip()

    def test_setup_command_placed_before_image_name(self, tmp_path):
        """GZA_DOCKER_SETUP_COMMAND should be added before image name."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_setup_command="make setup",
        )

        image_idx = cmd.index("test-image")
        e_indices = [i for i, x in enumerate(cmd) if x == "-e"]
        setup_cmd_idx = next(
            i for i in e_indices
            if cmd[i + 1].startswith("GZA_DOCKER_SETUP_COMMAND=")
        )
        assert setup_cmd_idx < image_idx

    def test_default_setup_command_installs_gza_shim(self, tmp_path):
        """Default setup command should install and expose a container gza shim."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        cmd = build_docker_cmd(
            docker_config,
            tmp_path,
            timeout_minutes=10,
            docker_setup_command="",
        )

        setup_value = next(
            cmd[i + 1]
            for i, token in enumerate(cmd)
            if token == "-e" and cmd[i + 1].startswith("GZA_DOCKER_SETUP_COMMAND=")
        )
        setup_cmd = setup_value.split("=", 1)[1]
        assert "cat > /tmp/gza-shims/gza <<'EOF'" in setup_cmd
        assert "if [ -x /workspace/bin/gza ]; then" in setup_cmd
        assert 'exec /workspace/bin/gza "$@"' in setup_cmd
        assert 'path_without_shim="${PATH#/tmp/gza-shims:}"' in setup_cmd
        assert 'gza_path="$(PATH="$path_without_shim" command -v gza 2>/dev/null || true)"' in setup_cmd
        assert 'exec "$gza_path" "$@"' in setup_cmd
        assert "Supported options:" in setup_cmd
        assert "Set docker_setup_command in gza.yaml to install gza into PATH" in setup_cmd
        assert 'exec uv run --directory /workspace gza "$@"' not in setup_cmd
        assert 'export PATH="/tmp/gza-shims:/workspace/bin:$PATH"' in setup_cmd

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


class TestDockerfileTemplate:
    """Tests for Dockerfile generation."""

    def test_template_includes_npm_package(self):
        """Template should include the npm package."""
        content = DOCKERFILE_TEMPLATE.format(
            npm_package="@anthropic-ai/claude-code",
            cli_command="claude",
        )
        assert "npm install -g @anthropic-ai/claude-code" in content

    def test_template_includes_cli_command(self):
        """Template should include the CLI command."""
        content = DOCKERFILE_TEMPLATE.format(
            npm_package="@google/gemini-cli",
            cli_command="gemini",
        )
        assert 'CMD ["gemini"]' in content

    def test_template_creates_gza_user(self):
        """Template should create gza user for isolation."""
        content = DOCKERFILE_TEMPLATE.format(
            npm_package="@test/cli",
            cli_command="test",
        )
        assert "useradd" in content
        assert "gza" in content
        assert "USER gza" in content

    def test_template_includes_ripgrep(self):
        """Template should include ripgrep for agent search tooling."""
        content = DOCKERFILE_TEMPLATE.format(
            npm_package="@test/cli",
            cli_command="test",
        )
        assert "ripgrep" in content

    def test_checked_in_provider_dockerfiles_include_ripgrep(self):
        """Checked-in Claude/Codex Dockerfiles should include ripgrep."""
        repo_root = Path(__file__).resolve().parents[1]
        for relpath in ("etc/Dockerfile.claude", "etc/Dockerfile.codex"):
            dockerfile = repo_root / relpath
            assert dockerfile.exists()
            assert "ripgrep" in dockerfile.read_text()


class TestGeminiCostCalculation:
    """Tests for Gemini cost calculation."""

    def test_gemini_25_pro_pricing(self):
        """Should use correct pricing for gemini-2.5-pro."""
        # 1M input tokens at $1.25, 1M output tokens at $10.00
        cost = calculate_cost("gemini-2.5-pro", 1_000_000, 1_000_000)
        assert cost == pytest.approx(11.25, rel=0.01)

    def test_gemini_25_flash_pricing(self):
        """Should use correct pricing for gemini-2.5-flash."""
        # 1M input at $0.15, 1M output at $0.60
        cost = calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
        assert cost == pytest.approx(0.75, rel=0.01)

    def test_unknown_model_uses_default(self):
        """Unknown models should use default (expensive) pricing."""
        cost = calculate_cost("gemini-99-ultra", 1_000_000, 1_000_000)
        # Default is same as 2.5-pro
        expected = calculate_cost("gemini-2.5-pro", 1_000_000, 1_000_000)
        assert cost == expected

    def test_small_token_counts(self):
        """Should handle small token counts correctly."""
        # 1000 input tokens, 500 output tokens with 2.5-pro pricing
        cost = calculate_cost("gemini-2.5-pro", 1000, 500)
        # 1000 * 1.25/1M + 500 * 10/1M = 0.00125 + 0.005 = 0.00625
        assert cost == pytest.approx(0.00625, rel=0.01)

    def test_zero_tokens(self):
        """Should handle zero tokens."""
        cost = calculate_cost("gemini-2.5-pro", 0, 0)
        assert cost == 0.0


class TestGeminiOutputParsing:
    """Tests for Gemini stream-json parsing."""

    def test_tool_use_before_new_assistant_message_creates_new_step(self, tmp_path):
        """Tool events after a new user message should not attach to prior assistant step."""
        import json

        provider = GeminiProvider()
        log_file = tmp_path / "gemini.log"

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "first"}) + "\n",
            json.dumps({"type": "message", "role": "user", "content": "next"}) + "\n",
            json.dumps({"type": "tool_use", "id": "tool_1", "tool_name": "Bash", "tool_input": {"command": "echo hi"}}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "second"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5, "tool_calls": 1}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="",
            )

        run_steps = result._accumulated_data["run_step_events"]
        assert len(run_steps) == 2
        assert run_steps[0]["message_text"] == "first"
        assert run_steps[0]["substeps"] == []
        assert run_steps[1]["message_text"] == "second"
        assert len(run_steps[1]["substeps"]) == 1
        assert run_steps[1]["substeps"][0]["payload"]["tool_name"] == "Bash"
        assert result.num_steps_computed == 2
        assert result.num_steps_reported == 2
        assert result.num_turns_reported == 1

    def test_maps_tool_lifecycle_events_to_substeps(self, tmp_path):
        """Gemini tool lifecycle events should map to tool_* substeps on current step."""
        import json

        provider = GeminiProvider()
        log_file = tmp_path / "gemini.log"

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "working"}) + "\n",
            json.dumps({"type": "tool_use", "id": "call_1", "tool_name": "Bash", "tool_input": {"command": "ls"}}) + "\n",
            json.dumps({"type": "tool_output", "call_id": "call_1", "output": "ok"}) + "\n",
            json.dumps({"type": "tool_retry", "call_id": "call_2", "retry_of_call_id": "call_1"}) + "\n",
            json.dumps({"type": "tool_error", "call_id": "call_2", "error": "failed"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5, "tool_calls": 2}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="",
            )

        run_steps = result._accumulated_data["run_step_events"]
        assert len(run_steps) == 1
        assert run_steps[0]["message_text"] == "working"
        assert [s["type"] for s in run_steps[0]["substeps"]] == [
            "tool_call",
            "tool_output",
            "tool_retry",
            "tool_error",
        ]
        assert [s["legacy_event_id"] for s in run_steps[0]["substeps"]] == [
            "T1.2",
            "T1.3",
            "T1.4",
            "T1.5",
        ]

    def test_pre_message_tool_creates_synthetic_step_with_summary(self, tmp_path):
        """First tool event should create synthetic step until assistant text arrives."""
        import json

        provider = GeminiProvider()
        log_file = tmp_path / "gemini.log"

        json_lines = [
            json.dumps({"type": "tool_use", "id": "call_1", "tool_name": "Bash", "tool_input": {"command": "echo hi"}}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "done"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5, "tool_calls": 1}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="",
            )

        run_steps = result._accumulated_data["run_step_events"]
        assert len(run_steps) == 1
        assert run_steps[0]["message_text"] == "done"
        assert run_steps[0]["legacy_event_id"] == "T1.1"
        assert run_steps[0]["summary"] is None
        assert run_steps[0]["substeps"][0]["legacy_event_id"] == "T1.2"


class TestCredentialChecks:
    """Tests for credential checking logic."""

    def test_claude_checks_config_dir(self, tmp_path):
        """Claude should check for ~/.claude directory."""
        provider = ClaudeProvider()

        with patch.object(Path, "home", return_value=tmp_path):
            # No config dir, no env var
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            # Create config dir
            (tmp_path / ".claude").mkdir()
            assert provider.check_credentials() is True

    def test_claude_checks_api_key(self):
        """Claude should check for ANTHROPIC_API_KEY."""
        provider = ClaudeProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test"}):
                assert provider.check_credentials() is True

    def test_gemini_checks_gemini_api_key(self):
        """Gemini should check for GEMINI_API_KEY."""
        provider = GeminiProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}):
                assert provider.check_credentials() is True

    def test_gemini_checks_google_api_key(self):
        """Gemini should check for GOOGLE_API_KEY."""
        provider = GeminiProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {"GOOGLE_API_KEY": "test-key"}):
                assert provider.check_credentials() is True

    def test_gemini_checks_application_credentials(self):
        """Gemini should check for GOOGLE_APPLICATION_CREDENTIALS."""
        provider = GeminiProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "/path/to/creds.json"}):
                assert provider.check_credentials() is True

    def test_gemini_checks_config_dir(self, tmp_path):
        """Gemini should check for ~/.gemini directory."""
        provider = GeminiProvider()

        with patch.object(Path, "home", return_value=tmp_path):
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            (tmp_path / ".gemini").mkdir()
            assert provider.check_credentials() is True


class TestProviderRunMethods:
    """Tests for provider run method routing."""

    def test_claude_routes_to_docker_when_enabled(self, tmp_path):
        """Claude should route to Docker when use_docker is True."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="claude",
            use_docker=True,
        )
        provider = ClaudeProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_docker.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_docker.assert_called_once()
                mock_direct.assert_not_called()

    def test_claude_routes_to_direct_when_disabled(self, tmp_path):
        """Claude should route to direct when use_docker is False."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="claude",
            use_docker=False,
        )
        provider = ClaudeProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_direct.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_direct.assert_called_once()
                mock_docker.assert_not_called()

    def test_claude_direct_interactive_uses_true_interactive_args_and_stdin_prompt(self, tmp_path):
        """Foreground interactive Claude direct mode should not use print-mode stream-json flags."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="claude",
            use_docker=False,
            timeout_minutes=5,
        )
        provider = ClaudeProvider()

        with patch.object(provider, "_run_interactive_command", return_value=MagicMock(exit_code=0)) as mock_run:
            provider._run_direct_interactive(
                config,
                "interactive prompt",
                tmp_path / "log.txt",
                tmp_path,
            )

        cmd = mock_run.call_args[0][0]
        assert "-p" not in cmd
        assert "--output-format" not in cmd
        assert "--max-turns" in cmd
        assert "interactive prompt" not in cmd
        assert mock_run.call_args.kwargs["stdin_input"] == "interactive prompt"
        assert mock_run.call_args.kwargs["timeout_minutes"] == 5

    def test_claude_docker_interactive_uses_true_interactive_args_and_stdin_prompt(self, tmp_path):
        """Foreground interactive Claude Docker mode should not use print-mode stream-json flags."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="claude",
            use_docker=True,
            timeout_minutes=5,
        )
        config.docker_image = "test-image"
        provider = ClaudeProvider()

        with (
            patch("gza.providers.claude.ensure_docker_image", return_value=True),
            patch("gza.providers.claude.build_docker_cmd", return_value=["docker", "run"]) as mock_build_docker_cmd,
            patch.object(provider, "_run_interactive_command", return_value=MagicMock(exit_code=0)) as mock_run,
        ):
            provider._run_docker_interactive(
                config,
                "interactive prompt",
                tmp_path / "log.txt",
                tmp_path,
            )

        assert mock_build_docker_cmd.call_args.kwargs["interactive"] is True
        cmd = mock_run.call_args[0][0]
        assert "-p" not in cmd
        assert "--output-format" not in cmd
        assert "--max-turns" in cmd
        assert "interactive prompt" not in cmd
        assert mock_run.call_args.kwargs["stdin_input"] == "interactive prompt"
        assert mock_run.call_args.kwargs["timeout_minutes"] == 5

    def test_gemini_routes_to_docker_when_enabled(self, tmp_path):
        """Gemini should route to Docker when use_docker is True."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="gemini",
            use_docker=True,
        )
        provider = GeminiProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_docker.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_docker.assert_called_once()
                mock_direct.assert_not_called()

    def test_gemini_routes_to_direct_when_disabled(self, tmp_path):
        """Gemini should route to direct when use_docker is False."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="gemini",
            use_docker=False,
        )
        provider = GeminiProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_direct.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_direct.assert_called_once()
                mock_docker.assert_not_called()


class TestCodexGitRepoCheckBypass:
    """Tests that Codex execution bypasses git repo checks."""

    def test_codex_docker_includes_skip_git_repo_check(self, tmp_path):
        """Docker codex exec command should include --skip-git-repo-check."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=True,
            timeout_minutes=5,
        )
        config.docker_image = "test-codex-image"
        config.docker_volumes = []
        config.docker_setup_command = ""

        with patch("gza.providers.codex.ensure_docker_image", return_value=True), \
             patch("gza.providers.codex.build_docker_cmd", return_value=["timeout", "5m", "docker", "run", "--rm", "test-codex-image"]), \
             patch.object(provider, "_run_with_output_parsing", return_value=MagicMock(exit_code=0)) as mock_run_parse:
            provider._run_docker(config, "test prompt", tmp_path / "log.txt", tmp_path)

        cmd = mock_run_parse.call_args[0][0]
        assert "--skip-git-repo-check" in cmd

    def test_codex_direct_includes_skip_git_repo_check(self, tmp_path):
        """Direct codex exec command should include --skip-git-repo-check."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=False,
            timeout_minutes=5,
        )

        with patch.object(provider, "_run_with_output_parsing", return_value=MagicMock(exit_code=0)) as mock_run_parse:
            provider._run_direct(config, "test prompt", tmp_path / "log.txt", tmp_path)

        cmd = mock_run_parse.call_args[0][0]
        assert "--skip-git-repo-check" in cmd


class TestDockerDaemonCheck:
    """Tests for Docker daemon availability checks."""

    def test_is_docker_running_returns_true_when_daemon_available(self):
        """Should return True when docker info succeeds."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            assert is_docker_running() is True
            mock_run.assert_called_once()
            # Verify it called docker info
            call_args = mock_run.call_args[0][0]
            assert call_args == ["docker", "info"]

    def test_is_docker_running_returns_false_when_daemon_not_available(self):
        """Should return False when docker info fails."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            assert is_docker_running() is False

    def test_is_docker_running_returns_false_on_timeout(self):
        """Should return False when docker info times out."""
        import subprocess
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="docker", timeout=5)
            assert is_docker_running() is False

    def test_is_docker_running_returns_false_when_docker_not_installed(self):
        """Should return False when docker command not found."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError()
            assert is_docker_running() is False

    def test_verify_docker_credentials_fails_when_docker_not_running(self, capsys):
        """Should fail immediately with message when Docker daemon is not running."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=False):
            result = verify_docker_credentials(
                docker_config=docker_config,
                version_cmd=["testcli", "--version"],
                error_patterns=["auth error"],
                error_message="Auth failed",
            )

        assert result.ok is False
        assert result.failure_reason == "INFRASTRUCTURE_ERROR"
        assert result.message == "Preflight failed: Docker daemon is not running"
        captured = capsys.readouterr()
        assert "Docker daemon is not running" in captured.out
        assert "--no-docker" in captured.out

    def test_verify_docker_credentials_proceeds_when_docker_running(self):
        """Should proceed with credential check when Docker is running."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=".testconfig",
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=True):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=0,
                    stdout="v1.0.0",
                    stderr="",
                )
                result = verify_docker_credentials(
                    docker_config=docker_config,
                    version_cmd=["testcli", "--version"],
                    error_patterns=["auth error"],
                    error_message="Auth failed",
                )

        assert result.ok is True
        # Verify docker run was called (not just docker info)
        call_args = mock_run.call_args[0][0]
        assert "docker" in call_args
        assert "run" in call_args


class TestClaudeErrorTypeExtraction:
    """Tests for Claude provider extracting error_type from result."""

    def test_extracts_max_turns_error_from_result(self, tmp_path):
        """Should set error_type='max_steps' when result has subtype error_max_turns."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude's stream-json output with error_max_turns
        json_lines = [
            json.dumps({"type": "assistant", "message": {"content": []}}) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "error_max_turns",
                "num_turns": 60,
                "total_cost_usd": 1.35,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0  # Claude returns 0 even on max turns
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.error_type == "max_steps"
        assert result.num_turns_reported == 60
        assert result.cost_usd == 1.35
        assert result.exit_code == 0  # Preserves actual exit code

    def test_no_error_type_on_success(self, tmp_path):
        """Should not set error_type when result is successful."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate successful Claude output
        json_lines = [
            json.dumps({"type": "assistant", "message": {"content": []}}) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 5,
                "total_cost_usd": 0.10,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.error_type is None
        assert result.num_turns_reported == 5
        assert result.exit_code == 0

    def test_stores_computed_turn_count(self, tmp_path):
        """Should store num_turns_computed from unique assistant message IDs."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude output with 2 distinct assistant message IDs
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_002", "content": [], "usage": {"input_tokens": 200, "output_tokens": 80}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 3,  # Provider reports 3 but we computed 2 unique messages
                "total_cost_usd": 0.10,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.num_turns_reported == 3
        assert result.num_turns_computed == 2

    def test_computed_turn_count_deduplicates_same_message_id(self, tmp_path):
        """Should deduplicate repeated assistant message IDs in computed count."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Same message ID appears twice - should only count once
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 10, "output_tokens": 5}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.num_turns_reported == 1
        assert result.num_turns_computed == 1  # Deduplicated

    def test_tool_use_before_next_message_boundary_dedupes_repeated_message_id(self, tmp_path):
        """Tool substeps should stay on the current step and dedupe repeated chunks by call id."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_001",
                    "content": [{"type": "text", "text": "first"}],
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                },
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_002",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool_1",
                            "name": "Bash",
                            "input": {"command": "echo hi"},
                        }
                    ],
                    "usage": {"input_tokens": 200, "output_tokens": 80},
                },
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_002",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool_1",
                            "name": "Bash",
                            "input": {"command": "echo hi"},
                        },
                        {"type": "text", "text": "second"},
                    ],
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
            }) + "\n",
            json.dumps(
                {
                    "type": "result",
                    "subtype": "success",
                    "num_turns": 2,
                    "total_cost_usd": 0.10,
                }
            ) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        run_steps = result._accumulated_data["run_step_events"]
        assert len(run_steps) == 2
        assert run_steps[0]["message_text"] == "first"
        assert run_steps[0]["substeps"] == []
        assert run_steps[1]["message_text"] == "second"
        assert run_steps[1]["legacy_turn_id"] == "T2"
        assert len(run_steps[1]["substeps"]) == 1
        assert run_steps[1]["substeps"][0]["call_id"] == "tool_1"
        assert run_steps[1]["substeps"][0]["payload"]["tool_name"] == "Bash"

    def test_stores_token_counts_from_usage(self, tmp_path):
        """Should accumulate input and output token counts from assistant messages."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate two assistant messages with different token types
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_001",
                    "content": [],
                    "usage": {
                        "input_tokens": 100,
                        "cache_creation_input_tokens": 50,
                        "cache_read_input_tokens": 20,
                        "output_tokens": 75,
                    },
                },
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_002",
                    "content": [],
                    "usage": {
                        "input_tokens": 200,
                        "cache_creation_input_tokens": 0,
                        "cache_read_input_tokens": 10,
                        "output_tokens": 100,
                    },
                },
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 2,
                "total_cost_usd": 0.10,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        # input_tokens = (100 + 50 + 20) + (200 + 0 + 10) = 170 + 210 = 380
        assert result.input_tokens == 380
        # output_tokens = 75 + 100 = 175
        assert result.output_tokens == 175


class TestClaudeStepMapping:
    """Tests for Claude message-step/substep mapping."""

    def test_maps_tool_use_and_tool_result_to_lifecycle_substeps(self, tmp_path):
        """Claude content items should map to tool_call/tool_output/tool_error types."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_1",
                    "usage": {"input_tokens": 100, "output_tokens": 10},
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "tool_1",
                            "name": "Bash",
                            "input": {"command": "ls"},
                        },
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool_1",
                            "content": "ok",
                            "is_error": False,
                        },
                        {
                            "type": "tool_result",
                            "tool_use_id": "tool_1",
                            "content": "failed",
                            "is_error": True,
                        },
                        {"type": "text", "text": "done"},
                    ],
                },
            }) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 7, "total_cost_usd": 0.1}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        run_steps = result._accumulated_data["run_step_events"]
        assert len(run_steps) == 1
        assert run_steps[0]["legacy_event_id"] == "T1.1"
        assert [s["type"] for s in run_steps[0]["substeps"]] == [
            "tool_call",
            "tool_output",
            "tool_error",
        ]
        assert [s["legacy_event_id"] for s in run_steps[0]["substeps"]] == ["T1.2", "T1.3", "T1.4"]
        assert result.num_turns_reported == 7
        assert result.num_steps_computed == 1
        assert result.num_steps_reported == 1

    def test_sets_zero_step_metrics_when_no_assistant_message(self, tmp_path):
        """Claude should persist explicit zero step metrics for runs with no step events."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "result", "subtype": "error_max_turns", "num_turns": 0, "total_cost_usd": 0.0}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.num_steps_computed == 0
        assert result.num_steps_reported == 0

    def test_captures_session_id_from_system_init_event(self, tmp_path):
        """Should capture session_id from system/init event early in stream."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "system", "subtype": "init", "session_id": "ses_early_abc123", "tools": []}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_1", "content": [], "usage": {}}}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.01}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.session_id == "ses_early_abc123"

    def test_on_session_id_callback_called_from_system_init(self, tmp_path):
        """on_session_id callback should be invoked as soon as system/init event is parsed."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        captured: list[str] = []

        json_lines = [
            json.dumps({"type": "system", "subtype": "init", "session_id": "ses_callback_xyz", "tools": []}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_1", "content": [], "usage": {}}}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.01}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_session_id=captured.append,
            )

        assert captured == ["ses_callback_xyz"]

    def test_on_session_id_callback_called_only_once(self, tmp_path):
        """on_session_id callback should only be called once even if session_id appears in both system and result events."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        captured: list[str] = []

        json_lines = [
            json.dumps({"type": "system", "subtype": "init", "session_id": "ses_once", "tools": []}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.0, "session_id": "ses_once"}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_session_id=captured.append,
            )

        assert captured == ["ses_once"]

    def test_interactive_run_uses_true_interactive_pty_stdio_and_parses_callbacks(self, tmp_path):
        """Foreground interactive runs should keep PTY stdio without print-mode flags."""
        import json

        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude")
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        config.chat_text_display_length = 80
        config.model = ""
        config.claude.args = []

        captured_sessions: list[str] = []
        captured_steps: list[int] = []

        json_lines = [
            json.dumps({"type": "system", "subtype": "init", "session_id": "sess-interactive-42"}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_1"}}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_2"}}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "result": "# Review\\n\\nVerdict: APPROVED"}) + "\n",
        ]

        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.side_effect = [None, 0]

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(10, 11)),
            patch("gza.providers.claude.select.select", side_effect=[([10], [], []), ([10], [], [])]),
            patch("gza.providers.claude.os.read", side_effect=["".join(json_lines).encode("utf-8"), b""]),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=False),
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process) as mock_popen,
        ):
            result = provider.run(
                config,
                prompt="Interactive callback test",
                log_file=log_file,
                work_dir=work_dir,
                on_session_id=captured_sessions.append,
                on_step_count=captured_steps.append,
                interactive=True,
            )

        mock_popen.assert_called_once()
        popen_args = mock_popen.call_args
        cmd = popen_args.args[0]
        assert "-p" not in cmd
        assert "--output-format" not in cmd
        assert "--max-turns" in cmd
        assert "Interactive callback test" not in cmd
        assert popen_args.kwargs["stdin"] == 11
        assert popen_args.kwargs["stdout"] == 11
        assert popen_args.kwargs["stderr"] == 11
        assert result.exit_code == 0
        assert result.session_id == "sess-interactive-42"
        assert captured_sessions == ["sess-interactive-42"]
        assert captured_steps == [1, 2]
        assert '"type": "result"' in log_file.read_text()

    def test_interactive_launch_log_omits_prompt_for_direct_mode(self, tmp_path):
        """Interactive direct launch should keep prompt out of argv and launch log."""
        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive-direct.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude")
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20
        prompt = "Sensitive project context should never appear in logs"

        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.return_value = 0

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(20, 21)),
            patch("gza.providers.claude.select.select", side_effect=[([20], [], [])]),
            patch("gza.providers.claude.os.read", side_effect=[b""]),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=False),
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process) as mock_popen,
        ):
            provider.run(
                config,
                prompt=prompt,
                log_file=log_file,
                work_dir=work_dir,
                interactive=True,
            )

        launched_cmd = mock_popen.call_args.args[0]
        assert prompt not in launched_cmd
        assert "-p" not in launched_cmd
        assert "--output-format" not in launched_cmd
        assert "--max-turns" in launched_cmd
        log_text = log_file.read_text()
        assert '"subtype": "interactive_launch"' in log_text
        assert prompt not in log_text

    def test_interactive_launch_log_omits_prompt_for_docker_mode(self, tmp_path):
        """Interactive Docker launch should keep prompt out of argv and launch log."""
        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive-docker.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude", use_docker=True)
        config.timeout_minutes = 10
        config.max_steps = 20
        config.docker_image = "test-image"
        prompt = "Sensitive injected review context should be redacted"

        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.return_value = 0

        with (
            patch("gza.providers.claude.ensure_docker_image", return_value=True),
            patch("gza.providers.claude.build_docker_cmd", return_value=["docker", "run"]),
            patch("gza.providers.claude.pty.openpty", return_value=(30, 31)),
            patch("gza.providers.claude.select.select", side_effect=[([30], [], [])]),
            patch("gza.providers.claude.os.read", side_effect=[b""]),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=False),
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process) as mock_popen,
        ):
            provider.run(
                config,
                prompt=prompt,
                log_file=log_file,
                work_dir=work_dir,
                interactive=True,
            )

        launched_cmd = mock_popen.call_args.args[0]
        assert prompt not in launched_cmd
        assert "-p" not in launched_cmd
        assert "--output-format" not in launched_cmd
        assert "--max-turns" in launched_cmd
        log_text = log_file.read_text()
        assert '"subtype": "interactive_launch"' in log_text
        assert prompt not in log_text

    def test_interactive_run_large_prompt_is_seeded_via_stdin_not_argv(self, tmp_path):
        """Large prompts should be sent via PTY stdin and excluded from process argv."""
        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive-large.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude")
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        large_prompt = "X" * 200_000
        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.side_effect = [None, 0]

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(40, 41)),
            patch("gza.providers.claude.select.select", side_effect=[([40], [], []), ([40], [], [])]),
            patch("gza.providers.claude.os.read", side_effect=[b'{"type":"result","result":"ok"}\n', b""]),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=False),
            patch("gza.providers.claude.os.write") as mock_write,
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process) as mock_popen,
        ):
            result = provider.run(
                config,
                prompt=large_prompt,
                log_file=log_file,
                work_dir=work_dir,
                interactive=True,
            )

        launched_cmd = mock_popen.call_args.args[0]
        assert large_prompt not in launched_cmd
        assert result.exit_code == 0
        assert mock_write.call_count >= 1
        seeded_bytes = mock_write.call_args_list[0].args[1]
        assert large_prompt.encode("utf-8") in seeded_bytes
        assert seeded_bytes.endswith(b"\n")

    def test_interactive_run_keeps_stdin_connected_after_prompt_seed(self, tmp_path):
        """Interactive run should forward live stdin input to Claude after seeding."""
        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive-stdin-forward.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude")
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        prompt = "seed prompt"
        live_input = b"follow-up from user\n"

        mock_process = MagicMock()
        mock_process.wait.return_value = None
        mock_process.returncode = 0
        mock_process.poll.side_effect = [None, None, 0]

        def _fake_os_read(fd: int, _size: int) -> bytes:
            if fd == 50:
                if not _fake_os_read.master_reads:
                    _fake_os_read.master_reads += 1
                    return b'{"type":"result","result":"ok"}\n'
                return b""
            if fd == 60:
                return live_input
            return b""

        _fake_os_read.master_reads = 0  # type: ignore[attr-defined]

        class _FakeStdin:
            def fileno(self) -> int:
                return 60

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(50, 51)),
            patch("gza.providers.claude.select.select", side_effect=[([50], [], []), ([60], [], []), ([50], [], [])]),
            patch("gza.providers.claude.os.read", side_effect=_fake_os_read),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.isatty", return_value=True),
            patch("gza.providers.claude.os.write") as mock_write,
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process),
            patch("gza.providers.claude.sys.stdin", new=_FakeStdin()),
        ):
            result = provider.run(
                config,
                prompt=prompt,
                log_file=log_file,
                work_dir=work_dir,
                interactive=True,
            )

        assert result.exit_code == 0
        writes = [call.args[1] for call in mock_write.call_args_list]
        assert any(prompt.encode("utf-8") in written for written in writes)
        assert live_input in writes

    def test_interactive_prompt_seed_failure_aborts_run_and_logs_outcome(self, tmp_path):
        """Prompt seed failure should abort the interactive run instead of continuing silently."""
        from gza.config import Config
        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "interactive-seed-failure.log"
        work_dir = tmp_path / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        config = Config(project_dir=tmp_path, project_name="test-project", provider="claude")
        config.use_docker = False
        config.timeout_minutes = 10
        config.max_steps = 20

        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.return_value = None

        with (
            patch("gza.providers.claude.pty.openpty", return_value=(70, 71)),
            patch("gza.providers.claude.os.close"),
            patch("gza.providers.claude.os.write", side_effect=OSError("pty write failed")),
            patch("gza.providers.claude.subprocess.Popen", return_value=mock_process),
            patch("gza.providers.claude.sys.stderr"),
        ):
            result = provider.run(
                config,
                prompt="seed me",
                log_file=log_file,
                work_dir=work_dir,
                interactive=True,
            )

        assert result.exit_code == 1
        assert result.error_type == "startup_failed"
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once_with(timeout=2)
        log_text = log_file.read_text()
        assert "Failed to seed interactive stdin prompt; aborting interactive run." in log_text
        assert '"subtype": "outcome"' in log_text

    def test_session_id_captured_from_result_when_no_system_init(self, tmp_path):
        """session_id should still be captured from result event when no system/init event is present."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        captured: list[str] = []

        json_lines = [
            json.dumps({"type": "assistant", "message": {"id": "msg_1", "content": [], "usage": {}}}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.0, "session_id": "ses_from_result"}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_session_id=captured.append,
            )

        assert result.session_id == "ses_from_result"
        assert captured == ["ses_from_result"]


class TestClaudeToolLogging:
    """Tests for enhanced Claude provider tool logging."""

    def test_logs_glob_pattern(self, tmp_path, capsys):
        """Should log Glob tool with pattern details."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude's stream-json output with Glob tool call
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "Glob",
                            "input": {"pattern": "**/*.py"}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ Glob **/*.py" in captured.out

    def test_logs_todowrite_summary(self, tmp_path, capsys):
        """Should log TodoWrite tool with todos summary."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude's stream-json output with TodoWrite tool call
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "TodoWrite",
                            "input": {
                                "todos": [
                                    {"content": "Task 1", "status": "pending", "activeForm": "Working on task 1"},
                                    {"content": "Task 2", "status": "in_progress", "activeForm": "Working on task 2"},
                                    {"content": "Task 3", "status": "completed", "activeForm": "Completed task 3"},
                                ]
                            }
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ TodoWrite 3 todos (pending: 1, in_progress: 1, completed: 1)" in captured.out

    def test_logs_todowrite_empty_list(self, tmp_path, capsys):
        """Should log TodoWrite with empty todos list."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude's stream-json output with empty TodoWrite
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "TodoWrite",
                            "input": {"todos": []}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ TodoWrite 0 todos" in captured.out

    def test_logs_file_path_tools(self, tmp_path, capsys):
        """Should still log file path for file-related tools."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Simulate Claude's stream-json output with Read tool call
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "Read",
                            "input": {"file_path": "/workspace/test.py"}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ Read /workspace/test.py" in captured.out

    def test_logs_generic_tool_with_string_params(self, tmp_path, capsys):
        """Should log generic tools with their string parameters."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "Skill",
                            "input": {"skill": "commit", "args": "-m fix"}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ Skill skill=commit args=-m fix" in captured.out

    def test_logs_generic_tool_truncates_long_strings(self, tmp_path, capsys):
        """Should truncate string parameters longer than 60 chars."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        long_value = "a" * 80

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "SomeTool",
                            "input": {"param": long_value}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ SomeTool param=" + "a" * 57 + "..." in captured.out

    def test_logs_generic_tool_escapes_newlines(self, tmp_path, capsys):
        """Should escape newlines in string parameters."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "SomeTool",
                            "input": {"param": "line1\nline2"}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ SomeTool param=line1\\nline2" in captured.out

    def test_logs_generic_tool_shows_list_length(self, tmp_path, capsys):
        """Should show list lengths for list parameters."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "SomeTool",
                            "input": {"items": [1, 2, 3]}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ SomeTool items=list[3]" in captured.out

    def test_logs_generic_tool_shows_dict_indicator(self, tmp_path, capsys):
        """Should show {...} for dict parameters."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "SomeTool",
                            "input": {"config": {"key": "value"}}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ SomeTool config={...}" in captured.out

    def test_logs_generic_tool_with_no_params(self, tmp_path, capsys):
        """Should log tool name only when tool_input is empty."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "SomeTool",
                            "input": {}
                        }
                    ]
                }
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ SomeTool\n" in captured.out


class TestStepTimestampLogging:
    """Tests for timestamp logging at the start of each turn in the log file."""

    def test_logs_timestamp_to_log_file_on_new_step(self, tmp_path):
        """Should write a step timestamp line to the log file when a new step starts."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content

    def test_logs_timestamp_for_each_step(self, tmp_path):
        """Should write a timestamp line for each new step."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_002", "content": [], "usage": {"input_tokens": 200, "output_tokens": 80}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 2,
                "total_cost_usd": 0.10,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content
        assert "--- Step 2 at " in log_content

    def test_timestamp_format_matches_expected_pattern(self, tmp_path):
        """Timestamp should match YYYY-MM-DD HH:MM:SS TZ format."""
        import json
        import re

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        # Pattern: "--- Step 1 at 2026-02-23 12:34:56 PST ---"
        pattern = r"--- Step 1 at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        assert re.search(pattern, log_content), f"Expected timestamp pattern not found in: {log_content!r}"

    def test_no_duplicate_timestamps_for_same_message_id(self, tmp_path):
        """Should not log extra timestamps when the same message ID is repeated."""
        import json

        from gza.providers.claude import ClaudeProvider

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        # Same message ID repeated
        json_lines = [
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 100, "output_tokens": 50}},
            }) + "\n",
            json.dumps({
                "type": "assistant",
                "message": {"id": "msg_001", "content": [], "usage": {"input_tokens": 10, "output_tokens": 5}},
            }) + "\n",
            json.dumps({
                "type": "result",
                "subtype": "success",
                "num_turns": 1,
                "total_cost_usd": 0.05,
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        # Only one Step 1 timestamp, no Step 2
        assert log_content.count("--- Step ") == 1
        assert "--- Step 2 at " not in log_content


class TestCodexStepTimestampLogging:
    """Tests for step header and timestamp logging in the Codex provider."""

    def test_logs_timestamp_to_log_file_on_turn_start(self, tmp_path):
        """Should write a step timestamp line to the log file at turn.started."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "done"},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content

    def test_logs_timestamp_for_each_turn(self, tmp_path):
        """Should write a timestamp line for each turn.started event."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "first"},
            }) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "second"},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content
        assert "--- Step 2 at " in log_content

    def test_step_timestamp_format_matches_expected_pattern(self, tmp_path):
        """Codex step timestamp should match YYYY-MM-DD HH:MM:SS TZ format."""
        import json
        import re

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "hello"}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        pattern = r"--- Step 1 at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        assert re.search(pattern, log_content), f"Expected timestamp pattern not found in: {log_content!r}"

    def test_uses_step_header_not_turn_header(self, tmp_path, capsys):
        """Codex live output should say Step N not Turn N."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "hello"}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "Step 1" in captured.out
        assert "Turn" not in captured.out


class TestGeminiStepHeaderAndTimestampLogging:
    """Tests for step header and timestamp logging in the Gemini provider."""

    def test_prints_step_header_on_new_assistant_message(self, tmp_path, capsys):
        """Should print a step header when a new assistant message step begins."""
        import json

        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "Hello world"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 100, "output_tokens": 50}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "--yolo"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        captured = capsys.readouterr()
        assert "Step 1" in captured.out

    def test_logs_timestamp_to_log_file_on_new_step(self, tmp_path):
        """Should write a step timestamp line to the log file when a new assistant step starts."""
        import json

        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "Hello world"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 100, "output_tokens": 50}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "--yolo"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content

    def test_logs_timestamp_for_each_step(self, tmp_path):
        """Should write a timestamp line for each new assistant step."""
        import json

        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "message", "role": "user", "content": "go"}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "step one"}) + "\n",
            json.dumps({"type": "message", "role": "user", "content": "more"}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "step two"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 200, "output_tokens": 100}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "--yolo"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        log_content = log_file.read_text()
        assert "--- Step 1 at " in log_content
        assert "--- Step 2 at " in log_content

    def test_step_timestamp_format_matches_expected_pattern(self, tmp_path):
        """Gemini step timestamp should match YYYY-MM-DD HH:MM:SS TZ format."""
        import json
        import re

        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "hi"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "--yolo"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        log_content = log_file.read_text()
        pattern = r"--- Step 1 at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        assert re.search(pattern, log_content), f"Expected timestamp pattern not found in: {log_content!r}"

    def test_no_step_header_for_existing_step_text_update(self, tmp_path, capsys):
        """Should not print extra step header when existing step's text is updated."""
        import json

        from gza.providers.gemini import GeminiProvider

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        # Tool use creates a step, then assistant message updates it (no new step header)
        json_lines = [
            json.dumps({"type": "tool_use", "tool_name": "Bash", "tool_input": {"command": "ls"}, "id": "c1"}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "done"}) + "\n",
            json.dumps({"type": "result", "stats": {"input_tokens": 10, "output_tokens": 5}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "--yolo"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        log_content = log_file.read_text()
        # Only one step was truly new (from tool_use); assistant message updated it
        assert log_content.count("--- Step ") == 0


class TestFormatToolParam:
    """Tests for the _format_tool_param helper function."""

    def test_formats_short_string(self):
        """Should return short strings unchanged (except newline escaping)."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param("hello") == "hello"

    def test_truncates_long_string(self):
        """Should truncate strings longer than 60 chars."""
        from gza.providers.claude import _format_tool_param
        value = "x" * 80
        result = _format_tool_param(value)
        assert result == "x" * 57 + "..."
        assert len(result) == 60

    def test_escapes_newlines(self):
        """Should escape newlines in strings."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param("line1\nline2") == "line1\\nline2"

    def test_escapes_carriage_returns(self):
        """Should escape carriage returns in strings."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param("line1\rline2") == "line1\\rline2"

    def test_formats_list(self):
        """Should show list length."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param([1, 2, 3]) == "list[3]"
        assert _format_tool_param([]) == "list[0]"

    def test_formats_dict(self):
        """Should show {...} for dicts."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param({"key": "value"}) == "{...}"
        assert _format_tool_param({}) == "{...}"

    def test_formats_bool(self):
        """Should convert booleans to string."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param(True) == "True"
        assert _format_tool_param(False) == "False"

    def test_formats_int(self):
        """Should convert integers to string."""
        from gza.providers.claude import _format_tool_param
        assert _format_tool_param(42) == "42"


class TestGetImageCreatedTime:
    """Tests for Docker image timestamp retrieval."""

    def test_returns_timestamp_when_image_exists(self):
        """Should return Unix timestamp when image exists."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="2025-01-08T10:30:00.123456789Z\n",
            )
            result = _get_image_created_time("test-image")

        assert result is not None
        assert isinstance(result, float)
        # Verify the timestamp is reasonable (after 2025-01-01)
        assert result > 1735689600  # 2025-01-01 00:00:00 UTC

    def test_returns_none_when_image_not_found(self):
        """Should return None when image doesn't exist."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="")
            result = _get_image_created_time("nonexistent-image")

        assert result is None

    def test_handles_timestamps_without_nanoseconds(self):
        """Should handle timestamps without fractional seconds."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="2025-01-08T10:30:00Z\n",
            )
            result = _get_image_created_time("test-image")

        assert result is not None

    def test_returns_none_on_invalid_timestamp(self):
        """Should return None for unparseable timestamps."""
        with patch("gza.providers.base.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="not-a-timestamp\n",
            )
            result = _get_image_created_time("test-image")

        assert result is None


class TestEnsureDockerImage:
    """Tests for Docker image build logic."""

    def test_returns_false_when_daemon_not_running(self, tmp_path):
        """Should short-circuit with a clear error when the Docker daemon is down."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=False):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is False
        mock_run.assert_not_called()

    def test_returns_true_when_image_up_to_date(self, tmp_path):
        """Should return True without building when image is newer than Dockerfile."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        # Create Dockerfile
        etc_dir = tmp_path / "etc"
        etc_dir.mkdir()
        dockerfile = etc_dir / "Dockerfile.testcli"
        dockerfile.write_text("FROM node:20-slim")

        # Mock image as newer than Dockerfile
        dockerfile_mtime = dockerfile.stat().st_mtime
        image_time = dockerfile_mtime + 100  # Image created after Dockerfile

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=image_time):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is True
        # subprocess.run should NOT be called (no build needed)
        mock_run.assert_not_called()

    def test_rebuilds_when_dockerfile_newer(self, tmp_path):
        """Should rebuild image when Dockerfile is newer than image."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        # Create Dockerfile
        etc_dir = tmp_path / "etc"
        etc_dir.mkdir()
        dockerfile = etc_dir / "Dockerfile.testcli"
        dockerfile.write_text("FROM node:20-slim")

        # Mock image as older than Dockerfile
        dockerfile_mtime = dockerfile.stat().st_mtime
        image_time = dockerfile_mtime - 100  # Image created before Dockerfile

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=image_time):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is True
        # Verify docker build was called
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert "docker" in call_args
        assert "build" in call_args

    def test_builds_when_image_not_exists(self, tmp_path):
        """Should build image when it doesn't exist."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=None):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is True
        mock_run.assert_called_once()

    def test_preserves_custom_dockerfile(self, tmp_path):
        """Should not overwrite existing custom Dockerfile."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        # Create custom Dockerfile with extra content
        etc_dir = tmp_path / "etc"
        etc_dir.mkdir()
        dockerfile = etc_dir / "Dockerfile.testcli"
        custom_content = "FROM python:3.12\nRUN pip install pytest"
        dockerfile.write_text(custom_content)

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=None):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                ensure_docker_image(docker_config, tmp_path)

        # Dockerfile should still have custom content
        assert dockerfile.read_text() == custom_content

    def test_generates_dockerfile_when_missing(self, tmp_path):
        """Should generate default Dockerfile when none exists."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=None):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                ensure_docker_image(docker_config, tmp_path)

        dockerfile = tmp_path / "etc" / "Dockerfile.testcli"
        assert dockerfile.exists()
        content = dockerfile.read_text()
        assert "@test/cli" in content
        assert "testcli" in content

    def test_returns_false_on_build_failure(self, tmp_path):
        """Should return False when docker build fails."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@test/cli",
            cli_command="testcli",
            config_dir=None,
            env_vars=[],
        )

        with patch("gza.providers.base.is_docker_running", return_value=True), \
             patch("gza.providers.base._get_image_created_time", return_value=None):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=1)
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is False


class TestCodexProvider:
    """Tests for Codex provider."""

    def test_codex_docker_config_api_key_takes_precedence_over_oauth(self):
        """API key should take precedence over OAuth when both are present."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=True):
            with patch.dict(os.environ, {"CODEX_API_KEY": "sk-test"}, clear=False):
                config = _get_docker_config("my-project-gza")
                assert config.image_name == "my-project-gza"
                assert config.npm_package == "@openai/codex"
                assert config.cli_command == "codex"
                # API key wins: no .codex mount, env var passed instead
                assert config.config_dir is None
                assert "CODEX_API_KEY" in config.env_vars

    def test_codex_docker_config_with_oauth_fallback(self):
        """OAuth should be used when no API key is configured."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=True):
            with patch.dict(os.environ, {}, clear=True):
                config = _get_docker_config("my-project-gza")
                assert config.image_name == "my-project-gza"
                assert config.npm_package == "@openai/codex"
                assert config.cli_command == "codex"
                assert config.config_dir == ".codex"
                assert config.env_vars == []

    def test_codex_docker_config_with_codex_api_key(self):
        """Codex should use CODEX_API_KEY (canonical) when no OAuth credentials exist."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=False):
            with patch.dict(os.environ, {"CODEX_API_KEY": "sk-test"}, clear=True):
                config = _get_docker_config("my-project-gza")
                assert config.image_name == "my-project-gza"
                assert config.npm_package == "@openai/codex"
                assert config.cli_command == "codex"
                assert config.config_dir is None
                assert "CODEX_API_KEY" in config.env_vars
                assert "OPENAI_API_KEY" not in config.env_vars

    def test_codex_docker_config_with_openai_api_key_alias(self):
        """Codex should accept OPENAI_API_KEY as a backward-compatible alias."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=False):
            with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-openai-test"}, clear=True):
                config = _get_docker_config("my-project-gza")
                assert config.config_dir is None
                assert "OPENAI_API_KEY" in config.env_vars
                assert "CODEX_API_KEY" not in config.env_vars

    def test_codex_docker_config_both_api_keys_set(self):
        """When both CODEX_API_KEY and OPENAI_API_KEY are set, both are passed through."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=False):
            with patch.dict(
                os.environ,
                {"CODEX_API_KEY": "sk-codex", "OPENAI_API_KEY": "sk-openai"},
                clear=True,
            ):
                config = _get_docker_config("my-project-gza")
                assert config.config_dir is None
                assert "CODEX_API_KEY" in config.env_vars
                assert "OPENAI_API_KEY" in config.env_vars

    def test_codex_docker_config_no_credentials(self):
        """When no credentials exist, default to API key mode with CODEX_API_KEY hint."""
        from gza.providers.codex import _get_docker_config

        with patch("gza.providers.codex._has_codex_oauth", return_value=False):
            with patch.dict(os.environ, {}, clear=True):
                config = _get_docker_config("my-project-gza")
                assert config.config_dir is None
                assert "CODEX_API_KEY" in config.env_vars

    def test_check_credentials_with_codex_api_key(self):
        """Codex should check for CODEX_API_KEY (canonical)."""
        provider = CodexProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            with patch.dict(os.environ, {"CODEX_API_KEY": "sk-test"}, clear=True):
                assert provider.check_credentials() is True

    def test_check_credentials_with_openai_api_key_alias(self):
        """Codex should accept OPENAI_API_KEY as a backward-compatible alias."""
        provider = CodexProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {"OPENAI_API_KEY": "sk-openai"}, clear=True):
                assert provider.check_credentials() is True

    def test_check_credentials_with_config_dir(self, tmp_path):
        """Codex should accept OAuth (~/.codex directory) as a fallback credential."""
        provider = CodexProvider()

        with patch.dict(os.environ, {}, clear=True):
            with patch.object(Path, "home", return_value=tmp_path):
                assert provider.check_credentials() is False

                (tmp_path / ".codex").mkdir()
                assert provider.check_credentials() is True

    def test_check_credentials_api_key_takes_precedence_over_oauth(self, tmp_path):
        """API key check succeeds even when OAuth is present (API key takes precedence)."""
        provider = CodexProvider()

        (tmp_path / ".codex").mkdir()
        with patch.object(Path, "home", return_value=tmp_path):
            # With API key set, returns True via the API key path
            with patch.dict(os.environ, {"CODEX_API_KEY": "sk-test"}, clear=True):
                assert provider.check_credentials() is True
            # OAuth alone also returns True (as a valid fallback)
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is True

    def test_credential_setup_hint_mentions_canonical_and_alias(self):
        """credential_setup_hint should mention CODEX_API_KEY as canonical and OPENAI_API_KEY as alias."""
        provider = CodexProvider()
        hint = provider.credential_setup_hint
        assert "CODEX_API_KEY" in hint
        assert "OPENAI_API_KEY" in hint

    def test_routes_to_docker_when_enabled(self, tmp_path):
        """Codex should route to Docker when use_docker is True."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=True,
        )
        provider = CodexProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_docker.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_docker.assert_called_once()
                mock_direct.assert_not_called()

    def test_routes_to_direct_when_disabled(self, tmp_path):
        """Codex should route to direct when use_docker is False."""
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=False,
        )
        provider = CodexProvider()

        with patch.object(provider, "_run_docker") as mock_docker:
            with patch.object(provider, "_run_direct") as mock_direct:
                mock_direct.return_value = MagicMock(exit_code=0)
                provider.run(config, "test prompt", tmp_path / "log.txt", tmp_path)

                mock_direct.assert_called_once()
                mock_docker.assert_not_called()

    def test_resume_direct_uses_session_id_and_stdin_prompt(self, tmp_path):
        """Resume should target the stored session id and pass prompt via stdin."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=False,
            timeout_minutes=10,
            model="gpt-5.3-codex",
        )
        log_file = tmp_path / "log.txt"

        with patch.object(provider, "_run_with_output_parsing") as mock_run:
            mock_run.return_value = MagicMock(exit_code=0)
            provider._run_direct(
                config=config,
                prompt="resume prompt",
                log_file=log_file,
                work_dir=tmp_path,
                resume_session_id="thread_123",
            )

        cmd = mock_run.call_args.args[0]
        assert cmd == [
            "timeout",
            "10m",
            "codex",
            "-c",
            "check_for_update_on_startup=false",
            "exec",
            "resume",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "thread_123",
            "-",
            "-m",
            "gpt-5.3-codex",
        ]
        assert mock_run.call_args.kwargs["cwd"] == tmp_path
        assert mock_run.call_args.kwargs["stdin_input"] == "resume prompt"

    def test_resume_docker_uses_session_id_and_stdin_prompt(self, tmp_path):
        """Docker resume should target the stored session id and pass prompt via stdin."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=True,
            timeout_minutes=10,
            model="gpt-5.3-codex",
        )
        log_file = tmp_path / "log.txt"
        docker_base_cmd = ["timeout", "10m", "docker", "run", "--rm", "image"]

        with patch("gza.providers.codex._get_docker_config") as mock_get_docker_config, \
             patch("gza.providers.codex.ensure_docker_image", return_value=True), \
             patch("gza.providers.codex.build_docker_cmd", return_value=docker_base_cmd.copy()), \
             patch.object(provider, "_run_with_output_parsing") as mock_run:
            mock_get_docker_config.return_value = MagicMock()
            mock_run.return_value = MagicMock(exit_code=0)

            provider._run_docker(
                config=config,
                prompt="resume prompt",
                log_file=log_file,
                work_dir=tmp_path,
                resume_session_id="thread_456",
            )

        cmd = mock_run.call_args.args[0]
        assert cmd == docker_base_cmd + [
            "codex",
            "-c",
            "check_for_update_on_startup=false",
            "exec",
            "resume",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "thread_456",
            "-",
            "-m",
            "gpt-5.3-codex",
        ]
        assert mock_run.call_args.kwargs["stdin_input"] == "resume prompt"

    def test_direct_exec_includes_reasoning_effort_override(self, tmp_path):
        """Direct codex exec should pass model_reasoning_effort via config override."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=False,
            timeout_minutes=10,
            model="gpt-5.3-codex",
            reasoning_effort="high",
        )
        log_file = tmp_path / "log.txt"

        with patch.object(provider, "_run_with_output_parsing") as mock_run:
            mock_run.return_value = MagicMock(exit_code=0)
            provider._run_direct(
                config=config,
                prompt="do work",
                log_file=log_file,
                work_dir=tmp_path,
            )

        cmd = mock_run.call_args.args[0]
        assert "-c" in cmd
        assert "model_reasoning_effort=high" in cmd

    def test_resume_exec_includes_reasoning_effort_override(self, tmp_path):
        """Codex resume exec should pass model_reasoning_effort via config override."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test",
            provider="codex",
            use_docker=False,
            timeout_minutes=10,
            model="gpt-5.3-codex",
            reasoning_effort="medium",
        )
        log_file = tmp_path / "log.txt"

        with patch.object(provider, "_run_with_output_parsing") as mock_run:
            mock_run.return_value = MagicMock(exit_code=0)
            provider._run_direct(
                config=config,
                prompt="resume prompt",
                log_file=log_file,
                work_dir=tmp_path,
                resume_session_id="thread_123",
            )

        cmd = mock_run.call_args.args[0]
        assert "-c" in cmd
        assert "model_reasoning_effort=medium" in cmd


class TestCodexCostCalculation:
    """Tests for Codex cost calculation."""

    def test_default_pricing(self):
        """Should use default pricing for unknown models."""
        from gza.providers.codex import calculate_cost

        # 1M input tokens at $2.50, 1M output tokens at $10.00
        cost = calculate_cost(1_000_000, 1_000_000, "unknown-model")
        assert cost == pytest.approx(12.50, rel=0.01)

    def test_gpt_5_2_codex_pricing(self):
        """Should use correct pricing for gpt-5.2-codex."""
        from gza.providers.codex import calculate_cost

        # 1M input at $2.50, 1M output at $10.00
        cost = calculate_cost(1_000_000, 1_000_000, "gpt-5.2-codex")
        assert cost == pytest.approx(12.50, rel=0.01)

    def test_o3_pricing(self):
        """Should use correct pricing for o3."""
        from gza.providers.codex import calculate_cost

        # 1M input at $10.00, 1M output at $40.00
        cost = calculate_cost(1_000_000, 1_000_000, "o3")
        assert cost == pytest.approx(50.00, rel=0.01)

    def test_small_token_counts(self):
        """Should handle small token counts correctly."""
        from gza.providers.codex import calculate_cost

        # 1000 input tokens, 500 output tokens with default pricing
        cost = calculate_cost(1000, 500, "gpt-5.2-codex")
        # 1000 * 2.50/1M + 500 * 10/1M = 0.0025 + 0.005 = 0.0075
        assert cost == pytest.approx(0.0075, rel=0.01)

    def test_zero_tokens(self):
        """Should handle zero tokens."""
        from gza.providers.codex import calculate_cost

        cost = calculate_cost(0, 0, "gpt-5.2-codex")
        assert cost == 0.0


class TestCodexOutputParsing:
    """Tests for Codex output parsing."""

    def test_parses_turn_events(self, tmp_path):
        """Should parse turn.started and turn.completed events."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        # Simulate Codex's JSON output
        json_lines = [
            json.dumps({"type": "thread.started", "thread_id": "thread_123"}) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "turn.completed",
                "usage": {"input_tokens": 1000, "output_tokens": 500, "cached_input_tokens": 200}
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.num_turns_reported == 1
        assert result.input_tokens == 1000
        assert result.output_tokens == 500
        assert result.session_id == "thread_123"

    def test_parses_command_execution(self, tmp_path, capsys):
        """Should log command execution items."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "command_execution", "command": "ls -la"}
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "→ Bash ls -la" in captured.out

    def test_parses_agent_messages(self, tmp_path, capsys):
        """Should log agent message items."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "I will help you with that task."}
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "I will help you with that task." in captured.out

    def test_tracks_computed_turns_from_agent_messages(self, tmp_path):
        """Should track computed turn count based on agent_message items."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "step one"},
            }) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "step two"},
            }) + "\n",
            json.dumps({
                "type": "turn.completed",
                "usage": {"input_tokens": 100, "output_tokens": 50},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.num_turns_reported == 1
        assert result.num_turns_computed == 2
        assert result.num_steps_computed == 2

    def test_logs_tool_call_under_agent_message_step(self, tmp_path, capsys):
        """Tool calls should appear under the preceding agent_message step."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "Let me check"},
            }) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "command_execution", "command": "ls -la"},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert "Step 1" in captured.out
        assert "→ Bash ls -la" in captured.out
        # No [S1.1] prefix — tool calls are substeps of the logical step
        assert "[S" not in captured.out

    def test_does_not_print_startup_non_json_line_twice(self, tmp_path, capsys):
        """Startup line should be shown once, not duplicated by parser fallback."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        lines = [
            "Reading prompt from stdin...\n",
            json.dumps({"type": "turn.started"}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "resume", "--json", "--last"],
                log_file=log_file,
                timeout_minutes=30,
            )

        captured = capsys.readouterr()
        assert captured.out.count("Reading prompt from stdin...") == 1

    def test_tracks_max_steps_exceeded(self, tmp_path):
        """Should track when max_steps is exceeded based on message-step count."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        # Simulate exceeding max_steps (set to 2)
        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "step 1"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "step 2"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "step 3"}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                max_steps=2,  # Set max_steps to 2
            )

        assert result.num_steps_computed == 3
        assert result.error_type == "max_steps"

    def test_new_turn_tool_substep_does_not_attach_to_previous_step(self, tmp_path):
        """Tool items before the next message should attach to the new turn's step."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "first"}}) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "command_execution", "command": "echo before"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "second"}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        run_steps = result._accumulated_data["run_step_events"]
        # Each agent_message creates a logical step; tool calls before a
        # message get their own "Pre-message tool activity" step.
        assert len(run_steps) == 3
        assert run_steps[0]["message_text"] == "first"
        assert run_steps[0]["substeps"] == []
        # Pre-message tool activity step
        assert run_steps[1]["summary"] == "Pre-message tool activity"
        assert len(run_steps[1]["substeps"]) == 1
        assert run_steps[1]["substeps"][0]["payload"]["command"] == "echo before"
        # Second agent message step
        assert run_steps[2]["message_text"] == "second"
        assert run_steps[2]["legacy_turn_id"] == "T2"

    def test_maps_command_execution_to_tool_call_and_output(self, tmp_path):
        """Codex command_execution should emit lifecycle substeps and deterministic legacy IDs."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {
                        "id": "cmd_1",
                        "type": "command_execution",
                        "command": "ls -la",
                        "aggregated_output": "file.txt",
                        "exit_code": 0,
                    },
                }
            ) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "done"}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        run_steps = result._accumulated_data["run_step_events"]
        # Pre-message tool activity step + agent_message step
        assert len(run_steps) == 2
        assert run_steps[0]["summary"] == "Pre-message tool activity"
        assert [s["type"] for s in run_steps[0]["substeps"]] == ["tool_call", "tool_output"]
        assert run_steps[1]["message_text"] == "done"

    def test_uses_shared_formatter_for_turn_tool_message_and_error(self, tmp_path):
        """Codex parser should route key output lines through shared formatter."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        mock_formatter = MagicMock()

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "command_execution", "command": "echo hi"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "done"}}) + "\n",
            json.dumps({"type": "turn.error", "message": "bad turn"}) + "\n",
        ]

        with patch("gza.providers.codex.StreamOutputFormatter", return_value=mock_formatter):
            with patch("gza.providers.base.subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter(json_lines)
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                provider._run_with_output_parsing(
                    cmd=["codex", "exec", "--json", "-"],
                    log_file=log_file,
                    timeout_minutes=30,
                )

        mock_formatter.print_step_header.assert_called_once()
        mock_formatter.print_tool_event.assert_called()
        mock_formatter.print_agent_message.assert_called()
        mock_formatter.print_error.assert_called()

    def test_parses_usage_from_non_turn_completed_event(self, tmp_path):
        """Should capture usage from completion events beyond turn.completed."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "response.completed",
                "usage": {"input_tokens": 321, "output_tokens": 123, "cached_input_tokens": 7},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.input_tokens == 321
        assert result.output_tokens == 123
        assert result.cost_usd is not None

    def test_estimates_tokens_when_usage_missing(self, tmp_path):
        """Should estimate tokens/cost when no usage event is emitted."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "command_execution", "command": "echo hi", "aggregated_output": "hello world"},
            }) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "working on it"},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                stdin_input="run tests and summarize results",
            )

        assert result.input_tokens is not None
        assert result.input_tokens > 0
        assert result.output_tokens is not None
        assert result.output_tokens > 0
        assert result.cost_usd is not None
        assert result.tokens_estimated is True
        assert result.cost_estimated is True

    def test_step_headers_estimate_mid_turn_then_result_uses_real_usage(self, tmp_path):
        """Step headers should estimate before turn usage, while RunResult uses real usage."""
        import json

        from gza.providers.codex import CodexProvider, calculate_cost

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        mock_formatter = MagicMock()

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "a" * 4000},
                }
            ) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "b" * 4000},
                }
            ) + "\n",
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {"input_tokens": 300, "output_tokens": 100},
                }
            ) + "\n",
        ]

        with patch("gza.providers.codex.StreamOutputFormatter", return_value=mock_formatter):
            with patch("gza.providers.base.subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter(json_lines)
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                result = provider._run_with_output_parsing(
                    cmd=["codex", "exec", "--json", "-"],
                    log_file=log_file,
                    timeout_minutes=30,
                )

        assert mock_formatter.print_step_header.call_count == 2
        step_1_args = mock_formatter.print_step_header.call_args_list[0].args
        step_2_args = mock_formatter.print_step_header.call_args_list[1].args

        # print_step_header(step_num, total_tokens, cost, elapsed_seconds, ...)
        assert step_1_args[1] > 0
        assert step_1_args[2] > 0
        assert step_2_args[1] > step_1_args[1]
        assert step_2_args[2] >= step_1_args[2]

        assert result.input_tokens == 300
        assert result.output_tokens == 100
        assert result.tokens_estimated is False
        assert result.cost_estimated is False
        assert result.cost_usd == calculate_cost(300, 100, "")

    def test_step_headers_keep_cumulative_estimates_across_turns_without_usage(self, tmp_path):
        """When usage has not arrived, step header estimates should keep rolling across turns."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        mock_formatter = MagicMock()

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "a" * 4000},
                }
            ) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "b" * 4000},
                }
            ) + "\n",
        ]

        with patch("gza.providers.codex.StreamOutputFormatter", return_value=mock_formatter):
            with patch("gza.providers.base.subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter(json_lines)
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                provider._run_with_output_parsing(
                    cmd=["codex", "exec", "--json", "-"],
                    log_file=log_file,
                    timeout_minutes=30,
                )

        assert mock_formatter.print_step_header.call_count == 2
        step_1_args = mock_formatter.print_step_header.call_args_list[0].args
        step_2_args = mock_formatter.print_step_header.call_args_list[1].args

        # print_step_header(step_num, total_tokens, cost, elapsed_seconds, ...)
        assert step_1_args[1] > 0
        assert step_2_args[1] >= step_1_args[1]
        assert step_2_args[2] >= step_1_args[2]

    def test_step_headers_rebase_estimates_after_turn_usage_arrives(self, tmp_path):
        """Later turns should start from real totals and estimate only post-usage deltas."""
        import json

        from gza.providers.codex import CodexProvider, calculate_cost

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        mock_formatter = MagicMock()

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "a" * 4000},
                }
            ) + "\n",
            json.dumps(
                {
                    "type": "turn.completed",
                    "usage": {"input_tokens": 300, "output_tokens": 100},
                }
            ) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "b" * 4000},
                }
            ) + "\n",
        ]

        with patch("gza.providers.codex.StreamOutputFormatter", return_value=mock_formatter):
            with patch("gza.providers.base.subprocess.Popen") as mock_popen:
                mock_process = MagicMock()
                mock_process.stdout = iter(json_lines)
                mock_process.wait.return_value = None
                mock_process.returncode = 0
                mock_popen.return_value = mock_process

                result = provider._run_with_output_parsing(
                    cmd=["codex", "exec", "--json", "-"],
                    log_file=log_file,
                    timeout_minutes=30,
                )

        assert mock_formatter.print_step_header.call_count == 2
        step_1_args = mock_formatter.print_step_header.call_args_list[0].args
        step_2_args = mock_formatter.print_step_header.call_args_list[1].args

        # Step 1: estimate only ("a" * 4000 => 1000 output tokens).
        assert step_1_args[1] == 1000
        assert step_1_args[2] == calculate_cost(0, 1000, "")

        # Step 2: real turn1 totals (300/100) + estimate for turn2 delta only (1000 output).
        assert step_2_args[1] == 1400
        assert step_2_args[2] == calculate_cost(300, 1100, "")
        assert step_2_args[1] < 2400  # Guard against old double-counting behavior.

        assert result.input_tokens == 300
        assert result.output_tokens == 100

    def test_on_session_id_callback_called_from_thread_started(self, tmp_path):
        """on_session_id callback should be invoked when thread.started event is parsed."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        captured: list[str] = []

        json_lines = [
            json.dumps({"type": "thread.started", "thread_id": "thread_early_456"}) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 50}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                on_session_id=captured.append,
            )

        assert captured == ["thread_early_456"]
        assert result.session_id == "thread_early_456"

    def test_on_session_id_callback_called_only_once_for_codex(self, tmp_path):
        """on_session_id callback should only be called once even if thread.started appears twice."""
        import json

        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        captured: list[str] = []

        json_lines = [
            json.dumps({"type": "thread.started", "thread_id": "thread_once"}) + "\n",
            json.dumps({"type": "thread.started", "thread_id": "thread_once"}) + "\n",
            json.dumps({"type": "turn.completed", "usage": {"input_tokens": 10, "output_tokens": 5}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                on_session_id=captured.append,
            )

        assert captured == ["thread_once"]


class TestSyncKeychainCredentials:
    """Tests for sync_keychain_credentials function."""

    def test_skips_on_non_darwin(self):
        """Should return False on non-macOS platforms."""
        from gza.providers.claude import sync_keychain_credentials
        with patch("gza.providers.claude.sys") as mock_sys:
            mock_sys.platform = "linux"
            assert sync_keychain_credentials() is False

    def test_skips_when_security_not_found(self):
        """Should return False when security command is not available."""
        from gza.providers.claude import sync_keychain_credentials
        with patch("gza.providers.claude.sys") as mock_sys, \
             patch("gza.providers.claude.shutil.which", return_value=None):
            mock_sys.platform = "darwin"
            assert sync_keychain_credentials() is False

    def test_skips_on_security_failure(self):
        """Should return False when security command fails."""
        from gza.providers.claude import sync_keychain_credentials
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("gza.providers.claude.sys") as mock_sys, \
             patch("gza.providers.claude.shutil.which", return_value="/usr/bin/security"), \
             patch("gza.providers.claude.subprocess.run", return_value=mock_result):
            mock_sys.platform = "darwin"
            assert sync_keychain_credentials() is False

    def test_skips_on_invalid_json(self):
        """Should return False when keychain entry is not valid JSON."""
        from gza.providers.claude import sync_keychain_credentials
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "not-json"
        with patch("gza.providers.claude.sys") as mock_sys, \
             patch("gza.providers.claude.shutil.which", return_value="/usr/bin/security"), \
             patch("gza.providers.claude.subprocess.run", return_value=mock_result):
            mock_sys.platform = "darwin"
            assert sync_keychain_credentials() is False

    def test_skips_when_missing_oauth_key(self):
        """Should return False when JSON doesn't contain claudeAiOauth."""
        from gza.providers.claude import sync_keychain_credentials
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps({"someOtherKey": "value"})
        with patch("gza.providers.claude.sys") as mock_sys, \
             patch("gza.providers.claude.shutil.which", return_value="/usr/bin/security"), \
             patch("gza.providers.claude.subprocess.run", return_value=mock_result):
            mock_sys.platform = "darwin"
            assert sync_keychain_credentials() is False

    def test_writes_credentials_file(self, tmp_path):
        """Should write credentials to ~/.claude/.credentials.json."""
        from gza.providers.claude import sync_keychain_credentials
        creds = {"claudeAiOauth": {"accessToken": "test-token", "refreshToken": "test-refresh"}}
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(creds)

        fake_home = tmp_path / "home"
        fake_home.mkdir()

        with patch("gza.providers.claude.sys") as mock_sys, \
             patch("gza.providers.claude.shutil.which", return_value="/usr/bin/security"), \
             patch("gza.providers.claude.subprocess.run", return_value=mock_result), \
             patch("gza.providers.claude.Path.home", return_value=fake_home):
            mock_sys.platform = "darwin"
            assert sync_keychain_credentials() is True

        creds_path = fake_home / ".claude" / ".credentials.json"
        assert creds_path.exists()
        written = json.loads(creds_path.read_text())
        assert written["claudeAiOauth"]["accessToken"] == "test-token"
        # Check file permissions (owner read/write only)
        assert oct(creds_path.stat().st_mode & 0o777) == "0o600"


class TestClaudeConfigIntegration:
    """Tests for ClaudeConfig in Config loading."""

    def test_default_claude_config(self, tmp_path):
        """Config should have default ClaudeConfig."""
        config = Config(project_dir=tmp_path, project_name="test")
        assert isinstance(config.claude, ClaudeConfig)
        assert config.claude.fetch_auth_token_from_keychain is False
        assert "--allowedTools" in config.claude.args

    def test_load_claude_section(self, tmp_path):
        """Config.load should parse claude section."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude:\n"
            "  fetch_auth_token_from_keychain: true\n"
            "  args:\n"
            "    - --verbose\n"
        )
        config = Config.load(tmp_path)
        assert config.claude.fetch_auth_token_from_keychain is True
        assert config.claude.args == ["--verbose"]

    def test_backward_compat_claude_args(self, tmp_path):
        """Top-level claude_args should still work with deprecation warning."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude_args:\n"
            "  - --verbose\n"
        )
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = Config.load(tmp_path)
        assert config.claude.args == ["--verbose"]
        assert any("deprecated" in str(warning.message).lower() for warning in w)

    def test_claude_args_section_takes_precedence(self, tmp_path):
        """claude.args should take precedence over top-level claude_args."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude_args:\n"
            "  - --old\n"
            "claude:\n"
            "  args:\n"
            "    - --new\n"
        )
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = Config.load(tmp_path)
        assert config.claude.args == ["--new"]
        assert any("deprecated" in str(warning.message).lower() for warning in w)

    def test_validate_claude_section(self, tmp_path):
        """Validate should accept valid claude section."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude:\n"
            "  fetch_auth_token_from_keychain: true\n"
            "  args:\n"
            "    - --verbose\n"
        )
        is_valid, errors, warnings = Config.validate(tmp_path)
        assert is_valid
        assert not errors

    def test_validate_claude_section_bad_type(self, tmp_path):
        """Validate should reject non-dict claude section."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude: not-a-dict\n"
        )
        is_valid, errors, warnings = Config.validate(tmp_path)
        assert not is_valid
        assert any("'claude' must be a dictionary" in e for e in errors)

    def test_validate_claude_args_deprecation_warning(self, tmp_path):
        """Validate should warn about deprecated claude_args."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "claude_args:\n"
            "  - --verbose\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert is_valid
        assert any("deprecated" in w.lower() for w in warns)

    def test_validate_rejects_model_incompatible_with_provider(self, tmp_path):
        """Validate should reject obvious cross-provider model mismatches."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "model: claude-3-5-haiku-latest\n"
        )
        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert not is_valid
        assert any("'model' model 'claude-3-5-haiku-latest' appears incompatible with provider 'codex'" in e for e in errors)

    def test_validate_rejects_task_type_model_incompatible_with_provider(self, tmp_path):
        """Validate should reject incompatible task_types.<type>.model overrides."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "task_types:\n"
            "  review:\n"
            "    model: claude-3-5-haiku-latest\n"
        )
        is_valid, errors, _warnings = Config.validate(tmp_path)
        assert not is_valid
        assert any("task_types.review.model" in e and "incompatible with provider 'codex'" in e for e in errors)

    def test_load_rejects_incompatible_task_type_model(self, tmp_path):
        """Load should fail fast on incompatible provider/task-type model config."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "task_types:\n"
            "  review:\n"
            "    model: claude-3-5-haiku-latest\n"
        )

        with pytest.raises(ConfigError, match="Invalid provider/model configuration"):
            Config.load(tmp_path)


class TestProviderScopedConfig:
    """Tests for provider-scoped model/task-type configuration."""

    def test_load_parses_provider_scoped_config(self, tmp_path):
        """Config.load should parse providers section into provider-scoped config."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: claude\n"
            "providers:\n"
            "  claude:\n"
            "    model: claude-sonnet-4-5\n"
            "    reasoning_effort: medium\n"
            "    task_types:\n"
            "      review:\n"
            "        model: claude-haiku-4-5\n"
            "        reasoning_effort: high\n"
            "        max_steps: 25\n"
            "        max_turns: 20\n"
            "  codex:\n"
            "    model: o4-mini\n"
        )
        config = Config.load(tmp_path)

        assert config.reasoning_effort == ""
        assert config.providers["claude"].model == "claude-sonnet-4-5"
        assert config.providers["claude"].reasoning_effort == "medium"
        assert config.providers["claude"].task_types["review"].model == "claude-haiku-4-5"
        assert config.providers["claude"].task_types["review"].reasoning_effort == "high"
        assert config.providers["claude"].task_types["review"].max_steps == 25
        assert config.providers["claude"].task_types["review"].max_turns == 20
        assert config.providers["codex"].model == "o4-mini"

    def test_load_parses_task_provider_routing(self, tmp_path):
        """Config.load should parse task_providers routing map."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "task_providers:\n"
            "  review: claude\n"
            "  plan: gemini\n"
        )
        config = Config.load(tmp_path)

        assert config.task_providers == {"review": "claude", "plan": "gemini"}
        assert config.get_provider_for_task("review") == "claude"
        assert config.get_provider_for_task("implement") == "codex"

    def test_validate_rejects_invalid_task_providers_shape(self, tmp_path):
        """Validate should reject non-dict and unknown provider names in task_providers."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "task_providers:\n"
            "  review: unknown\n"
        )
        is_valid, errors, _warns = Config.validate(tmp_path)

        assert not is_valid
        assert any("task_providers.review" in e and "must be one of" in e for e in errors)

    def test_validate_uses_task_provider_route_for_legacy_task_type_model(self, tmp_path):
        """task_types.<type>.model compatibility should validate against routed provider."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "task_providers:\n"
            "  review: claude\n"
            "task_types:\n"
            "  review:\n"
            "    model: claude-3-5-haiku-latest\n"
        )
        is_valid, errors, _warnings = Config.validate(tmp_path)

        assert is_valid
        assert not errors

    def test_validate_accepts_valid_providers_schema(self, tmp_path):
        """Validate should accept well-formed providers schema."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "providers:\n"
            "  codex:\n"
            "    model: o4-mini\n"
            "    task_types:\n"
            "      review:\n"
            "        max_turns: 10\n"
            "  claude:\n"
            "    model: claude-sonnet-4-5\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert is_valid
        assert not errors
        assert not [w for w in warns if "invalid" in w.lower()]

    def test_validate_rejects_invalid_provider_shapes(self, tmp_path):
        """Validate should reject unknown providers and invalid providers schema types."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "providers:\n"
            "  unknown:\n"
            "    model: x\n"
            "  claude: nope\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert not is_valid
        assert any("providers.unknown" in e for e in errors)
        assert any("'providers.claude' must be a dictionary" in e for e in errors)

    def test_validate_rejects_invalid_provider_task_type_values(self, tmp_path):
        """Validate should reject non-string scoped fields and non-positive max_turns in providers.task_types."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "providers:\n"
            "  claude:\n"
            "    reasoning_effort: 123\n"
            "    task_types:\n"
            "      review:\n"
            "        model: 123\n"
            "        reasoning_effort: 456\n"
            "        max_turns: 0\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert not is_valid
        assert any("providers.claude.reasoning_effort" in e for e in errors)
        assert any("providers.claude.task_types.review.model" in e for e in errors)
        assert any("providers.claude.task_types.review.reasoning_effort" in e for e in errors)
        assert any("providers.claude.task_types.review.max_turns" in e for e in errors)

    def test_validate_rejects_incompatible_provider_scoped_model(self, tmp_path):
        """Validate should reject providers.<provider>.model mismatched with provider family."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "providers:\n"
            "  codex:\n"
            "    model: claude-3-5-haiku-latest\n"
        )
        is_valid, errors, _warnings = Config.validate(tmp_path)

        assert not is_valid
        assert any("providers.codex.model" in e and "incompatible with provider 'codex'" in e for e in errors)

    def test_load_rejects_incompatible_provider_scoped_task_type_model(self, tmp_path):
        """Load should reject providers.<provider>.task_types.<type>.model mismatches."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "provider: codex\n"
            "providers:\n"
            "  codex:\n"
            "    task_types:\n"
            "      review:\n"
            "        model: claude-3-5-haiku-latest\n"
        )

        with pytest.raises(ConfigError, match="Invalid provider/model configuration"):
            Config.load(tmp_path)

    def test_validate_warns_for_mixed_legacy_and_scoped_config(self, tmp_path):
        """Validate should warn when scoped and legacy fields overlap on the same semantic target."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "model: legacy-model\n"
            "task_types:\n"
            "  review:\n"
            "    model: legacy-review\n"
            "    max_turns: 30\n"
            "providers:\n"
            "  claude:\n"
            "    model: scoped-model\n"
            "    task_types:\n"
            "      review:\n"
            "        model: scoped-review\n"
            "        max_turns: 20\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert is_valid
        assert not errors
        assert any("provider-scoped model" in w for w in warns)
        assert any("provider-scoped and legacy model are set for task type 'review'" in w for w in warns)
        assert any("provider-scoped and legacy max_turns are set for task type 'review'" in w for w in warns)

    def test_getters_apply_provider_scoped_precedence(self, tmp_path):
        """Provider-scoped getters should prefer scoped values then legacy fallbacks."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "max_steps: 60\n"
            "max_turns: 50\n"
            "model: legacy-model\n"
            "reasoning_effort: low\n"
            "task_types:\n"
            "  review:\n"
            "    model: legacy-review\n"
            "    reasoning_effort: medium\n"
            "    max_steps: 35\n"
            "    max_turns: 30\n"
            "providers:\n"
            "  claude:\n"
            "    model: scoped-model\n"
            "    reasoning_effort: high\n"
            "    task_types:\n"
            "      review:\n"
            "        model: scoped-review\n"
            "        reasoning_effort: minimal\n"
            "        max_steps: 22\n"
            "        max_turns: 20\n"
        )
        with pytest.warns(UserWarning):
            config = Config.load(tmp_path)
        assert config.get_model_for_task("review", "claude") == "scoped-review"
        assert config.get_model_for_task("task", "claude") == "scoped-model"
        assert config.get_model_for_task("review", "codex") == "legacy-review"
        assert config.get_reasoning_effort_for_task("review", "claude") == "minimal"
        assert config.get_reasoning_effort_for_task("task", "claude") == "high"
        assert config.get_reasoning_effort_for_task("review", "codex") == "medium"
        assert config.get_reasoning_effort_for_task("task", "codex") == "low"
        assert config.get_max_steps_for_task("review", "claude") == 22
        assert config.get_max_steps_for_task("review", "codex") == 35
        assert config.get_max_steps_for_task("task", "codex") == 60
        assert config.get_max_turns_for_task("review", "claude") == 22
        assert config.get_max_turns_for_task("review", "codex") == 35
        assert config.get_max_turns_for_task("task", "codex") == 60

    def test_validate_rejects_non_string_reasoning_effort_fields(self, tmp_path):
        """Validate should reject non-string reasoning_effort values across config scopes."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "reasoning_effort: 1\n"
            "defaults:\n"
            "  reasoning_effort: 2\n"
            "task_types:\n"
            "  review:\n"
            "    reasoning_effort: 3\n"
        )
        is_valid, errors, _warns = Config.validate(tmp_path)
        assert not is_valid
        assert any("'reasoning_effort' must be a string" in e for e in errors)
        assert any("'defaults.reasoning_effort' must be a string" in e for e in errors)
        assert any("'task_types.review.reasoning_effort' must be a string" in e for e in errors)

    def test_max_steps_falls_back_to_max_turns_with_warning(self, tmp_path):
        """Legacy max_turns should still resolve max steps and emit a deprecation warning."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "max_turns: 77\n"
        )
        with pytest.warns(DeprecationWarning, match="max_turns"):
            config = Config.load(tmp_path)

        assert config.max_steps == 77
        assert config.get_max_steps_for_task("task", "claude") == 77

    def test_max_steps_task_type_precedence_over_max_turns(self, tmp_path):
        """task_types.<type>.max_steps should win over task_types.<type>.max_turns and global values."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "max_steps: 50\n"
            "max_turns: 40\n"
            "task_types:\n"
            "  review:\n"
            "    max_steps: 11\n"
            "    max_turns: 9\n"
        )
        config = Config.load(tmp_path)
        assert config.get_max_steps_for_task("review", "claude") == 11


class TestOnStepCountCallback:
    """Tests for on_step_count callback in all three providers."""

    def test_claude_on_step_count_called_for_each_step(self, tmp_path):
        """on_step_count should be called each time a new step starts in Claude."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        json_lines = [
            json.dumps({"type": "system", "subtype": "init", "session_id": "ses1", "tools": []}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_1", "content": [], "usage": {}}}) + "\n",
            json.dumps({"type": "assistant", "message": {"id": "msg_2", "content": [], "usage": {}}}) + "\n",
            json.dumps({"type": "result", "subtype": "success", "num_turns": 2, "total_cost_usd": 0.01}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_step_count=counts.append,
            )

        assert counts == [1, 2]

    def test_claude_on_step_count_not_called_when_no_steps(self, tmp_path):
        """on_step_count should not be called when there are no assistant steps."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        json_lines = [
            json.dumps({"type": "result", "subtype": "success", "num_turns": 0, "total_cost_usd": 0.0}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_step_count=counts.append,
            )

        assert counts == []

    def test_codex_on_step_count_called_for_each_step(self, tmp_path):
        """on_step_count should be called each time a new step starts in Codex."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "step 1"},
            }) + "\n",
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "step 2"},
            }) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                on_step_count=counts.append,
            )

        assert counts == [1, 2]

    def test_gemini_on_step_count_called_for_each_step(self, tmp_path):
        """on_step_count should be called each time a new step starts in Gemini."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        json_lines = [
            json.dumps({"type": "message", "role": "assistant", "content": "First response"}) + "\n",
            json.dumps({"type": "message", "role": "user", "content": "ok"}) + "\n",
            json.dumps({"type": "message", "role": "assistant", "content": "Second response"}) + "\n",
            json.dumps({"type": "result", "stats": {}}) + "\n",
        ]

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(json_lines)
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.0-flash",
                on_step_count=counts.append,
            )

        assert counts == [1, 2]


# ---------------------------------------------------------------------------
# Full-conversation simulation tests
# ---------------------------------------------------------------------------
# These tests feed a realistic multi-step event stream through each provider's
# parser and verify step boundaries, substep structure, display output, and
# step counts match expectations.


class TestClaudeFullConversationSimulation:
    """Simulate a realistic multi-step Claude conversation end-to-end."""

    def _build_json_lines(self):
        """3-step conversation: text → tool+text → multi-tool+text → result."""
        return [
            # Step 1: plain text response
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_001",
                    "usage": {"input_tokens": 200, "output_tokens": 40,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0},
                    "content": [
                        {"type": "text", "text": "I'll examine the project structure first."},
                    ],
                },
            }) + "\n",
            # Step 2: tool call + result + text
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_002",
                    "usage": {"input_tokens": 350, "output_tokens": 80,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 100},
                    "content": [
                        {"type": "tool_use", "id": "call_glob", "name": "Glob",
                         "input": {"pattern": "**/*.py"}},
                        {"type": "tool_result", "tool_use_id": "call_glob",
                         "content": "src/main.py\nsrc/utils.py\ntests/test_main.py", "is_error": False},
                        {"type": "text", "text": "Found 3 Python files. Let me read the main module."},
                    ],
                },
            }) + "\n",
            # Step 3: multiple tools, a retry, error, then success
            json.dumps({
                "type": "assistant",
                "message": {
                    "id": "msg_003",
                    "usage": {"input_tokens": 500, "output_tokens": 120,
                              "cache_creation_input_tokens": 0, "cache_read_input_tokens": 50},
                    "content": [
                        {"type": "tool_use", "id": "call_read", "name": "Read",
                         "input": {"file_path": "/project/src/main.py"}},
                        {"type": "tool_result", "tool_use_id": "call_read",
                         "content": "def main(): pass", "is_error": False},
                        {"type": "tool_use", "id": "call_bash1", "name": "Bash",
                         "input": {"command": "python -m pytest tests/ -v"}},
                        {"type": "tool_result", "tool_use_id": "call_bash1",
                         "content": "Permission denied", "is_error": True},
                        {"type": "tool_retry", "id": "call_bash2",
                         "retry_of_call_id": "call_bash1"},
                        {"type": "tool_use", "id": "call_bash2", "name": "Bash",
                         "input": {"command": "python -m pytest tests/ -v"}},
                        {"type": "tool_result", "tool_use_id": "call_bash2",
                         "content": "3 passed", "is_error": False},
                        {"type": "text", "text": "All tests pass after retry."},
                    ],
                },
            }) + "\n",
            # Final result
            json.dumps({
                "type": "result", "subtype": "success",
                "num_turns": 3, "total_cost_usd": 0.05,
                "session_id": "ses_test123",
            }) + "\n",
        ]

    def test_step_count_and_boundaries(self, tmp_path):
        """Each unique msg_id should create exactly one step."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        steps = result._accumulated_data["run_step_events"]
        assert len(steps) == 3
        assert result.num_steps_computed == 3
        assert result.num_turns_reported == 3

    def test_substep_types_per_step(self, tmp_path):
        """Verify substep types match expected tool lifecycle per step."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        steps = result._accumulated_data["run_step_events"]

        # Step 1: text only → no substeps
        assert steps[0]["substeps"] == []
        assert steps[0]["message_text"] == "I'll examine the project structure first."

        # Step 2: tool_call + tool_output
        assert [s["type"] for s in steps[1]["substeps"]] == ["tool_call", "tool_output"]
        assert steps[1]["message_text"] == "Found 3 Python files. Let me read the main module."

        # Step 3: read + result, bash + error, retry + bash + result
        assert [s["type"] for s in steps[2]["substeps"]] == [
            "tool_call", "tool_output",      # Read
            "tool_call", "tool_error",        # Bash (failed)
            "tool_retry", "tool_call", "tool_output",  # Bash retry (success)
        ]
        assert steps[2]["message_text"] == "All tests pass after retry."

    def test_display_output_has_step_headers(self, tmp_path, capsys):
        """Live output should contain Step 1/2/3 headers."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        out = capsys.readouterr().out
        assert "Step 1" in out
        assert "Step 2" in out
        assert "Step 3" in out
        assert "→ Glob" in out
        assert "→ Read" in out
        assert "→ Bash" in out

    def test_step_count_callback(self, tmp_path):
        """on_step_count should fire for each new step."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                on_step_count=counts.append,
            )

        assert counts == [1, 2, 3]

    def test_log_file_has_step_timestamps(self, tmp_path):
        """Log file should contain step timestamp markers."""
        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["claude", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        pattern = r"--- Step \d+ at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        matches = re.findall(pattern, log_content)
        assert len(matches) == 3


class TestCodexFullConversationSimulation:
    """Simulate a realistic multi-step Codex conversation end-to-end."""

    def _build_json_lines(self):
        """3-step conversation across 2 API turns."""
        return [
            # API turn 1
            json.dumps({"type": "thread.started", "thread_id": "thread_abc"}) + "\n",
            json.dumps({"type": "turn.started"}) + "\n",
            # Step 1: agent thinks, then runs a tool
            json.dumps({"type": "item.completed", "item": {
                "type": "agent_message",
                "text": "I'll examine the project structure first.",
            }}) + "\n",
            json.dumps({"type": "item.completed", "item": {
                "id": "cmd_1",
                "type": "command_execution",
                "command": "find . -name '*.py' -type f",
                "aggregated_output": "./main.py\n./utils.py\n./tests/test_main.py",
                "exit_code": 0,
            }}) + "\n",
            # Step 2: agent responds with findings, runs another tool
            json.dumps({"type": "item.completed", "item": {
                "type": "agent_message",
                "text": "Found 3 Python files. Let me read the main module.",
            }}) + "\n",
            json.dumps({"type": "item.completed", "item": {
                "id": "cmd_2",
                "type": "command_execution",
                "command": "cat main.py",
                "aggregated_output": "def main(): pass",
                "exit_code": 0,
            }}) + "\n",
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 500, "output_tokens": 120, "cached_input_tokens": 50,
            }}) + "\n",
            # API turn 2
            json.dumps({"type": "turn.started"}) + "\n",
            # Step 3: agent runs tests and reports
            json.dumps({"type": "item.completed", "item": {
                "type": "agent_message",
                "text": "Now I'll run the test suite.",
            }}) + "\n",
            json.dumps({"type": "item.completed", "item": {
                "id": "cmd_3",
                "type": "command_execution",
                "command": "python -m pytest tests/ -v",
                "aggregated_output": "3 passed",
                "exit_code": 0,
            }}) + "\n",
            json.dumps({"type": "item.completed", "item": {
                "type": "agent_message",
                "text": "All 3 tests pass. The codebase looks healthy.",
            }}) + "\n",
            json.dumps({"type": "turn.completed", "usage": {
                "input_tokens": 800, "output_tokens": 200, "cached_input_tokens": 100,
            }}) + "\n",
        ]

    def test_step_count_and_boundaries(self, tmp_path):
        """Each agent_message should create a separate logical step."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        steps = result._accumulated_data["run_step_events"]
        # 4 agent_messages = 4 logical steps
        assert len(steps) == 4
        assert steps[0]["message_text"] == "I'll examine the project structure first."
        assert steps[1]["message_text"] == "Found 3 Python files. Let me read the main module."
        assert steps[2]["message_text"] == "Now I'll run the test suite."
        assert steps[3]["message_text"] == "All 3 tests pass. The codebase looks healthy."

    def test_substep_types_per_step(self, tmp_path):
        """Tool calls should attach as substeps to the preceding agent_message step."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        steps = result._accumulated_data["run_step_events"]

        # Step 1: agent_message + find command
        assert [s["type"] for s in steps[0]["substeps"]] == ["tool_call", "tool_output"]
        assert steps[0]["substeps"][0]["payload"]["command"] == "find . -name '*.py' -type f"

        # Step 2: agent_message + cat command
        assert [s["type"] for s in steps[1]["substeps"]] == ["tool_call", "tool_output"]
        assert steps[1]["substeps"][0]["payload"]["command"] == "cat main.py"

        # Step 3: agent_message + pytest command
        assert [s["type"] for s in steps[2]["substeps"]] == ["tool_call", "tool_output"]
        assert steps[2]["substeps"][0]["payload"]["command"] == "python -m pytest tests/ -v"

        # Step 4: final agent_message, no tools
        assert steps[3]["substeps"] == []

    def test_display_output_has_step_headers(self, tmp_path, capsys):
        """Live output should contain per-step headers, not per-turn."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        out = capsys.readouterr().out
        assert "Step 1" in out
        assert "Step 2" in out
        assert "Step 3" in out
        assert "Step 4" in out
        # No old-style [S1.x] prefixes
        assert "[S" not in out
        # Tool calls present
        assert "→ Bash" in out

    def test_step_count_callback(self, tmp_path):
        """on_step_count should fire for each logical step."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
                on_step_count=counts.append,
            )

        # 4 agent_messages = 4 step_count callbacks
        assert len(counts) == 4

    def test_log_file_has_step_timestamps(self, tmp_path):
        """Log file should contain step timestamp markers for each agent_message."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        log_content = log_file.read_text()
        pattern = r"--- Step \d+ at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        matches = re.findall(pattern, log_content)
        assert len(matches) == 4

    def test_session_id_captured(self, tmp_path):
        """thread_id should be captured as session ID."""
        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["codex", "exec", "--json", "-"],
                log_file=log_file,
                timeout_minutes=30,
            )

        assert result.session_id == "thread_abc"


class TestGeminiFullConversationSimulation:
    """Simulate a realistic multi-step Gemini conversation end-to-end."""

    def _build_json_lines(self):
        """3-step conversation: text+tool → text+tool → text+tool(retry) → text → result."""
        return [
            # Step 1: assistant message + tool call
            json.dumps({"type": "message", "role": "assistant",
                         "content": "I'll find all Python files first."}) + "\n",
            json.dumps({"type": "tool_use", "id": "call_1", "tool_name": "Bash",
                         "tool_input": {"command": "find . -name '*.py'"}}) + "\n",
            json.dumps({"type": "tool_output", "call_id": "call_1",
                         "output": "./main.py\n./utils.py\n./tests/test_main.py"}) + "\n",
            # Step 2: user turn resets, then assistant + tool
            json.dumps({"type": "message", "role": "user",
                         "content": "Now read main.py"}) + "\n",
            json.dumps({"type": "message", "role": "assistant",
                         "content": "Let me read main.py for you."}) + "\n",
            json.dumps({"type": "tool_use", "id": "call_2", "tool_name": "Read",
                         "tool_input": {"file_path": "/project/main.py"}}) + "\n",
            json.dumps({"type": "tool_output", "call_id": "call_2",
                         "output": "def main(): pass"}) + "\n",
            # Step 3: user turn resets, assistant + tool + error + retry
            json.dumps({"type": "message", "role": "user",
                         "content": "Run the tests"}) + "\n",
            json.dumps({"type": "message", "role": "assistant",
                         "content": "Running the test suite now."}) + "\n",
            json.dumps({"type": "tool_use", "id": "call_3", "tool_name": "Bash",
                         "tool_input": {"command": "pytest tests/ -v"}}) + "\n",
            json.dumps({"type": "tool_error", "call_id": "call_3",
                         "error": "Permission denied"}) + "\n",
            json.dumps({"type": "tool_retry", "call_id": "call_4",
                         "retry_of_call_id": "call_3"}) + "\n",
            json.dumps({"type": "tool_use", "id": "call_4", "tool_name": "Bash",
                         "tool_input": {"command": "python -m pytest tests/ -v"}}) + "\n",
            json.dumps({"type": "tool_output", "call_id": "call_4",
                         "output": "3 passed"}) + "\n",
            # Step 4: final summary (no tools)
            json.dumps({"type": "message", "role": "assistant",
                         "content": "All 3 tests pass after retry."}) + "\n",
            # Result
            json.dumps({"type": "result", "stats": {
                "input_tokens": 800, "output_tokens": 200,
            }}) + "\n",
        ]

    def test_step_count_and_boundaries(self, tmp_path):
        """Each assistant message should create a separate step."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        steps = result._accumulated_data["run_step_events"]
        assert len(steps) == 4
        assert steps[0]["message_text"] == "I'll find all Python files first."
        assert steps[1]["message_text"] == "Let me read main.py for you."
        assert steps[2]["message_text"] == "Running the test suite now."
        assert steps[3]["message_text"] == "All 3 tests pass after retry."

    def test_substep_types_per_step(self, tmp_path):
        """Tool calls should attach as substeps to the correct step."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            result = provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        steps = result._accumulated_data["run_step_events"]

        # Step 1: find command
        assert [s["type"] for s in steps[0]["substeps"]] == ["tool_call", "tool_output"]

        # Step 2: read command
        assert [s["type"] for s in steps[1]["substeps"]] == ["tool_call", "tool_output"]

        # Step 3: bash error + retry + success
        assert [s["type"] for s in steps[2]["substeps"]] == [
            "tool_call", "tool_error", "tool_retry", "tool_call", "tool_output",
        ]

        # Step 4: final text, no tools
        assert steps[3]["substeps"] == []

    def test_display_output_has_step_headers(self, tmp_path, capsys):
        """Live output should contain Step 1-4 headers."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        out = capsys.readouterr().out
        assert "Step 1" in out
        assert "Step 2" in out
        assert "Step 3" in out
        assert "Step 4" in out
        assert "→ Bash" in out
        assert "→ Read" in out

    def test_step_count_callback(self, tmp_path):
        """on_step_count should fire for each new step."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"
        counts: list[int] = []

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
                on_step_count=counts.append,
            )

        assert len(counts) == 4

    def test_log_file_has_step_timestamps(self, tmp_path):
        """Log file should contain step timestamp markers."""
        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        with patch("gza.providers.base.subprocess.Popen") as mock_popen:
            mock_process = MagicMock()
            mock_process.stdout = iter(self._build_json_lines())
            mock_process.wait.return_value = None
            mock_process.returncode = 0
            mock_popen.return_value = mock_process

            provider._run_with_output_parsing(
                cmd=["gemini", "-p", "test"],
                log_file=log_file,
                timeout_minutes=30,
                model="gemini-2.5-flash",
            )

        log_content = log_file.read_text()
        pattern = r"--- Step \d+ at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
        matches = re.findall(pattern, log_content)
        assert len(matches) == 4


class TestPreflightLogging:
    """verify_credentials should leave a breadcrumb in the task log."""

    def _read_preflight_entries(self, log_file: Path) -> list[dict]:
        entries: list[dict] = []
        for line in log_file.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if obj.get("subtype") == "preflight":
                entries.append(obj)
        return entries

    def test_codex_verify_missing_binary_is_logged(self, tmp_path: Path):
        log_file = tmp_path / "task.log"
        log_file.touch()
        provider = CodexProvider()

        with patch("gza.providers.codex.subprocess.run", side_effect=FileNotFoundError()):
            ok = provider._verify_direct(log_file=log_file)

        assert ok.ok is False
        assert ok.failure_reason == "INFRASTRUCTURE_ERROR"
        entries = self._read_preflight_entries(log_file)
        assert len(entries) == 1
        assert entries[0]["event"] == "verify_credentials_missing_binary"
        assert entries[0]["returncode"] is None
        assert "codex" in entries[0]["command"]

    def test_codex_verify_timeout_is_logged(self, tmp_path: Path):
        log_file = tmp_path / "task.log"
        log_file.touch()
        provider = CodexProvider()

        with patch(
            "gza.providers.codex.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["codex", "--version"], timeout=5),
        ):
            ok = provider._verify_direct(log_file=log_file)

        assert ok.ok is False
        assert ok.failure_reason == "INFRASTRUCTURE_ERROR"
        entries = self._read_preflight_entries(log_file)
        assert entries and entries[0]["event"] == "verify_credentials_timeout"

    def test_codex_verify_success_captures_output(self, tmp_path: Path):
        log_file = tmp_path / "task.log"
        log_file.touch()
        provider = CodexProvider()

        fake_result = subprocess.CompletedProcess(
            args=["codex", "--version"],
            returncode=0,
            stdout="codex-cli 0.120.0\n",
            stderr="",
        )
        with patch("gza.providers.codex.subprocess.run", return_value=fake_result):
            ok = provider._verify_direct(log_file=log_file)

        assert ok.ok is True
        entries = self._read_preflight_entries(log_file)
        assert entries and entries[0]["event"] == "verify_credentials_direct"
        assert entries[0]["returncode"] == 0
        assert "codex-cli" in entries[0]["stdout_tail"]

    def test_claude_verify_missing_binary_is_logged(self, tmp_path: Path):
        log_file = tmp_path / "task.log"
        log_file.touch()
        provider = ClaudeProvider()

        with patch("gza.providers.claude.subprocess.run", side_effect=FileNotFoundError()):
            ok = provider._verify_direct(log_file=log_file)

        assert ok.ok is False
        assert ok.failure_reason == "INFRASTRUCTURE_ERROR"
        entries = self._read_preflight_entries(log_file)
        assert entries and entries[0]["event"] == "verify_credentials_missing_binary"

    def test_codex_exec_cmd_passes_skip_update_flag(self, tmp_path: Path):
        """Codex exec invocations must set check_for_update_on_startup=false."""
        provider = CodexProvider()
        config = Config(
            project_dir=tmp_path,
            project_name="test-project",
            provider="codex",
            model="gpt-5.2-codex",
            use_docker=False,
            timeout_minutes=1,
        )
        log_file = tmp_path / "task.log"
        log_file.touch()

        captured_cmd: list[str] = []

        def fake_run_with_output_parsing(cmd, *args, **kwargs):
            captured_cmd.extend(cmd)
            from gza.providers.base import RunResult
            return RunResult(exit_code=0)

        with patch.object(provider, "_run_with_output_parsing", side_effect=fake_run_with_output_parsing):
            provider._run_direct(
                config=config,
                prompt="hello",
                log_file=log_file,
                work_dir=tmp_path,
            )
        codex_idx = captured_cmd.index("codex")
        assert captured_cmd[codex_idx + 1] == "-c"
        assert captured_cmd[codex_idx + 2] == "check_for_update_on_startup=false"

    def test_codex_headless_exec_builder_matches_supported_contract(self, tmp_path: Path):
        """Shared headless exec args should stay aligned with the supported Codex CLI contract."""
        assert build_headless_exec_args(tmp_path) == [
            "-c",
            "check_for_update_on_startup=false",
            "exec",
            "--json",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "-C",
            str(tmp_path),
            "-",
        ]

    def test_codex_provider_spec_examples_match_shared_headless_exec_contract(self):
        """Spec examples should stay aligned with the shared Codex headless exec argv."""
        repo_root = Path(__file__).resolve().parents[1]
        spec_text = (repo_root / "specs" / "codex-provider.md").read_text()
        expected_cli = f"`codex {' '.join(build_headless_exec_args('<workdir>'))}`"

        assert expected_cli in spec_text
        assert "--dangerously-work" not in spec_text

    def test_codex_debug_doc_examples_match_shared_headless_exec_contract(self):
        """Debug docs should reuse the supported Codex headless exec argv verbatim."""
        repo_root = Path(__file__).resolve().parents[1]
        debug_doc_text = (repo_root / "docs" / "debug" / "codex-docker-investigation.md").read_text()
        expected_subcommand = f"codex {' '.join(build_headless_exec_args('/workspace'))}"

        assert debug_doc_text.count(expected_subcommand) == 2
        assert "--dangerously-work" not in debug_doc_text
