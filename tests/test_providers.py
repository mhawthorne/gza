"""Tests for AI code generation providers."""

import io
import json
import os
import re
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from rich.console import Console

from gza.config import Config, ClaudeConfig, ConfigError
from gza.providers import (
    get_provider,
    ClaudeProvider,
    CodexProvider,
    GeminiProvider,
    DockerConfig,
)
from gza.providers.base import (
    build_docker_cmd,
    DOCKERFILE_TEMPLATE,
    is_docker_running,
    verify_docker_credentials,
    ensure_docker_image,
    _get_image_created_time,
    _format_command_for_log,
    _extract_startup_log_line,
)
from gza.providers.output_formatter import (
    StreamOutputFormatter,
    format_runtime,
    format_token_count,
    truncate_text,
)
from gza.providers.gemini import calculate_cost, GEMINI_PRICING


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

            def verify_credentials(self, config: Config) -> bool:
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

    def test_turn_header_is_colorized(self):
        """Turn headers should include ANSI color sequences."""
        output = io.StringIO()
        console = Console(file=output, force_terminal=True, color_system="truecolor")
        formatter = StreamOutputFormatter(console=console)

        formatter.print_turn_header(2, 1500, 0.1234, 65)

        rendered = output.getvalue()
        plain = re.sub(r"\x1b\[[0-9;]*m", "", rendered)
        assert "| Turn 2 | 1k tokens | $0.12 | 1m 5s |" in plain
        assert "\x1b[" in rendered

    def test_key_event_lines_are_colorized(self):
        """Tool, assistant, and error lines should all be colorized."""
        output = io.StringIO()
        console = Console(file=output, force_terminal=True, color_system="truecolor")
        formatter = StreamOutputFormatter(console=console)

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

        # Find the config mount (second -v)
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
        """Should not pass environment variables when they are not set."""
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

        assert "-e" not in cmd

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

        # Workspace and config should come before custom
        assert len(volume_mounts) >= 3
        assert volume_mounts[0] == f"{tmp_path}:/workspace"
        assert ".testconfig" in volume_mounts[1]
        assert "/custom:/custom" in volume_mounts

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

        # Should only have workspace and config mounts
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        assert len(v_indices) == 2

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

        # Should only have workspace and config mounts
        v_indices = [i for i, x in enumerate(cmd) if x == "-v"]
        assert len(v_indices) == 2

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
        assert "GZA_DOCKER_SETUP_COMMAND=uv sync --project /workspace" in env_values

    def test_no_setup_command_env_var_when_empty(self, tmp_path):
        """Should not pass GZA_DOCKER_SETUP_COMMAND when docker_setup_command is empty."""
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

        assert "GZA_DOCKER_SETUP_COMMAND" not in " ".join(cmd)

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

        assert result is False
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

        assert result is True
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


class TestTurnTimestampLogging:
    """Tests for timestamp logging at the start of each turn in the log file."""

    def test_logs_timestamp_to_log_file_on_new_turn(self, tmp_path):
        """Should write a turn timestamp line to the log file when a new turn starts."""
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
        assert "--- Turn 1 at " in log_content

    def test_logs_timestamp_for_each_turn(self, tmp_path):
        """Should write a timestamp line for each new turn."""
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
        assert "--- Turn 1 at " in log_content
        assert "--- Turn 2 at " in log_content

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
        # Pattern: "--- Turn 1 at 2026-02-23 12:34:56 PST ---"
        pattern = r"--- Turn 1 at \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+ ---"
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
        # Only one Turn 1 timestamp, no Turn 2
        assert log_content.count("--- Turn ") == 1
        assert "--- Turn 2 at " not in log_content


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

        with patch("gza.providers.base._get_image_created_time", return_value=image_time):
            with patch("gza.providers.base._get_image_label", return_value="testcli"):
                with patch("gza.providers.base.subprocess.run") as mock_run:
                    result = ensure_docker_image(docker_config, tmp_path)

        assert result is True
        # subprocess.run should NOT be called (no build needed)
        mock_run.assert_not_called()

    def test_rebuilds_when_image_label_mismatch(self, tmp_path):
        """Should rebuild image when existing tag was built for another CLI."""
        docker_config = DockerConfig(
            image_name="test-image",
            npm_package="@openai/codex",
            cli_command="codex",
            config_dir=None,
            env_vars=[],
        )

        # Create Dockerfile
        etc_dir = tmp_path / "etc"
        etc_dir.mkdir()
        dockerfile = etc_dir / "Dockerfile.codex"
        dockerfile.write_text("FROM node:20-slim")

        # Image exists and is newer, but label mismatch should still rebuild
        dockerfile_mtime = dockerfile.stat().st_mtime
        image_time = dockerfile_mtime + 100

        with patch("gza.providers.base._get_image_created_time", return_value=image_time):
            with patch("gza.providers.base._get_image_label", return_value="claude"):
                with patch("gza.providers.base.subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0)
                    result = ensure_docker_image(docker_config, tmp_path)

        assert result is True
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        assert call_args[:3] == ["docker", "build", "-t"]
        assert "--label" in call_args
        assert "gza.cli_command=codex" in call_args
        assert "gza.npm_package=@openai/codex" in call_args

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

        with patch("gza.providers.base._get_image_created_time", return_value=image_time):
            with patch("gza.providers.base._get_image_label", return_value="testcli"):
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

        with patch("gza.providers.base._get_image_created_time", return_value=None):
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

        with patch("gza.providers.base._get_image_created_time", return_value=None):
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

        with patch("gza.providers.base._get_image_created_time", return_value=None):
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

        with patch("gza.providers.base._get_image_created_time", return_value=None):
            with patch("gza.providers.base.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=1)
                result = ensure_docker_image(docker_config, tmp_path)

        assert result is False


class TestCodexProvider:
    """Tests for Codex provider."""

    def test_codex_docker_config_with_oauth(self, tmp_path):
        """Codex should prefer OAuth when ~/.codex/auth.json exists."""
        from gza.providers.codex import _get_docker_config, _has_codex_oauth

        # When OAuth exists, mount .codex and don't pass API key
        with patch("gza.providers.codex._has_codex_oauth", return_value=True):
            config = _get_docker_config("my-project-gza")
            assert config.image_name == "my-project-gza"
            assert config.npm_package == "@openai/codex"
            assert config.cli_command == "codex"
            assert config.config_dir == ".codex"
            assert config.env_vars == []

    def test_codex_docker_config_with_api_key(self):
        """Codex should use CODEX_API_KEY when no OAuth credentials exist."""
        from gza.providers.codex import _get_docker_config

        # When no OAuth, use API key
        with patch("gza.providers.codex._has_codex_oauth", return_value=False):
            config = _get_docker_config("my-project-gza")
            assert config.image_name == "my-project-gza"
            assert config.npm_package == "@openai/codex"
            assert config.cli_command == "codex"
            assert config.config_dir is None
            assert "CODEX_API_KEY" in config.env_vars

    def test_check_credentials_with_api_key(self):
        """Codex should check for CODEX_API_KEY."""
        provider = CodexProvider()

        with patch.object(Path, "home", return_value=Path("/nonexistent")):
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            with patch.dict(os.environ, {"CODEX_API_KEY": "sk-test"}):
                assert provider.check_credentials() is True

    def test_check_credentials_with_config_dir(self, tmp_path):
        """Codex should check for ~/.codex directory."""
        provider = CodexProvider()

        with patch.object(Path, "home", return_value=tmp_path):
            with patch.dict(os.environ, {}, clear=True):
                assert provider.check_credentials() is False

            (tmp_path / ".codex").mkdir()
            assert provider.check_credentials() is True

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

    def test_logs_item_prefix_with_turn_and_item_index(self, tmp_path, capsys):
        """Should include turn/item index prefix for item.completed output."""
        import json
        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
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
        assert "[T1.1]" in captured.out
        assert "→ Bash ls -la" in captured.out

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
        """Should track when max_steps is exceeded based on item.completed events."""
        import json
        from gza.providers.codex import CodexProvider

        provider = CodexProvider()
        log_file = tmp_path / "test.log"

        # Simulate exceeding max_steps (set to 2)
        json_lines = [
            json.dumps({"type": "turn.started"}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "command_execution", "command": "echo 1"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "step 2"}}) + "\n",
            json.dumps({"type": "item.completed", "item": {"type": "reasoning", "text": "step 3"}}) + "\n",
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

        mock_formatter.print_turn_header.assert_called_once()
        mock_formatter.print_tool_event.assert_called()
        mock_formatter.print_agent_message.assert_called()
        mock_formatter.print_error.assert_called()


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
            "    task_types:\n"
            "      review:\n"
            "        model: claude-haiku-4-5\n"
            "        max_steps: 25\n"
            "        max_turns: 20\n"
            "  codex:\n"
            "    model: o4-mini\n"
        )
        config = Config.load(tmp_path)

        assert config.providers["claude"].model == "claude-sonnet-4-5"
        assert config.providers["claude"].task_types["review"].model == "claude-haiku-4-5"
        assert config.providers["claude"].task_types["review"].max_steps == 25
        assert config.providers["claude"].task_types["review"].max_turns == 20
        assert config.providers["codex"].model == "o4-mini"

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
        """Validate should reject non-string model and non-positive max_turns in providers.task_types."""
        config_path = tmp_path / "gza.yaml"
        config_path.write_text(
            "project_name: test\n"
            "providers:\n"
            "  claude:\n"
            "    task_types:\n"
            "      review:\n"
            "        model: 123\n"
            "        max_turns: 0\n"
        )
        is_valid, errors, warns = Config.validate(tmp_path)
        assert not is_valid
        assert any("providers.claude.task_types.review.model" in e for e in errors)
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
            "task_types:\n"
            "  review:\n"
            "    model: legacy-review\n"
            "    max_steps: 35\n"
            "    max_turns: 30\n"
            "providers:\n"
            "  claude:\n"
            "    model: scoped-model\n"
            "    task_types:\n"
            "      review:\n"
            "        model: scoped-review\n"
            "        max_steps: 22\n"
            "        max_turns: 20\n"
        )
        config = Config.load(tmp_path)
        assert config.get_model_for_task("review", "claude") == "scoped-review"
        assert config.get_model_for_task("task", "claude") == "scoped-model"
        assert config.get_model_for_task("review", "codex") == "legacy-review"
        assert config.get_max_steps_for_task("review", "claude") == 22
        assert config.get_max_steps_for_task("review", "codex") == 35
        assert config.get_max_steps_for_task("task", "codex") == 60
        assert config.get_max_turns_for_task("review", "claude") == 22
        assert config.get_max_turns_for_task("review", "codex") == 35
        assert config.get_max_turns_for_task("task", "codex") == 60

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
