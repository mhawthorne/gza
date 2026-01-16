"""Tests for AI code generation providers."""

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from gza.config import Config
from gza.providers import (
    get_provider,
    ClaudeProvider,
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
        """Should set error_type='max_turns' when result has subtype error_max_turns."""
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

        assert result.error_type == "max_turns"
        assert result.num_turns == 60
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
        assert result.num_turns == 5
        assert result.exit_code == 0


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

        with patch("gza.providers.base._get_image_created_time", return_value=image_time):
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