"""Integration tests for provider smoke testing.

These tests require valid credentials and Docker, so they're marked
as integration tests and skipped by default.

Run with: uv run pytest tests/test_integration.py -v -m integration
"""

import os
from pathlib import Path

import pytest

from gza.config import Config
from gza.providers import ClaudeProvider, GeminiProvider


# Skip all tests in this module unless explicitly running integration tests
pytestmark = pytest.mark.integration


def _load_gza_env() -> None:
    """Load ~/.gza/.env if it exists (same as runner.py does)."""
    home_env = Path.home() / ".gza" / ".env"
    if home_env.exists():
        with open(home_env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


# Load env vars from ~/.gza/.env at module import time
_load_gza_env()


def has_claude_api_key() -> bool:
    """Check if Claude API key is available (required for Docker)."""
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def has_claude_credentials() -> bool:
    """Check if any Claude credentials are available (API key or OAuth)."""
    if os.getenv("ANTHROPIC_API_KEY"):
        return True
    # OAuth credentials stored in macOS Keychain, indicated by ~/.claude existing
    claude_config = Path.home() / ".claude"
    return claude_config.is_dir()


def has_gemini_api_key() -> bool:
    """Check if Gemini API key is available (required for Docker)."""
    return bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))


def has_gemini_credentials() -> bool:
    """Check if any Gemini credentials are available."""
    if os.getenv("GEMINI_API_KEY"):
        return True
    if os.getenv("GOOGLE_API_KEY"):
        return True
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return True
    gemini_config = Path.home() / ".gemini"
    return gemini_config.is_dir()


def has_docker() -> bool:
    """Check if Docker is available."""
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def has_claude_cli() -> bool:
    """Check if Claude CLI is installed."""
    import subprocess
    try:
        result = subprocess.run(
            ["claude", "--version"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def has_gemini_cli() -> bool:
    """Check if Gemini CLI is installed."""
    import subprocess
    try:
        result = subprocess.run(
            ["gemini", "--version"],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


class TestClaudeSmoke:
    """Smoke tests for Claude provider."""

    @pytest.mark.skipif(not has_claude_api_key(), reason="ANTHROPIC_API_KEY required for Docker")
    @pytest.mark.skipif(not has_docker(), reason="Docker not available")
    def test_claude_docker_writes_file(self, tmp_path):
        """Claude in Docker should be able to write a file.

        Note: Docker tests require ANTHROPIC_API_KEY env var.
        OAuth credentials (macOS Keychain) don't work in Docker.
        """
        config = Config(
            project_dir=tmp_path,
            project_name="smoke-test-claude",
            provider="claude",
            use_docker=True,
            timeout_minutes=2,
            max_turns=5,
        )

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        prompt = "Write the text 'hello world' to a file named result.txt. Do not write anything else."

        result = provider.run(config, prompt, log_file, tmp_path)

        # Check the file was created
        result_file = tmp_path / "result.txt"
        assert result_file.exists(), f"result.txt was not created. Exit code: {result.exit_code}"

        # Check content (lenient - just needs to contain hello or world)
        content = result_file.read_text().lower()
        assert "hello" in content or "world" in content, f"Unexpected content: {content}"

    @pytest.mark.skipif(not has_claude_credentials(), reason="Claude credentials not available")
    @pytest.mark.skipif(not has_claude_cli(), reason="Claude CLI not installed")
    def test_claude_direct_writes_file(self, tmp_path):
        """Claude direct (no Docker) should be able to write a file."""
        config = Config(
            project_dir=tmp_path,
            project_name="smoke-test-claude",
            provider="claude",
            use_docker=False,
            timeout_minutes=2,
            max_turns=5,
        )

        provider = ClaudeProvider()
        log_file = tmp_path / "test.log"

        prompt = "Write the text 'hello world' to a file named result.txt. Do not write anything else."

        result = provider.run(config, prompt, log_file, tmp_path)

        # Check the file was created
        result_file = tmp_path / "result.txt"
        assert result_file.exists(), f"result.txt was not created. Exit code: {result.exit_code}"

        # Check content (lenient - just needs to contain hello or world)
        content = result_file.read_text().lower()
        assert "hello" in content or "world" in content, f"Unexpected content: {content}"


class TestGeminiSmoke:
    """Smoke tests for Gemini provider."""

    @pytest.mark.skipif(not has_gemini_api_key(), reason="GEMINI_API_KEY or GOOGLE_API_KEY required for Docker")
    @pytest.mark.skipif(not has_docker(), reason="Docker not available")
    def test_gemini_docker_writes_file(self, tmp_path):
        """Gemini in Docker should be able to write a file.

        Note: Docker tests require GEMINI_API_KEY or GOOGLE_API_KEY env var.
        """
        config = Config(
            project_dir=tmp_path,
            project_name="smoke-test-gemini",
            provider="gemini",
            use_docker=True,
            timeout_minutes=2,
            max_turns=5,
        )

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        prompt = "Write the text 'hello world' to a file named result.txt. Do not write anything else."

        result = provider.run(config, prompt, log_file, tmp_path)

        # Check the file was created
        result_file = tmp_path / "result.txt"
        assert result_file.exists(), f"result.txt was not created. Exit code: {result.exit_code}"

        # Check content (lenient - just needs to contain hello or world)
        content = result_file.read_text().lower()
        assert "hello" in content or "world" in content, f"Unexpected content: {content}"

    @pytest.mark.skipif(not has_gemini_credentials(), reason="Gemini credentials not available")
    @pytest.mark.skipif(not has_gemini_cli(), reason="Gemini CLI not installed")
    def test_gemini_direct_writes_file(self, tmp_path):
        """Gemini direct (no Docker) should be able to write a file."""
        config = Config(
            project_dir=tmp_path,
            project_name="smoke-test-gemini",
            provider="gemini",
            use_docker=False,
            timeout_minutes=2,
            max_turns=5,
        )

        provider = GeminiProvider()
        log_file = tmp_path / "test.log"

        prompt = "Write the text 'hello world' to a file named result.txt. Do not write anything else."

        result = provider.run(config, prompt, log_file, tmp_path)

        # Check the file was created
        result_file = tmp_path / "result.txt"
        assert result_file.exists(), f"result.txt was not created. Exit code: {result.exit_code}"

        # Check content (lenient - just needs to contain hello or world)
        content = result_file.read_text().lower()
        assert "hello" in content or "world" in content, f"Unexpected content: {content}"
