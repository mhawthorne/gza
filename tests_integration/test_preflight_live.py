"""Integration live tests for the `gza preflight` provider/model round-trip."""

import os
import pwd
from pathlib import Path

import pytest

from gza.cli.config_cmds import CheckTarget, run_preflight_target
from gza.config import Config
from tests_integration.test_docker import has_codex_api_key, has_codex_cli, has_codex_credentials


def _repo_root() -> Path:
    """Resolve the real project root instead of assuming a Docker mount path."""
    return Path(__file__).resolve().parents[1]


def _restore_codex_home_if_needed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point HOME at the real user home when live Codex auth depends on OAuth files."""
    if has_codex_api_key():
        return

    home_dir = Path(pwd.getpwuid(os.getuid()).pw_dir)
    auth_file = home_dir / ".codex" / "auth.json"
    if auth_file.exists():
        monkeypatch.setenv("HOME", str(home_dir))


@pytest.mark.integration
@pytest.mark.timeout(30, method="signal")
@pytest.mark.skipif(not has_codex_credentials(), reason="Codex credentials not available")
@pytest.mark.skipif(not has_codex_cli(), reason="Codex CLI not installed")
class TestPreflightLiveCodex:
    def test_run_preflight_target_passes_for_direct_codex_default_model(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _restore_codex_home_if_needed(monkeypatch)
        project_dir = _repo_root()
        config = Config(
            project_dir=project_dir,
            project_name="gza",
            provider="codex",
            use_docker=False,
            timeout_minutes=2,
            max_turns=5,
        )

        result = run_preflight_target(
            config,
            CheckTarget(provider="codex", model=None, sources=["default"]),
            use_docker=False,
            work_dir=project_dir,
            log_file=tmp_path / "codex-pass.jsonl",
        )

        assert result.status == "PASS"
        assert result.duration_s >= 0
        assert result.detail.endswith("s")

    def test_run_preflight_target_fails_for_bad_direct_codex_model(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _restore_codex_home_if_needed(monkeypatch)
        project_dir = _repo_root()
        config = Config(
            project_dir=project_dir,
            project_name="gza",
            provider="codex",
            use_docker=False,
            timeout_minutes=2,
            max_turns=5,
        )

        result = run_preflight_target(
            config,
            CheckTarget(provider="codex", model="gpt-5.4-codex", sources=["cli"]),
            use_docker=False,
            work_dir=project_dir,
            log_file=tmp_path / "codex-fail.jsonl",
        )

        assert result.status == "FAIL"
        assert any(
            token in result.detail.lower()
            for token in ("model", "unknown", "invalid", "not found", "not supported")
        ), result.detail
