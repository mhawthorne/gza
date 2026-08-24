"""Tests for runtime-owned cwd/environment normalization."""

from pathlib import Path

from gza.config import Config
from gza.runtime_context import (
    RuntimeExecutionContext,
    build_tmux_new_session_command,
    normalize_subprocess_env,
    sanitize_environment_values,
    write_tmux_environment_file,
)


def test_runtime_context_strips_repository_selecting_git_config_from_supervisor_and_dotenv(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    foreign_worktree = tmp_path / "foreign"
    foreign_worktree.mkdir()
    (project_dir / ".env").write_text(
        "\n".join(
            [
                "GIT_CONFIG_COUNT=1",
                "GIT_CONFIG_KEY_0=core.worktree",
                f"GIT_CONFIG_VALUE_0={foreign_worktree}",
                "GIT_CONFIG_PARAMETERS='core.worktree=foreign'",
                f"GIT_CEILING_DIRECTORIES={tmp_path}",
                "GIT_SSH_COMMAND=ssh -i project-key",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.worktree")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(tmp_path / "ambient-foreign"))
    monkeypatch.setenv("GIT_CONFIG_PARAMETERS", "'core.worktree=ambient'")
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path / "ambient-ceiling"))
    monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -i ambient-key")

    config = Config(project_dir=project_dir, project_name="owned")
    runtime = RuntimeExecutionContext.from_config(config)

    for key in (
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CEILING_DIRECTORIES",
    ):
        assert key not in runtime.env
        assert key not in normalize_subprocess_env(runtime.env, project_dir)
    assert runtime.env["PWD"] == str(project_dir.resolve())
    assert runtime.env["GIT_SSH_COMMAND"] == "ssh -i project-key"


def test_tmux_new_session_command_uses_env_file_without_serializing_values(tmp_path: Path) -> None:
    env = {
        "PATH": "/owned/bin",
        "HOME": str(tmp_path / "home"),
        "PWD": str(tmp_path),
        "GZA_DB_PATH": str(tmp_path / "owned.db"),
        "ANTHROPIC_API_KEY": "anthropic-poison",
        "CODEX_API_KEY": "codex-poison",
        "GEMINI_API_KEY": "gemini-poison",
        "PROJECT_ONLY_TOKEN": "arbitrary-poison",
        "bad-name": "ignored",
    }
    env_file = write_tmux_environment_file(cwd=tmp_path, env=env)

    cmd = build_tmux_new_session_command(
        "gza-owned",
        cwd=tmp_path,
        env=env,
        env_file=env_file,
        cols=120,
        rows=40,
        pane_command=["python", "-m", "gza.tmux_proxy"],
    )

    assert cmd[:5] == ["tmux", "new-session", "-d", "-c", str(tmp_path.resolve())]
    argv_text = "\0".join(cmd)
    for secret in ("anthropic-poison", "codex-poison", "gemini-poison", "arbitrary-poison"):
        assert secret not in argv_text
    assert "-e" not in cmd
    assert str(env_file) in cmd

    env_file_text = env_file.read_text(encoding="utf-8")
    assert "export PATH=/owned/bin" in env_file_text
    assert f"export PWD={tmp_path}" in env_file_text
    assert f"export GZA_DB_PATH={tmp_path / 'owned.db'}" in env_file_text
    assert "export ANTHROPIC_API_KEY=anthropic-poison" in env_file_text
    assert "export CODEX_API_KEY=codex-poison" in env_file_text
    assert "export GEMINI_API_KEY=gemini-poison" in env_file_text
    assert "export PROJECT_ONLY_TOKEN=arbitrary-poison" in env_file_text
    assert "bad-name" not in env_file_text


def test_sanitize_environment_values_masks_arbitrary_runtime_values(tmp_path: Path) -> None:
    env = {
        "ANTHROPIC_API_KEY": "anthropic-poison",
        "CODEX_API_KEY": "codex-poison",
        "GEMINI_API_KEY": "gemini-poison",
        "PROJECT_ONLY_TOKEN": "arbitrary-poison",
        "PWD": str(tmp_path / "project"),
    }
    message = (
        "tmux failed with anthropic-poison codex-poison gemini-poison "
        f"arbitrary-poison in {tmp_path / 'project'}"
    )

    sanitized = sanitize_environment_values(message, env)

    for value in env.values():
        assert value not in sanitized
    assert sanitized.count("[redacted]") == len(env)
