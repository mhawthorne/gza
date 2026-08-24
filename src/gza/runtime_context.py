"""Runtime-owned execution context helpers."""

from __future__ import annotations

import os
import re
import shlex
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from .config import APP_NAME, DEFAULT_DB_FILE

# Variables that can redirect repository discovery, object lookup, ref views,
# ancestry calculation, or dynamic Git configuration. Auth/identity variables
# such as GIT_SSH_COMMAND, GIT_ASKPASS, GIT_AUTHOR_*, and GIT_COMMITTER_* remain
# available to runtime subprocesses.
GIT_REPOSITORY_ENV_KEYS = frozenset(
    {
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CEILING_DIRECTORIES",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_DIR",
        "GIT_GRAFT_FILE",
        "GIT_INDEX_FILE",
        "GIT_NAMESPACE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_REPLACE_REF_BASE",
        "GIT_SHALLOW_FILE",
        "GIT_WORK_TREE",
    }
)
DOTENV_PROTECTED_ENV_KEYS = frozenset({"PWD"}) | GIT_REPOSITORY_ENV_KEYS
CONFIG_BOOTSTRAP_ENV_KEYS = frozenset({"GZA_DB_PATH", "HOME"}) | DOTENV_PROTECTED_ENV_KEYS
_TMUX_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_git_repository_env_key(key: str) -> bool:
    return key in GIT_REPOSITORY_ENV_KEYS or key.startswith("GIT_CONFIG_")


def _read_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def build_dotenv_runtime_env(
    project_dir: Path,
    *,
    base_env: Mapping[str, str] | None = None,
    home_dir: Path | None = None,
    protected_keys: frozenset[str] | set[str] | tuple[str, ...] = frozenset(),
) -> dict[str, str]:
    """Build the environment a project runtime should use without mutating os.environ."""
    env = dict(os.environ if base_env is None else base_env)
    home = Path.home() if home_dir is None else home_dir
    protected = set(DOTENV_PROTECTED_ENV_KEYS)
    protected.update(protected_keys)

    for key, value in _read_dotenv(home / f".{APP_NAME}" / ".env").items():
        if key not in protected and not _is_git_repository_env_key(key):
            env.setdefault(key, value)
    for dotenv_path in (project_dir / ".env", project_dir / f".{APP_NAME}" / ".env"):
        for key, value in _read_dotenv(dotenv_path).items():
            if key not in protected and not _is_git_repository_env_key(key):
                env[key] = value
    return env


def normalize_subprocess_env(env: Mapping[str, str] | None, cwd: str | os.PathLike[str] | None) -> dict[str, str]:
    """Return an env owned by the explicit subprocess cwd/repository boundary."""
    normalized = dict(os.environ if env is None else env)
    for key in list(normalized):
        if _is_git_repository_env_key(key):
            normalized.pop(key, None)
    resolved_cwd = Path.cwd().resolve() if cwd is None else Path(cwd).resolve()
    normalized["PWD"] = str(resolved_cwd)
    return normalized


def build_tmux_new_session_command(
    session_name: str,
    *,
    cwd: str | os.PathLike[str],
    env: Mapping[str, str] | None = None,
    env_file: str | os.PathLike[str] | None = None,
    cols: int,
    rows: int,
    pane_command: list[str],
) -> list[str]:
    """Build a tmux new-session command without serializing env values into argv."""
    resolved_cwd = str(Path(cwd).resolve())
    effective_pane_command = list(pane_command)
    if env_file is not None:
        effective_pane_command = [
            "sh",
            "-c",
            '. "$1"; rm -f "$1"; shift; exec "$@"',
            "gza-tmux-env",
            str(env_file),
            *effective_pane_command,
        ]
    tmux_cmd = [
        "tmux",
        "new-session",
        "-d",
        "-c",
        resolved_cwd,
    ]
    tmux_cmd.extend(
        [
            "-s",
            session_name,
            "-x",
            str(cols),
            "-y",
            str(rows),
            "--",
            *effective_pane_command,
        ]
    )
    return tmux_cmd


def write_tmux_environment_file(
    *,
    cwd: str | os.PathLike[str],
    env: Mapping[str, str],
    prefix: str = "tmux-env-",
) -> Path:
    """Write a private shell env file for a tmux pane to source."""
    tmp_dir = Path(cwd) / ".gza" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fd, raw_path = tempfile.mkstemp(prefix=prefix, suffix=".sh", dir=tmp_dir)
    path = Path(raw_path)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            for key in sorted(env):
                if _TMUX_ENV_NAME_RE.match(key):
                    f.write(f"export {key}={shlex.quote(str(env[key]))}\n")
        path.chmod(0o600)
    except Exception:
        try:
            path.unlink()
        finally:
            raise
    return path


def sanitize_environment_values(text: object, env: Mapping[str, str] | None) -> str:
    """Redact environment values from output that may include command argv or stderr."""
    sanitized = str(text)
    if env is None:
        return sanitized
    values = sorted(
        {str(value) for value in env.values() if value is not None and str(value)},
        key=len,
        reverse=True,
    )
    for value in values:
        sanitized = sanitized.replace(value, "[redacted]")
    return sanitized


@dataclass(frozen=True)
class RuntimeExecutionContext:
    """Explicit cwd/env/identity bundle for worker launch and control."""

    cwd: Path
    env: dict[str, str]
    project_id: str
    db_path: Path

    @classmethod
    def from_config(cls, config: object) -> RuntimeExecutionContext:
        project_dir = Path(getattr(config, "project_dir"))
        db_path = _resolved_config_db_path(config, project_dir)
        env = build_dotenv_runtime_env(
            project_dir,
            protected_keys=CONFIG_BOOTSTRAP_ENV_KEYS,
        )
        env["GZA_DB_PATH"] = str(db_path)
        env = normalize_subprocess_env(env, project_dir)
        return cls(
            cwd=project_dir,
            env=env,
            project_id=str(getattr(config, "project_id", "")),
            db_path=db_path,
        )

    @property
    def identity_digest(self) -> str:
        seed = f"{self.db_path}\n{self.project_id}".encode()
        return sha256(seed).hexdigest()[:12]


def runtime_tmux_session_name(
    *,
    task_id: str,
    project_id: str,
    db_path: Path,
    session_kind: str = "worker",
) -> str:
    """Return a tmux session name unique to the execution project and task."""
    digest = sha256(f"{Path(db_path).resolve()}\n{project_id}".encode()).hexdigest()[:12]
    project_key = "".join(ch for ch in project_id if ch.isalnum())[:20] or "project"
    if session_kind == "worker":
        return f"gza-{project_key}-{task_id}-{digest}"
    kind_key = "".join(ch for ch in session_kind if ch.isalnum())[:20] or "session"
    return f"gza-{kind_key}-{project_key}-{task_id}-{digest}"


def _resolved_config_db_path(config: object, project_dir: Path) -> Path:
    db_path = getattr(config, "db_path", None)
    try:
        if db_path is not None:
            return Path(db_path).resolve()
    except TypeError:
        pass
    return (project_dir / DEFAULT_DB_FILE).resolve()
