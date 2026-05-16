"""Helpers for identifying Gza-managed worktree roots."""

from pathlib import Path

from .config import Config


def managed_worktree_root_paths(config: Config) -> list[Path]:
    """Return configured root paths where Gza may manage live worktrees."""
    roots = [config.worktree_path]

    interactive_dir_value = getattr(config, "interactive_worktree_dir", "")
    interactive_dir = interactive_dir_value.strip() if isinstance(interactive_dir_value, str) else ""
    if interactive_dir:
        interactive_path = Path(interactive_dir)
        if not interactive_path.is_absolute():
            interactive_path = config.project_dir / interactive_path
        roots.append(interactive_path)

    return roots
