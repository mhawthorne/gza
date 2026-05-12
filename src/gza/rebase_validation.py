"""Shared validation helpers for provider-backed rebase resolution."""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .git import Git


class RebaseValidationLogger(Protocol):
    """Minimal logger contract for surfacing validation failures."""

    def error(self, message: str, *, extra: dict | None = None) -> None: ...


@dataclass(frozen=True)
class RuffDiagnostic:
    path: str
    line: int
    column: int
    code: str


def tracked_python_files(git: Git) -> list[Path]:
    result = git._run("ls-files", "--", "*.py", check=False)
    return sorted(
        git.repo_dir / line
        for line in result.stdout.splitlines()
        if line.endswith(".py")
    )


def parse_ruff_diagnostics(output: str, repo_dir: Path) -> set[RuffDiagnostic]:
    diagnostics: set[RuffDiagnostic] = set()
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("Found ") or line == "All checks passed!":
            continue
        parts = line.split(":", 3)
        if len(parts) != 4:
            continue
        path_str, line_str, column_str, rest = parts
        code = rest.strip().split(" ", 1)[0]
        if code not in {"F401", "F821"}:
            continue
        try:
            rel_path = str(Path(path_str).resolve().relative_to(repo_dir.resolve()))
        except ValueError:
            rel_path = path_str
        diagnostics.add(
            RuffDiagnostic(
                path=rel_path,
                line=int(line_str),
                column=int(column_str),
                code=code,
            )
        )
    return diagnostics


def run_selected_ruff_check(repo_dir: Path, files: list[Path]) -> tuple[set[RuffDiagnostic], str]:
    if not files:
        return set(), ""

    cmd = [
        sys.executable,
        "-m",
        "ruff",
        "check",
        "--select",
        "F401,F821",
        "--output-format",
        "concise",
        *[str(path) for path in files],
    ]
    result = subprocess.run(cmd, cwd=repo_dir, capture_output=True, text=True)
    output = result.stdout.strip()
    error_output = result.stderr.strip()
    if result.returncode not in (0, 1):
        details = error_output or output or f"ruff exited with status {result.returncode}"
        raise RuntimeError(details)
    return parse_ruff_diagnostics(output, repo_dir), output


def capture_rebase_validation_baseline(git: Git) -> tuple[str, set[RuffDiagnostic]]:
    before_head = git.rev_parse("HEAD")
    pre_existing_diagnostics, _output = run_selected_ruff_check(
        git.repo_dir,
        tracked_python_files(git),
    )
    return before_head, pre_existing_diagnostics


def is_rebase_in_progress(worktree_path: Path) -> bool:
    """Check whether git still reports an in-progress rebase for this checkout."""
    git_file = worktree_path / ".git"
    if git_file.is_file():
        try:
            git_dir_text = git_file.read_text().strip()
            if git_dir_text.startswith("gitdir: "):
                raw = git_dir_text[len("gitdir: "):]
                git_dir: Path = Path(raw) if Path(raw).is_absolute() else (worktree_path / raw).resolve()
            else:
                git_dir = git_file
        except OSError:
            git_dir = worktree_path / ".git"
    else:
        git_dir = worktree_path / ".git"
    return (git_dir / "rebase-merge").exists() or (git_dir / "rebase-apply").exists()


def changed_python_files_since_head(git: Git, before_head: str) -> list[Path]:
    changed_paths: set[str] = set()

    after_head = git.rev_parse("HEAD")
    if after_head != before_head:
        result = git._run(
            "diff",
            "--name-only",
            "--diff-filter=ACMRTUXB",
            f"{before_head}..{after_head}",
            check=False,
        )
        changed_paths.update(line for line in result.stdout.splitlines() if line.endswith(".py"))

    for _status, filepath in git.status_porcelain():
        if filepath.endswith(".py"):
            changed_paths.add(filepath)

    return sorted(git.repo_dir / rel_path for rel_path in changed_paths if (git.repo_dir / rel_path).exists())


def validate_rebase_resolution_output(
    *,
    git: Git,
    before_head: str,
    pre_existing_diagnostics: set[RuffDiagnostic],
    task_logger: RebaseValidationLogger,
) -> bool:
    changed_files = changed_python_files_since_head(git, before_head)
    if not changed_files:
        return True

    try:
        post_diagnostics, _output = run_selected_ruff_check(git.repo_dir, changed_files)
    except RuntimeError as exc:
        task_logger.error(f"Post-rebase ruff validation failed to run: {exc}")
        return False

    changed_rel_paths = {
        str(path.resolve().relative_to(git.repo_dir.resolve()))
        for path in changed_files
    }
    baseline_for_changed = {
        diagnostic for diagnostic in pre_existing_diagnostics if diagnostic.path in changed_rel_paths
    }
    new_diagnostics = sorted(
        post_diagnostics - baseline_for_changed,
        key=lambda diagnostic: (diagnostic.path, diagnostic.line, diagnostic.column, diagnostic.code),
    )
    if not new_diagnostics:
        return True

    task_logger.error(
        "Post-rebase ruff validation found new F401/F821 diagnostics in provider-touched files."
    )
    for diagnostic in new_diagnostics:
        task_logger.error(
            f"  {diagnostic.path}:{diagnostic.line}:{diagnostic.column}: {diagnostic.code}"
        )
    return False
