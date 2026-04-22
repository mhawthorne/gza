"""Flag pytest monkeypatch.setattr calls that mutate stdlib module attributes.

Why: monkeypatching a stdlib symbol (e.g. ``os.get_terminal_size``, ``subprocess.run``,
``sys.stdout.isatty``) mutates it process-wide. Pytest, xdist, and the logging layer
call those same symbols, so a broken patch can crash the test runner itself with
``INTERNALERROR`` instead of a clean test failure — making the root cause hard to spot.

Covered patterns:
  monkeypatch.setattr(<stdlib>, "attr", value)            # e.g. subprocess.run
  monkeypatch.setattr(<stdlib>.<x>, "attr", value)        # e.g. sys.stdout.isatty
  monkeypatch.setattr(<our_mod>.<stdlib>, "attr", value)  # e.g. tv_module.os
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATHS = [Path("tests")]

STDLIB_ROOTS = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "shutil",
        "time",
        "signal",
        "socket",
        "threading",
        "builtins",
        "logging",
        "io",
    }
)


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    col: int
    rule: str
    target: str

    def format(self) -> str:
        return f"{self.path}:{self.line}:{self.col}: {self.rule} monkeypatch.setattr target `{self.target}` touches stdlib"


def _target_names(node: ast.expr) -> list[str]:
    """Return every Name id and Attribute attr appearing in an expression chain."""
    names: list[str] = []
    current: ast.expr | None = node
    while isinstance(current, ast.Attribute):
        names.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        names.append(current.id)
    return names


def _target_source(node: ast.expr) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return "<expr>"


def _is_monkeypatch_setattr(call: ast.Call) -> bool:
    func = call.func
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "setattr"
        and isinstance(func.value, ast.Name)
        and func.value.id == "monkeypatch"
    )


def _suppressed(source_lines: list[str], line: int, rule_id: str) -> bool:
    if 1 <= line <= len(source_lines):
        return f"noqa: {rule_id}" in source_lines[line - 1]
    return False


def check_file(path: Path, rule_id: str) -> list[Violation]:
    text = path.read_text()
    tree = ast.parse(text, filename=str(path))
    source_lines = text.splitlines()
    violations: list[Violation] = []

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_monkeypatch_setattr(node) and node.args):
            continue
        target = node.args[0]
        names = _target_names(target)
        if not any(name in STDLIB_ROOTS for name in names):
            continue
        if _suppressed(source_lines, node.lineno, rule_id):
            continue
        violations.append(
            Violation(
                path=path,
                line=node.lineno,
                col=node.col_offset + 1,
                rule=rule_id,
                target=_target_source(target),
            )
        )

    return violations
