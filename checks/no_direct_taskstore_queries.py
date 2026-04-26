"""Flag direct task-store query calls in high-level CLI/API presentation code."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATHS = [
    Path("src/gza/api/v0.py"),
    Path("src/gza/cli/query.py"),
    Path("src/gza/cli/watch.py"),
]

QUERY_METHODS = frozenset(
    {
        "get_all",
        "get_history",
        "get_in_progress",
        "get_pending",
        "get_pending_pickup",
    }
)

TARGET_FUNCTIONS = {
    "src/gza/api/v0.py": {
        "get_history",
        "get_in_progress",
        "get_incomplete",
        "get_pending",
        "get_recent_completed",
    },
    "src/gza/cli/query.py": {"cmd_next"},
    "src/gza/cli/watch.py": {"cmd_queue"},
}


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    col: int
    rule: str
    method: str

    def format(self) -> str:
        return (
            f"{self.path}:{self.line}:{self.col}: {self.rule} direct SqliteTaskStore query "
            f"`{self.method}()` bypasses TaskQueryService"
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
    target_functions = next(
        (functions for target_path, functions in TARGET_FUNCTIONS.items() if path.as_posix().endswith(target_path)),
        None,
    )
    if not target_functions:
        return violations

    class _Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.function_stack: list[str] = []

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.function_stack.append(node.name)
            self.generic_visit(node)
            self.function_stack.pop()

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self.function_stack.append(node.name)
            self.generic_visit(node)
            self.function_stack.pop()

        def visit_Call(self, node: ast.Call) -> None:
            current_function = self.function_stack[-1] if self.function_stack else None
            if (
                current_function in target_functions
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in QUERY_METHODS
                and not _suppressed(source_lines, node.lineno, rule_id)
            ):
                violations.append(
                    Violation(
                        path=path,
                        line=node.lineno,
                        col=node.col_offset + 1,
                        rule=rule_id,
                        method=node.func.attr,
                    )
                )
            self.generic_visit(node)

    _Visitor().visit(tree)

    return violations
