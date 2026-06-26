"""Keep failure_reason writes owned by the shared helper paths."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATHS = [Path("src/gza")]

_ALLOWED_FAILURE_REASON_MARK_FAILED_OWNERS = {
    ("src/gza/failure_reasons.py", "mark_task_failed_from_cause"),
}
_ALLOWED_DIRECT_FAILURE_REASON_ASSIGNMENTS = {
    ("src/gza/cli/execution.py", "cmd_set_status"),
    ("src/gza/db.py", "mark_failed"),
}


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str

    def format(self) -> str:
        return f"{self.path}:{self.line} {self.message}"


def _normalized_path(path: Path) -> str:
    parts = path.parts
    for prefix in (("src", "gza"),):
        for index in range(len(parts) - len(prefix) + 1):
            if parts[index : index + len(prefix)] == prefix:
                return Path(*parts[index:]).as_posix()
    try:
        return path.relative_to(Path.cwd()).as_posix()
    except ValueError:
        return path.as_posix()


class _FailureReasonOwnershipVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._normalized_path = _normalized_path(path)
        self._function_stack: list[str] = []
        self.violations: list[Violation] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._function_stack.append(node.name)
        self.generic_visit(node)
        self._function_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "mark_failed"
            and any(keyword.arg == "failure_reason" for keyword in node.keywords)
            and not self._is_allowlisted(_ALLOWED_FAILURE_REASON_MARK_FAILED_OWNERS)
        ):
            self.violations.append(
                Violation(
                    path=self._path,
                    line=node.lineno,
                    message=(
                        "failure_reason writes must flow through the shared ownership helper; "
                        "only allowlisted owners may pass failure_reason= to mark_failed()"
                    ),
                )
            )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._check_failure_reason_assignment(target, node.value, node.lineno)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._check_failure_reason_assignment(node.target, node.value, node.lineno)
        self.generic_visit(node)

    def _check_failure_reason_assignment(
        self,
        target: ast.expr,
        value: ast.expr | None,
        lineno: int,
    ) -> None:
        if not isinstance(target, ast.Attribute) or target.attr != "failure_reason":
            return
        if self._is_none_literal(value):
            return
        if self._is_allowlisted(_ALLOWED_DIRECT_FAILURE_REASON_ASSIGNMENTS):
            return
        self.violations.append(
            Violation(
                path=self._path,
                line=lineno,
                message=(
                    "failure_reason writes must stay in the shared ownership paths; use the "
                    "shared helper instead of assigning task.failure_reason directly"
                ),
            )
        )

    def _is_allowlisted(self, allowed_owners: set[tuple[str, str]]) -> bool:
        current_function = self._function_stack[-1] if self._function_stack else None
        return (self._normalized_path, current_function) in allowed_owners

    @staticmethod
    def _is_none_literal(value: ast.expr | None) -> bool:
        return isinstance(value, ast.Constant) and value.value is None


def check_file(path: Path, _rule_id: str) -> list[Violation]:
    source = path.read_text()
    if "failure_reason" not in source and "mark_failed" not in source:
        return []

    tree = ast.parse(source, filename=str(path))
    visitor = _FailureReasonOwnershipVisitor(path)
    visitor.visit(tree)
    return visitor.violations
