"""Guards the narrow allowlist for production sqlite3.connect call sites."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class _ConnectCallSite:
    path: str
    lineno: int
    function: str | None


class _SqliteConnectVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.function_stack: list[str] = []
        self.call_sites: list[_ConnectCallSite] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "connect"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "sqlite3"
        ):
            self.call_sites.append(
                _ConnectCallSite(
                    path=str(self.path.as_posix()),
                    lineno=node.lineno,
                    function=self.function_stack[-1] if self.function_stack else None,
                )
            )
        self.generic_visit(node)


def _collect_sqlite_connect_call_sites(repo_root: Path) -> list[_ConnectCallSite]:
    src_root = repo_root / "src" / "gza"
    call_sites: list[_ConnectCallSite] = []
    for path in sorted(src_root.rglob("*.py")):
        source = path.read_text()
        if "sqlite3" not in source or "connect" not in source:
            continue
        relative_path = path.relative_to(repo_root)
        visitor = _SqliteConnectVisitor(relative_path)
        visitor.visit(ast.parse(source, filename=str(relative_path)))
        call_sites.extend(visitor.call_sites)
    return call_sites


def _is_allowlisted(call_site: _ConnectCallSite) -> bool:
    if call_site.path == "src/gza/db.py":
        return True
    return (
        call_site.path == "src/gza/runner.py"
        and call_site.function == "_backup_sqlite_file"
    )


def test_production_sqlite_connect_call_sites_stay_within_allowlist() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    call_sites = _collect_sqlite_connect_call_sites(repo_root)

    violations = [call_site for call_site in call_sites if not _is_allowlisted(call_site)]
    assert not violations, (
        "Found disallowed sqlite3.connect call sites outside the documented allowlist: "
        + ", ".join(
            f"{call_site.path}:{call_site.lineno} ({call_site.function or '<module>'})"
            for call_site in violations
        )
    )

    runner_backup_calls = [
        call_site
        for call_site in call_sites
        if call_site.path == "src/gza/runner.py" and call_site.function == "_backup_sqlite_file"
    ]
    assert runner_backup_calls, "Expected runner backup sqlite3.connect allowlist entry to exist."


def test_collect_sqlite_connect_call_sites_detects_whitespace_around_attribute_access(
    tmp_path: Path,
) -> None:
    src_root = tmp_path / "src" / "gza"
    src_root.mkdir(parents=True)
    module_path = src_root / "sample.py"
    module_path.write_text(
        "import sqlite3\n\n"
        "def disallowed_connect() -> None:\n"
        "    sqlite3 . connect(':memory:')\n"
    )

    assert _collect_sqlite_connect_call_sites(tmp_path) == [
        _ConnectCallSite(
            path="src/gza/sample.py",
            lineno=4,
            function="disallowed_connect",
        )
    ]
