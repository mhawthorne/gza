"""Keep shell-backed CLI and git tests out of the unit suite."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path

DEFAULT_PATHS = [Path("tests")]

_DIRECT_GIT_RUN_CALL = re.compile(r"\.\s*_run\s*\(")
_SUBPROCESS_NAME = re.compile(r"\bsubprocess\b")
_SUBPROCESS_IMPORT_CALL = re.compile(r"\bfrom\s+subprocess\s+import\s+(?:run|Popen|check_call|check_output)\b")
_SUBPROCESS_MODULE_IMPORT = re.compile(r"\bimport\s+subprocess\b")
_FORBIDDEN_SUBPROCESS_CALLS = {"run", "Popen", "check_call", "check_output"}


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str

    def format(self) -> str:
        return f"{self.path}:{self.line} {self.message}"


def _has_direct_git_run_candidate(source: str) -> bool:
    return _DIRECT_GIT_RUN_CALL.search(source) is not None


def _has_cli_subprocess_candidate(source: str) -> bool:
    return _SUBPROCESS_NAME.search(source) is not None and (
        "sys.executable" in source
        or "'gza'" in source
        or '"gza"' in source
        or "'uv'" in source
        or '"uv"' in source
    )


def _has_git_subprocess_candidate(source: str) -> bool:
    return _SUBPROCESS_NAME.search(source) is not None and (
        "'git'" in source
        or '"git"' in source
    )


def _has_subprocess_alias_candidate(source: str) -> bool:
    return _SUBPROCESS_IMPORT_CALL.search(source) is not None or _SUBPROCESS_MODULE_IMPORT.search(source) is not None


def iter_candidate_files(tests_root: Path) -> list[Path]:
    candidates: set[Path] = set(tests_root.rglob("test_*.py"))
    candidates.update(tests_root.rglob("conftest.py"))

    helpers_root = tests_root / "helpers"
    if helpers_root.exists():
        candidates.update(helpers_root.rglob("*.py"))

    return sorted(candidates)


def _find_file_violations(test_file: Path) -> list[Violation]:
    source = test_file.read_text()
    has_direct_git_run_call = _has_direct_git_run_candidate(source)
    has_cli_subprocess_call = _has_cli_subprocess_candidate(source)
    has_git_subprocess_call = _has_git_subprocess_candidate(source)
    has_generic_subprocess_candidate = _SUBPROCESS_NAME.search(source) is not None
    has_subprocess_alias_call = _has_subprocess_alias_candidate(source)
    has_legacy_cli_helper = "run_gza" in source or "run_gza_subprocess" in source
    has_tests_functional_import = "tests_functional" in source
    if not (
        has_direct_git_run_call
        or has_cli_subprocess_call
        or has_git_subprocess_call
        or has_generic_subprocess_candidate
        or has_subprocess_alias_call
        or has_legacy_cli_helper
        or has_tests_functional_import
    ):
        return []

    module = ast.parse(source, filename=str(test_file))
    subprocess_names = {
        alias.asname or alias.name
        for node in ast.walk(module)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name == "subprocess"
    }
    parent_map = (
        {child: parent for parent in ast.walk(module) for child in ast.iter_child_nodes(parent)}
        if has_direct_git_run_call
        or has_git_subprocess_call
        or has_generic_subprocess_candidate
        or has_subprocess_alias_call
        else {}
    )
    subprocess_module_aliases = {"subprocess"}
    subprocess_callable_aliases: dict[str, str] = {}
    violations: list[Violation] = []

    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "subprocess":
                    subprocess_module_aliases.add(alias.asname or alias.name)
            continue

        if isinstance(node, ast.ImportFrom):
            if node.module == "subprocess":
                for alias in node.names:
                    if alias.name not in _FORBIDDEN_SUBPROCESS_CALLS:
                        continue
                    bound_name = alias.asname or alias.name
                    subprocess_callable_aliases[bound_name] = alias.name
                    violations.append(
                        Violation(
                            path=test_file,
                            line=node.lineno,
                            message=(
                                "direct subprocess callable imports are banned in tests/ because they "
                                "bypass the unit-suite runtime guard; import subprocess and patch the "
                                "seam instead, or move the test to tests_functional/"
                            ),
                        )
                    )
            if node.module in {"tests.helpers.cli", "tests.cli.conftest"}:
                for alias in node.names:
                    if alias.name == "run_gza":
                        violations.append(
                            Violation(
                                path=test_file,
                                line=node.lineno,
                                message="legacy run_gza helper is banned in tests/; use invoke_gza or move the test to tests_functional/",
                            )
                        )
                    if alias.name == "run_gza_subprocess":
                        violations.append(
                            Violation(
                                path=test_file,
                                line=node.lineno,
                                message="CLI subprocess helper belongs in tests_functional/",
                            )
                        )
            if node.module == "tests_functional" or (
                node.module and node.module.startswith("tests_functional.")
            ):
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="unit tests must not import tests_functional modules",
                    )
                )
            continue

        if not isinstance(node, ast.Call):
            continue

        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in {"run", "Popen", "check_call", "check_output"}
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in subprocess_module_aliases
        ):
            if not node.args:
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="subprocess-backed test belongs in tests_functional/",
                    )
                )
                continue

            command_arg = node.args[0]
            parts: list[str | None] | None = None
            if isinstance(command_arg, ast.List):
                parts = []
                for elt in command_arg.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        parts.append(elt.value)
                    elif (
                        isinstance(elt, ast.Attribute)
                        and isinstance(elt.value, ast.Name)
                        and elt.value.id == "sys"
                    ):
                        parts.append(f"sys.{elt.attr}")
                    else:
                        parts.append(None)

            if parts is not None and (
                parts[:3] == ["uv", "run", "gza"] or parts[:3] == ["sys.executable", "-m", "gza"]
            ):
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="CLI subprocess invocation belongs in tests_functional/",
                    )
                )
                continue

            if parts is not None and parts[:1] == ["git"]:
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="direct git subprocess belongs in tests_functional/",
                    )
                )
                continue

            if isinstance(command_arg, ast.Name):
                flagged_direct_git = False
                current = parent_map.get(node)
                while current is not None:
                    if (
                        isinstance(current, ast.For)
                        and isinstance(current.target, ast.Name)
                        and current.target.id == command_arg.id
                        and isinstance(current.iter, (ast.Tuple, ast.List))
                    ):
                        for candidate in current.iter.elts:
                            if (
                                isinstance(candidate, ast.List)
                                and candidate.elts
                                and isinstance(candidate.elts[0], ast.Constant)
                                and candidate.elts[0].value == "git"
                            ):
                                violations.append(
                                    Violation(
                                        path=test_file,
                                        line=node.lineno,
                                        message="direct git subprocess belongs in tests_functional/",
                                    )
                                )
                                flagged_direct_git = True
                                break
                        break
                    current = parent_map.get(current)
                if flagged_direct_git:
                    continue

            violations.append(
                Violation(
                    path=test_file,
                    line=node.lineno,
                    message="subprocess-backed test belongs in tests_functional/",
                )
            )
            continue

        if isinstance(node.func, ast.Name):
            if node.func.id in subprocess_callable_aliases:
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message=(
                            f"subprocess.{subprocess_callable_aliases[node.func.id]} alias calls belong in "
                            "tests_functional/ unless patched through an in-process seam"
                        ),
                    )
                )
                continue
            if node.func.id == "run_gza":
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="legacy run_gza helper is banned in tests/; use invoke_gza or move the test to tests_functional/",
                    )
                )
                continue
            if node.func.id == "run_gza_subprocess":
                violations.append(
                    Violation(
                        path=test_file,
                        line=node.lineno,
                        message="CLI subprocess helper belongs in tests_functional/",
                    )
                )
                continue

        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "_run"
        ):
            continue

        current = parent_map.get(node)
        function_name: str | None = None
        class_name: str | None = None
        while current is not None:
            if function_name is None and isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                function_name = current.name
            if class_name is None and isinstance(current, ast.ClassDef):
                class_name = current.name
            current = parent_map.get(current)

        if (
            function_name is not None
            and (
                (test_file.name == "test_git.py" and class_name == "TestGitRun")
                or (test_file.name == "test_github.py" and class_name == "TestGitHubRun")
            )
        ):
            continue

        violations.append(
            Violation(
                path=test_file,
                line=node.lineno,
                message="direct Git._run shell command belongs in tests_functional/",
            )
        )

    return violations


@cache
def _find_file_violations_cached(
    test_file: Path,
    *,
    mtime_ns: int,
    size: int,
) -> tuple[Violation, ...]:
    del mtime_ns, size
    return tuple(_find_file_violations(test_file))


def find_unit_suite_boundary_violations(tests_root: Path) -> list[Violation]:
    violations: list[Violation] = []
    for path in iter_candidate_files(tests_root):
        stat = path.stat()
        violations.extend(
            _find_file_violations_cached(path, mtime_ns=stat.st_mtime_ns, size=stat.st_size)
        )
    return violations


def check_file(path: Path, _rule_id: str) -> list[Violation]:
    if "tests" not in path.parts:
        return []
    return _find_file_violations(path)
