"""Tests for shared failure-reason resolution and ownership."""

import ast
from pathlib import Path

from gza.db import TaskStats
from gza.failure_reasons import resolve_failure_reason

_ALLOWED_FAILURE_REASON_MARK_FAILED_OWNERS = {
    ("src/gza/failure_reasons.py", "mark_task_failed_from_cause"),
}
_ALLOWED_DIRECT_FAILURE_REASON_ASSIGNMENTS = {
    ("src/gza/cli/execution.py", "cmd_set_status"),
    ("src/gza/db.py", "mark_failed"),
}

class _FailureReasonOwnershipVisitor(ast.NodeVisitor):
    def __init__(self, *, repo_root: Path, path: Path) -> None:
        self._repo_root = repo_root
        self._path = path
        self._relative_path = path.relative_to(repo_root).as_posix()
        self._function_stack: list[str] = []
        self.violations: list[str] = []

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
        if isinstance(func, ast.Attribute) and func.attr == "mark_failed":
            if any(keyword.arg == "failure_reason" for keyword in node.keywords):
                if not self._is_allowlisted(_ALLOWED_FAILURE_REASON_MARK_FAILED_OWNERS):
                    self.violations.append(f"{self._relative_path}:{node.lineno}")
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
        if not self._is_allowlisted(_ALLOWED_DIRECT_FAILURE_REASON_ASSIGNMENTS):
            self.violations.append(f"{self._relative_path}:{lineno}")

    def _is_allowlisted(self, allowed_owners: set[tuple[str, str]]) -> bool:
        current_function = self._function_stack[-1] if self._function_stack else None
        return (self._relative_path, current_function) in allowed_owners

    @staticmethod
    def _is_none_literal(value: ast.expr | None) -> bool:
        return isinstance(value, ast.Constant) and value.value is None


def _find_failure_reason_ownership_violations(repo_root: Path, source_root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted(source_root.rglob("*.py")):
        source = path.read_text()
        if "failure_reason" not in source and "mark_failed" not in source:
            continue
        tree = ast.parse(source, filename=str(path))
        visitor = _FailureReasonOwnershipVisitor(repo_root=repo_root, path=path)
        visitor.visit(tree)
        violations.extend(visitor.violations)
    return violations


def test_resolve_failure_reason_uses_reported_turns_when_computed_is_below_limit() -> None:
    reason = resolve_failure_reason(
        error_type="max_turns",
        exit_code=0,
        log_file=None,
        stats=TaskStats(num_turns_computed=49, num_turns_reported=60),
        turn_limit=50,
    )

    assert reason == "MAX_TURNS"


def test_resolve_failure_reason_uses_reported_steps_when_computed_is_below_limit() -> None:
    reason = resolve_failure_reason(
        error_type="max_steps",
        exit_code=0,
        log_file=None,
        stats=TaskStats(num_steps_computed=49, num_steps_reported=60),
        step_limit=50,
    )

    assert reason == "MAX_STEPS"


def test_resolve_failure_reason_maps_provider_error_types() -> None:
    assert resolve_failure_reason(error_type="config_error", exit_code=1, log_file=None) == "CONFIG_ERROR"
    assert (
        resolve_failure_reason(
            error_type="provider_unavailable",
            exit_code=1,
            log_file=None,
        ) == "PROVIDER_UNAVAILABLE"
    )


def test_resolve_failure_reason_log_fallback_preserves_config_error(tmp_path: Path) -> None:
    log_file = tmp_path / "task.log"
    log_file.write_text("provider failed\n[GZA_FAILURE:CONFIG_ERROR]\n", encoding="utf-8")

    assert (
        resolve_failure_reason(
            error_type=None,
            exit_code=1,
            log_file=log_file,
            fallback_to_log=True,
        )
        == "CONFIG_ERROR"
    )


def test_resolve_failure_reason_log_fallback_filters_provider_unavailable(tmp_path: Path) -> None:
    log_file = tmp_path / "task.log"
    log_file.write_text(
        "provider failed\n[GZA_FAILURE:PROVIDER_UNAVAILABLE]\n",
        encoding="utf-8",
    )

    # Provider unavailability remains runner-owned on the fallback path.
    assert (
        resolve_failure_reason(
            error_type=None,
            exit_code=1,
            log_file=log_file,
            fallback_to_log=True,
        )
        == "UNKNOWN"
    )


def test_production_failure_reason_persistence_uses_shared_helper() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source_root = repo_root / "src" / "gza"
    assert _find_failure_reason_ownership_violations(repo_root, source_root) == []


def test_failure_reason_ownership_guard_flags_direct_production_assignment(tmp_path: Path) -> None:
    repo_root = tmp_path
    source_root = repo_root / "src" / "gza"
    source_root.mkdir(parents=True)

    (source_root / "db.py").write_text(
        "def mark_failed(task, failure_reason):\n"
        "    task.failure_reason = failure_reason if failure_reason is not None else \"UNKNOWN\"\n",
        encoding="utf-8",
    )
    (source_root / "cli").mkdir()
    (source_root / "cli" / "execution.py").write_text(
        "def cmd_set_status(task, args):\n"
        "    if args.status == \"failed\" and args.reason:\n"
        "        task.failure_reason = args.reason\n"
        "    elif args.status != \"failed\":\n"
        "        task.failure_reason = None\n",
        encoding="utf-8",
    )
    (source_root / "query.py").write_text(
        "def reset(task):\n"
        "    task.failure_reason = None\n",
        encoding="utf-8",
    )
    (source_root / "bad.py").write_text(
        "def bypass_shared_owner(task):\n"
        "    task.failure_reason = \"TEST_FAILURE\"\n",
        encoding="utf-8",
    )

    assert _find_failure_reason_ownership_violations(repo_root, source_root) == [
        "src/gza/bad.py:2",
    ]


def test_failure_reason_ownership_guard_prefilter_keeps_keyword_and_whitespace_variants(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    source_root = repo_root / "src" / "gza"
    source_root.mkdir(parents=True)

    (source_root / "good.py").write_text(
        "class Recorder:\n"
        "    def mark_failed(self, *, failure_reason=None):\n"
        "        return failure_reason\n\n"
        "def allowed(recorder):\n"
        "    recorder.mark_failed (\n"
        "        failure_reason='TIMEOUT',\n"
        "    )\n",
        encoding="utf-8",
    )
    (source_root / "bad.py").write_text(
        "def bad(task):\n"
        "    task.failure_reason = (\n"
        "        'TIMEOUT'\n"
        "    )\n",
        encoding="utf-8",
    )

    assert _find_failure_reason_ownership_violations(repo_root, source_root) == [
        "src/gza/bad.py:2",
        "src/gza/good.py:6",
    ]


def test_failure_reason_ownership_guard_prefilter_keeps_annotated_assignment_variants(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    source_root = repo_root / "src" / "gza"
    source_root.mkdir(parents=True)

    (source_root / "bad.py").write_text(
        "def bad(task):\n"
        "    task.failure_reason: str = 'TIMEOUT'\n",
        encoding="utf-8",
    )

    assert _find_failure_reason_ownership_violations(repo_root, source_root) == [
        "src/gza/bad.py:2",
    ]
