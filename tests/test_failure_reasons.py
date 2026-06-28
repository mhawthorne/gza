"""Tests for shared failure-reason resolution and ownership."""

import sqlite3
from pathlib import Path

import pytest

from checks.failure_reason_ownership import check_file as check_failure_reason_ownership
from gza.db import TaskStats
from gza.failure_reasons import (
    is_readonly_db_failure,
    preserves_failure_reason_over_terminal_no_work,
    resolve_failure_reason,
)


def _find_failure_reason_ownership_violations(repo_root: Path, source_root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted(source_root.rglob("*.py")):
        for violation in check_failure_reason_ownership(path, "failure_reason_ownership"):
            relative_path = violation.path.relative_to(repo_root).as_posix()
            violations.append(f"{relative_path}:{violation.line}")
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


@pytest.mark.parametrize(
    ("failure_reason", "expected"),
    [
        ("MAX_TURNS", True),
        ("MAX_STEPS", True),
        ("TIMEOUT", True),
        ("TERMINATED", True),
        ("PROVIDER_UNAVAILABLE", True),
        ("CONFIG_ERROR", False),
        ("TERMINAL_NO_WORK", False),
    ],
)
def test_preserves_failure_reason_over_terminal_no_work(
    failure_reason: str,
    expected: bool,
) -> None:
    assert preserves_failure_reason_over_terminal_no_work(failure_reason) is expected


def test_resolve_failure_reason_maps_provider_error_types() -> None:
    assert resolve_failure_reason(error_type="config_error", exit_code=1, log_file=None) == "CONFIG_ERROR"
    assert (
        resolve_failure_reason(
            error_type="provider_unavailable",
            exit_code=1,
            log_file=None,
        ) == "PROVIDER_UNAVAILABLE"
    )
    assert (
        resolve_failure_reason(
            error_type="retryable_provider_error",
            exit_code=1,
            log_file=None,
        ) == "RETRYABLE_PROVIDER_ERROR"
    )
    assert (
        resolve_failure_reason(
            error_type="infrastructure_error",
            exit_code=125,
            log_file=None,
        ) == "INFRASTRUCTURE_ERROR"
    )


def test_resolve_failure_reason_prefers_infrastructure_error_before_log_fallback(tmp_path: Path) -> None:
    log_file = tmp_path / "task.log"
    log_file.write_text("", encoding="utf-8")

    assert (
        resolve_failure_reason(
            error_type="infrastructure_error",
            exit_code=137,
            log_file=log_file,
            fallback_to_log=True,
        )
        == "INFRASTRUCTURE_ERROR"
    )


@pytest.mark.parametrize(
    ("error_or_message", "expected"),
    [
        (sqlite3.OperationalError("attempt to write a readonly database"), True),
        (sqlite3.OperationalError("database is locked"), False),
    ],
)
def test_is_readonly_db_failure_classifies_sqlite_operational_error_variants(
    error_or_message: BaseException,
    expected: bool,
) -> None:
    assert is_readonly_db_failure(error_or_message) is expected


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


def test_resolve_failure_reason_log_fallback_filters_terminal_no_work(tmp_path: Path) -> None:
    log_file = tmp_path / "task.log"
    log_file.write_text(
        "runner classified branch\n[GZA_FAILURE:TERMINAL_NO_WORK]\n",
        encoding="utf-8",
    )

    assert (
        resolve_failure_reason(
            error_type=None,
            exit_code=1,
            log_file=log_file,
            fallback_to_log=True,
        )
        == "UNKNOWN"
    )


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
