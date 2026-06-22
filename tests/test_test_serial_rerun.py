from __future__ import annotations

from unittest.mock import Mock

import pytest

import gza.test_serial_rerun as test_serial_rerun


def _pass_result(
    *,
    exit_code: int,
    failed_nodeids: list[str] | None = None,
    collection_errors: list[str] | None = None,
    internal_errors: list[str] | None = None,
) -> test_serial_rerun._PytestPassResult:
    return test_serial_rerun._PytestPassResult(
        exit_code=exit_code,
        failed_nodeids=list(failed_nodeids or []),
        collection_errors=list(collection_errors or []),
        internal_errors=list(internal_errors or []),
    )


def test_failure_capture_plugin_records_unique_per_test_failures_only() -> None:
    plugin = test_serial_rerun._FailureCapturePlugin()
    setup_fail = Mock(failed=True, when="setup", nodeid="tests/test_sample.py::test_case")
    call_fail_duplicate = Mock(failed=True, when="call", nodeid="tests/test_sample.py::test_case")
    teardown_fail = Mock(failed=True, when="teardown", nodeid="tests/test_other.py::test_other")
    passed = Mock(failed=False, when="call", nodeid="tests/test_pass.py::test_pass")

    plugin.pytest_runtest_logreport(setup_fail)
    plugin.pytest_runtest_logreport(call_fail_duplicate)
    plugin.pytest_runtest_logreport(teardown_fail)
    plugin.pytest_runtest_logreport(passed)

    assert plugin.failed_nodeids == [
        "tests/test_sample.py::test_case",
        "tests/test_other.py::test_other",
    ]


def test_run_unit_phase_returns_parallel_green_without_serial_rerun(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_pass = Mock(return_value=(_pass_result(exit_code=0), "latency: p50=1ms p95=1ms p99=1ms max=1ms n=1"))
    monkeypatch.setattr(test_serial_rerun, "_run_pytest_pass", run_pass)

    exit_code = test_serial_rerun.run_unit_phase(["tests/", "-n", "2"], cap=2, rerun_enabled=True, emit_summary=True)

    assert exit_code == 0
    assert run_pass.call_count == 1
    assert run_pass.call_args.args[0][-1] == "--maxfail=3"
    captured = capsys.readouterr()
    assert captured.out == "latency: p50=1ms p95=1ms p99=1ms max=1ms n=1\n"
    assert captured.err == ""


def test_run_unit_phase_serial_rerun_masks_parallel_only_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[list[str]] = []
    results = iter(
        [
            (_pass_result(exit_code=1, failed_nodeids=["tests/test_sample.py::test_flake"]), "latency: p50=1ms n=1"),
            (_pass_result(exit_code=0), None),
        ]
    )

    def _fake_run(pytest_args: list[str], *, emit_sigterm_summary: bool) -> tuple[test_serial_rerun._PytestPassResult, str | None]:
        del emit_sigterm_summary
        calls.append(pytest_args)
        return next(results)

    monkeypatch.setattr(test_serial_rerun, "_run_pytest_pass", _fake_run)

    exit_code = test_serial_rerun.run_unit_phase(
        ["tests/", "-n", "2", "-o", "faulthandler_timeout=60"],
        cap=2,
        rerun_enabled=True,
        emit_summary=True,
    )

    assert exit_code == 0
    assert calls == [
        ["tests/", "-n", "2", "-o", "faulthandler_timeout=60", "--maxfail=3"],
        ["tests/test_sample.py::test_flake", "-n0", "-v", "--maxfail=0", "-o", "faulthandler_timeout=60"],
    ]
    captured = capsys.readouterr()
    assert "parallel pass failed; 1 test(s) failed and are within cap 2; re-running serially:" in captured.err
    assert "PARALLEL-ONLY FAILURE (passed serially): tests/test_sample.py::test_flake" in captured.err


def test_run_unit_phase_does_not_mask_over_cap_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_pass = Mock(
        return_value=(
            _pass_result(
                exit_code=1,
                failed_nodeids=["a", "b", "c"],
            ),
            None,
        )
    )
    monkeypatch.setattr(test_serial_rerun, "_run_pytest_pass", run_pass)

    exit_code = test_serial_rerun.run_unit_phase(["tests/"], cap=2, rerun_enabled=True, emit_summary=False)

    assert exit_code == 1
    assert run_pass.call_count == 1
    assert "NOT masking - over cap (3 > 2)" in capsys.readouterr().err


def test_run_unit_phase_does_not_mask_non_rerunnable_parallel_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_pass = Mock(
        return_value=(
            _pass_result(exit_code=1, collection_errors=["tests/test_bad.py"]),
            None,
        )
    )
    monkeypatch.setattr(test_serial_rerun, "_run_pytest_pass", run_pass)

    exit_code = test_serial_rerun.run_unit_phase(["tests/"], cap=2, rerun_enabled=True, emit_summary=False)

    assert exit_code == 1
    assert run_pass.call_count == 1
    assert "NOT masking - collection errors are not rerunnable: tests/test_bad.py" in capsys.readouterr().err


def test_run_unit_phase_reports_confirmed_failures_from_serial_rerun(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    results = iter(
        [
            (_pass_result(exit_code=1, failed_nodeids=["tests/test_sample.py::test_broken"]), None),
            (_pass_result(exit_code=1, failed_nodeids=["tests/test_sample.py::test_broken"]), None),
        ]
    )
    monkeypatch.setattr(
        test_serial_rerun,
        "_run_pytest_pass",
        lambda pytest_args, *, emit_sigterm_summary: next(results),
    )

    exit_code = test_serial_rerun.run_unit_phase(["tests/"], cap=2, rerun_enabled=True, emit_summary=False)

    assert exit_code == 1
    assert "CONFIRMED FAILURE (failed serially too): tests/test_sample.py::test_broken" in capsys.readouterr().err


def test_main_rejects_invalid_env_values(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("GZA_UNIT_RERUN_CAP", "0")

    exit_code = test_serial_rerun.main([])

    assert exit_code == 2
    assert "GZA_UNIT_RERUN_CAP must be a positive integer" in capsys.readouterr().err


def test_main_honors_disable_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GZA_UNIT_SERIAL_RERUN", "0")
    run_unit_phase = Mock(return_value=0)
    monkeypatch.setattr(test_serial_rerun, "run_unit_phase", run_unit_phase)

    exit_code = test_serial_rerun.main(["--", "tests/", "-n", "2"])

    assert exit_code == 0
    assert run_unit_phase.call_args.kwargs["rerun_enabled"] is False
