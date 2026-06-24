from pathlib import Path

import conftest as root_conftest
import pytest

pytest_plugins = ("pytester",)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _install_timeout_harness(pytester: pytest.Pytester) -> None:
    pytester.makeini(
        "\n".join(
            [
                "[pytest]",
                f"pythonpath = {REPO_ROOT}",
                "timeout_method = signal",
                "markers =",
                "    cpu_budget: override the per-test CPU-time latency budget (kwarg ms=<int>)",
            ]
        )
    )
    pytester.makeconftest((REPO_ROOT / "tests" / "conftest.py").read_text())


def _disable_parent_subprocess_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(root_conftest, "UNIT_RUNTIME_SUBPROCESS_GUARD_ENABLED", False)


def test_cpu_heavy_unit_test_fails_with_cpu_budget_message(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_timeout_harness(pytester)
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "10")
    _disable_parent_subprocess_guard(monkeypatch)
    pytester.makepyfile(
        tests_test_cpu_guard="""
        import time

        def test_cpu_bound():
            start = time.process_time()
            while time.process_time() - start < 0.04:
                pass
        """
    )

    result = pytester.runpytest("tests_test_cpu_guard.py")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*CPU latency budget exceeded: tests_test_cpu_guard.py::test_cpu_bound consumed *ms CPU (budget 10ms).*",
            "*@pytest.mark.cpu_budget(ms=...)*",
        ]
    )


def test_wall_sleep_past_cpu_budget_still_passes(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_timeout_harness(pytester)
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "20")
    _disable_parent_subprocess_guard(monkeypatch)
    pytester.makepyfile(
        tests_test_sleep_guard="""
        import time

        def test_sleep_is_not_cpu():
            time.sleep(0.1)
        """
    )

    result = pytester.runpytest("tests_test_sleep_guard.py")

    result.assert_outcomes(passed=1)


def test_cpu_budget_marker_override_and_explicit_timeout_opt_out(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_timeout_harness(pytester)
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "10")
    _disable_parent_subprocess_guard(monkeypatch)
    pytester.makepyfile(
        tests_test_cpu_override="""
        import time

        import pytest

        @pytest.mark.cpu_budget(ms=100)
        def test_cpu_budget_override():
            start = time.process_time()
            while time.process_time() - start < 0.04:
                pass

        @pytest.mark.timeout(1, method="signal")
        def test_explicit_timeout_disables_cpu_guard():
            start = time.process_time()
            while time.process_time() - start < 0.04:
                pass
        """
    )

    result = pytester.runpytest("tests_test_cpu_override.py")

    result.assert_outcomes(passed=2)


def test_infinite_loop_is_killed_by_hang_guard(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_timeout_harness(pytester)
    monkeypatch.setenv("GZA_UNIT_TEST_HANG_TIMEOUT_MS", "100")
    monkeypatch.setenv("GZA_UNIT_TEST_CPU_BUDGET_MS", "10000")
    _disable_parent_subprocess_guard(monkeypatch)
    pytester.makepyfile(
        tests_test_hang_guard="""
        def test_infinite_loop():
            while True:
                pass
        """
    )

    result = pytester.runpytest("tests_test_hang_guard.py")

    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*Timeout*"])
