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








