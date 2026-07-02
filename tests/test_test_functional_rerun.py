from __future__ import annotations

from unittest.mock import Mock

import pytest

import gza.test_functional_rerun as test_functional_rerun


def test_main_rejects_invalid_env_values(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setenv("GZA_FUNCTIONAL_RERUN_CAP", "0")

    exit_code = test_functional_rerun.main([])

    assert exit_code == 2
    assert "GZA_FUNCTIONAL_RERUN_CAP must be a positive integer" in capsys.readouterr().err


def test_main_honors_disable_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GZA_FUNCTIONAL_SERIAL_RERUN", "0")
    run_functional_phase = Mock(return_value=0)
    monkeypatch.setattr(test_functional_rerun, "run_functional_phase", run_functional_phase)

    exit_code = test_functional_rerun.main(["--", "tests_functional/", "-n", "2"])

    assert exit_code == 0
    assert run_functional_phase.call_args.kwargs["rerun_enabled"] is False


def test_main_defaults_to_functional_pytest_lane(monkeypatch: pytest.MonkeyPatch) -> None:
    run_functional_phase = Mock(return_value=0)
    monkeypatch.setattr(test_functional_rerun, "run_functional_phase", run_functional_phase)

    exit_code = test_functional_rerun.main([])

    assert exit_code == 0
    assert run_functional_phase.call_args.args[0] == ["tests_functional/", "-q"]


def test_parse_args_help_mentions_functional_lane_and_default(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        test_functional_rerun._parse_args(["--help"])

    assert excinfo.value.code == 0
    help_text = capsys.readouterr().out
    assert "Run the functional pytest lane with a guarded serial rerun bridge." in help_text
    assert "Defaults to 'tests_functional/ -q'." in help_text
    assert "unit pytest lane" not in help_text
    assert "Defaults to 'tests/ -q'." not in help_text
