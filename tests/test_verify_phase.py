from __future__ import annotations

import subprocess
from unittest.mock import Mock

import pytest

from gza.git import GitError
from gza.runner import _extract_review_verify_phase_results
from gza.tools import verify_phase


def test_verify_phase_emits_start_and_passed_result(capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch) -> None:
    run_mock = Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=0))
    monkeypatch.setattr(verify_phase, "_run_command", run_mock)
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value="a" * 64))

    exit_code = verify_phase.main(["ruff", "--", "ruff", "check", "src/gza/"])

    assert exit_code == 0
    assert run_mock.call_args.args[0] == ["ruff", "check", "src/gza/"]
    lines = capsys.readouterr().out.strip().splitlines()
    assert lines[0] == "gza-verify phase=start name=ruff"
    assert lines[1].startswith("gza-verify phase=passed name=ruff duration_seconds=")
    assert lines[1].endswith(f"tree_fingerprint={'a' * 64}")


def test_verify_phase_emits_failed_result_without_fingerprint_when_unavailable(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=7)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(side_effect=GitError("missing gitdir")))

    exit_code = verify_phase.main(["unit", "--", "pytest", "tests/", "-x"])

    assert exit_code == 7
    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert lines[0] == "gza-verify phase=start name=unit"
    assert lines[1].startswith("gza-verify phase=failed name=unit duration_seconds=")
    assert "tree_fingerprint=" not in lines[1]
    assert "skipping tree fingerprint because git metadata is unavailable" in captured.err


def test_verify_phase_propagates_success_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=0)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value=None))

    assert verify_phase.main(["checks", "--", "python", "-m", "checks"]) == 0


def test_verify_phase_propagates_failure_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=23)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value=None))

    assert verify_phase.main(["checks", "--", "python", "-m", "checks"]) == 23


def test_verify_phase_output_round_trips_through_runner_parser(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    fingerprint = "b" * 64
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=0)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value=fingerprint))

    exit_code = verify_phase.main(["functional", "--", "pytest", "tests_functional/", "-x"])

    assert exit_code == 0
    phases = _extract_review_verify_phase_results(capsys.readouterr().out)
    assert len(phases) == 1
    assert phases[0]["name"] == "functional"
    assert phases[0]["status"] == "passed"
    assert isinstance(phases[0]["duration_seconds"], float)
    assert phases[0]["tree_fingerprint"] == fingerprint


def test_verify_phase_omits_invalid_fingerprint_and_preserves_parser_round_trip(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=0)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value="NOT-VALID"))

    exit_code = verify_phase.main(["functional", "--", "pytest", "tests_functional/", "-x"])

    assert exit_code == 0
    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert lines[0] == "gza-verify phase=start name=functional"
    assert lines[1].startswith("gza-verify phase=passed name=functional duration_seconds=")
    assert "tree_fingerprint=" not in lines[1]
    assert "skipping invalid tree fingerprint 'NOT-VALID'; expected 64 lowercase hex characters" in captured.err
    phases = _extract_review_verify_phase_results(captured.out)
    assert len(phases) == 1
    assert phases[0]["name"] == "functional"
    assert phases[0]["status"] == "passed"
    assert isinstance(phases[0]["duration_seconds"], float)
    assert "tree_fingerprint" not in phases[0]


@pytest.mark.parametrize(
    ("launch_error", "expected_exit_code"),
    [
        (FileNotFoundError("missing-tool"), 127),
        (PermissionError("permission denied"), 126),
    ],
)
def test_verify_phase_emits_failed_result_when_command_launch_fails(
    launch_error: OSError,
    expected_exit_code: int,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(verify_phase, "_run_command", Mock(side_effect=launch_error))
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(return_value="c" * 64))

    exit_code = verify_phase.main(["ruff", "--", "missing-tool", "--version"])

    assert exit_code == expected_exit_code
    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert lines[0] == "gza-verify phase=start name=ruff"
    assert lines[1].startswith("gza-verify phase=failed name=ruff duration_seconds=")
    assert lines[1].endswith(f"tree_fingerprint={'c' * 64}")
    assert "failed to launch command ['missing-tool', '--version']" in captured.err
    phases = _extract_review_verify_phase_results(captured.out)
    assert len(phases) == 1
    assert phases[0]["name"] == "ruff"
    assert phases[0]["status"] == "failed"
    assert isinstance(phases[0]["duration_seconds"], float)
    assert phases[0]["tree_fingerprint"] == "c" * 64


def test_verify_phase_warns_on_unexpected_fingerprint_failure_and_preserves_result_line(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        verify_phase,
        "_run_command",
        Mock(return_value=subprocess.CompletedProcess(args=["cmd"], returncode=0)),
    )
    monkeypatch.setattr(verify_phase, "_compute_tree_fingerprint", Mock(side_effect=ValueError("boom")))

    exit_code = verify_phase.main(["checks", "--", "python", "-m", "checks"])

    assert exit_code == 0
    captured = capsys.readouterr()
    lines = captured.out.strip().splitlines()
    assert lines[0] == "gza-verify phase=start name=checks"
    assert lines[1].startswith("gza-verify phase=passed name=checks duration_seconds=")
    assert "tree_fingerprint=" not in lines[1]
    assert "unexpected tree fingerprint failure: ValueError('boom')" in captured.err


def test_verify_phase_usage_errors(capsys: pytest.CaptureFixture[str]) -> None:
    assert verify_phase.main([]) == 2
    assert "Usage: python -m gza.tools.verify_phase <name> -- <cmd...>" in capsys.readouterr().err


@pytest.mark.parametrize("phase_name", ["bad/name", "bad name"])
def test_verify_phase_rejects_invalid_phase_names(
    phase_name: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    run_mock = Mock()
    monkeypatch.setattr(verify_phase, "_run_command", run_mock)

    exit_code = verify_phase.main([phase_name, "--", "true"])

    assert exit_code == 2
    captured = capsys.readouterr()
    assert run_mock.call_count == 0
    assert "gza-verify phase=" not in captured.out
    assert f"invalid phase name {phase_name!r}; expected [A-Za-z0-9_.-]+" in captured.err
    assert "Usage: python -m gza.tools.verify_phase <name> -- <cmd...>" in captured.err
