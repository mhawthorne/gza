"""Tests for aggregate improve diff comparison."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from gza.git import Git
from gza.improve_diff import ImproveDiffBaseline, compute_improve_changed_diff


def _git_completed_process(*, returncode: int = 0, stdout: str = "", stderr: str = "") -> SimpleNamespace:
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _make_patch_id_git(*, pre_patch_id: str, post_patch_id: str) -> MagicMock:
    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature": "feature-tip-after",
    }.get(ref)

    def _run(*args: str, **kwargs: object) -> SimpleNamespace:
        if args == ("diff", "--binary", "--find-renames", "main-start", "feature-tip-before"):
            return _git_completed_process(stdout="pre-diff\n")
        if args == ("diff", "--binary", "--find-renames", "main-start", "feature-tip-after"):
            return _git_completed_process(stdout="post-diff\n")
        if args == ("patch-id", "--stable"):
            stdin = kwargs.get("stdin")
            if stdin == b"pre-diff\n":
                return _git_completed_process(stdout=f"{pre_patch_id} 0000000000000000000000000000000000000000\n")
            if stdin == b"post-diff\n":
                return _git_completed_process(stdout=f"{post_patch_id} 0000000000000000000000000000000000000000\n")
        raise AssertionError(f"Unexpected git call: args={args!r}, kwargs={kwargs!r}")

    git._run.side_effect = _run
    return git


def test_compute_improve_changed_diff_returns_false_for_unchanged_tracked_diff() -> None:
    git = _make_patch_id_git(pre_patch_id="stable-patch", post_patch_id="stable-patch")
    baseline = ImproveDiffBaseline(branch_tip_before="feature-tip-before", target_at_start="main-start")

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is False
    assert comparison.detail == "no (no tracked improve changes)"
    assert comparison.warning is None


def test_compute_improve_changed_diff_returns_false_for_add_then_revert() -> None:
    git = _make_patch_id_git(pre_patch_id="aggregate-patch", post_patch_id="aggregate-patch")
    baseline = ImproveDiffBaseline(branch_tip_before="feature-tip-before", target_at_start="main-start")

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is False
    assert comparison.warning is None


def test_compute_improve_changed_diff_detects_real_tracked_change() -> None:
    git = _make_patch_id_git(pre_patch_id="original-patch", post_patch_id="changed-patch")
    baseline = ImproveDiffBaseline(branch_tip_before="feature-tip-before", target_at_start="main-start")

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is True
    assert comparison.detail == "yes (tracked improve diff changed or comparison unavailable)"


def test_compute_improve_changed_diff_returns_false_for_ignored_only_effective_change() -> None:
    git = _make_patch_id_git(pre_patch_id="tracked-patch", post_patch_id="tracked-patch")
    baseline = ImproveDiffBaseline(branch_tip_before="feature-tip-before", target_at_start="main-start")

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is False


def test_compute_improve_changed_diff_treats_missing_refs_as_changed() -> None:
    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.return_value = None
    baseline = ImproveDiffBaseline(branch_tip_before="feature-tip-before", target_at_start="main-start")

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is True
    assert comparison.warning == "improve diff comparison missing post-improve refs; treating as changed"


def test_compute_improve_changed_diff_treats_recovered_baseline_as_changed() -> None:
    git = MagicMock(spec=Git)
    baseline = ImproveDiffBaseline(
        branch_tip_before="feature-tip-before",
        target_at_start="main-start",
        recovered=True,
    )

    comparison = compute_improve_changed_diff(git, baseline=baseline, branch="feature")

    assert comparison.changed_diff is True
    assert comparison.warning == (
        "improve diff comparison unavailable for recovered/resumed improve; treating as changed"
    )
