"""Tests for aggregate rebase diff comparison."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from gza.git import Git
from gza.rebase_diff import (
    RebaseDiffBaseline,
    RebaseDiffProvenance,
    compute_rebase_changed_diff,
    compute_resolution_delta_context,
)


def _git_completed_process(*, returncode: int = 0, stdout: str = "", stderr: str = "") -> SimpleNamespace:
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _make_patch_id_git(*, pre_patch_id: str, post_patch_id: str) -> MagicMock:
    git = MagicMock(spec=Git)
    git.rev_parse_if_exists.side_effect = lambda ref: {
        "feature": "feature-tip",
        "main": "main-tip",
    }.get(ref)

    def _run(*args: str, **_kwargs: object) -> SimpleNamespace:
        if args == ("diff", "--binary", "--find-renames", "base-tip", "old-tip"):
            return _git_completed_process(stdout="pre-diff\n")
        if args == ("diff", "--binary", "--find-renames", "main-tip", "feature-tip"):
            return _git_completed_process(stdout="post-diff\n")
        if args == ("patch-id", "--stable"):
            stdin = _kwargs.get("stdin")
            if stdin == b"pre-diff\n":
                return _git_completed_process(stdout=f"{pre_patch_id} 0000000000000000000000000000000000000000\n")
            if stdin == b"post-diff\n":
                return _git_completed_process(stdout=f"{post_patch_id} 0000000000000000000000000000000000000000\n")
        raise AssertionError(f"Unexpected git call: args={args!r}, kwargs={_kwargs!r}")

    git._run.side_effect = _run
    return git


def test_compute_rebase_changed_diff_preserves_clean_unchanged_rebase(tmp_path: Path) -> None:
    git = _make_patch_id_git(pre_patch_id="stable-patch", post_patch_id="stable-patch")
    baseline = RebaseDiffBaseline(
        old_tip="old-tip",
        target_at_start="main-start",
        merge_base_at_start="base-tip",
    )

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is False
    assert comparison.warning is None
    assert comparison.detail == "no (review can be preserved)"


def test_compute_rebase_changed_diff_preserves_aggregate_patch_across_commit_topology_change(
    tmp_path: Path,
) -> None:
    git = _make_patch_id_git(pre_patch_id="aggregate-patch", post_patch_id="aggregate-patch")
    baseline = RebaseDiffBaseline(
        old_tip="old-tip",
        target_at_start="main-start",
        merge_base_at_start="base-tip",
    )

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is False
    assert comparison.warning is None


def test_compute_rebase_changed_diff_detects_content_change(tmp_path: Path) -> None:
    git = _make_patch_id_git(pre_patch_id="original-patch", post_patch_id="changed-patch")
    baseline = RebaseDiffBaseline(
        old_tip="old-tip",
        target_at_start="main-start",
        merge_base_at_start="base-tip",
    )

    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is True
    assert comparison.detail == "yes (review must be refreshed)"


def test_compute_rebase_changed_diff_treats_recovered_baseline_as_changed(tmp_path: Path) -> None:
    git = MagicMock(spec=Git)
    baseline = RebaseDiffBaseline(
        old_tip="old-tip",
        target_at_start="main-start",
        merge_base_at_start="base-tip",
        recovered=True,
    )
    comparison = compute_rebase_changed_diff(git, baseline=baseline, branch="feature", target="main")

    assert comparison.changed_diff is True
    assert comparison.detail == "yes (review must be refreshed)"
    assert comparison.warning == (
        "rebase diff comparison unavailable for recovered/resumed rebase; treating as changed"
    )


def test_compute_resolution_delta_context_returns_range_diff_for_changed_rebase() -> None:
    git = MagicMock(spec=Git)

    def _run(*args: str, **_kwargs: object) -> SimpleNamespace:
        if args == ("range-diff", "--no-color", "base-tip..old-tip", "main-tip..feature-tip"):
            return _git_completed_process(stdout="1:  old = 1:  new\n- old hunk\n+ new hunk\n")
        raise AssertionError(f"Unexpected git call: args={args!r}, kwargs={_kwargs!r}")

    git._run.side_effect = _run
    context = compute_resolution_delta_context(
        git,
        provenance=RebaseDiffProvenance(
            old_tip="old-tip",
            target_at_start="main-start",
            merge_base_at_start="base-tip",
            resolved_head_sha="feature-tip",
            resolved_target_sha="main-tip",
        ),
    )

    assert context.available is True
    assert context.before_range == "base-tip..old-tip"
    assert context.after_range == "main-tip..feature-tip"
    assert "new hunk" in (context.range_diff or "")


def test_compute_resolution_delta_context_fails_closed_when_provenance_missing() -> None:
    git = MagicMock(spec=Git)
    context = compute_resolution_delta_context(
        git,
        provenance=RebaseDiffProvenance(
            old_tip=None,
            target_at_start="main-start",
            merge_base_at_start=None,
            resolved_head_sha="feature-tip",
            resolved_target_sha="main-tip",
        ),
    )

    assert context.available is False
    assert "resolution delta unavailable" in context.summary
    git._run.assert_not_called()
