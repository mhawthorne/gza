"""Pure parsing helpers for conservative off-topic verify classification."""

from __future__ import annotations

import re
import shlex
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePath, PurePosixPath
from typing import TYPE_CHECKING, Literal

from gza.cli._common import _looks_like_verify_command

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from gza.git import Git
    from gza.runner import ReviewVerifyResult


VerifyFailureUnavailableReason = Literal[
    "not_pytest_command",
    "fail_fast_enabled",
    "missing_output",
    "no_failing_nodes",
    "incomplete_enumeration",
]
DiffScopeOutcome = Literal["off_topic", "branch_introduced", "unavailable"]
NodeDiffScopeOutcome = Literal["outside_diff", "inside_diff", "unavailable"]
LocalTargetBaselineMode = Literal["deterministic_once", "stress"]
LocalTargetBaselineUnavailableReason = Literal[
    "diff_scope_unavailable",
    "diff_scope_blocked",
    "not_pytest_command",
    "unsafe_pytest_command",
    "missing_target_branch",
    "missing_target_head_sha",
    "missing_target_tree_fingerprint",
    "no_failing_nodes",
]

_ASSERTION_LINE_PATTERN = re.compile(r"^E\s+(?P<signature>.+)$", re.MULTILINE)
_TRACEBACK_PATH_PATTERN = re.compile(
    r"^(?P<path>(?:[A-Za-z]:)?[^:\n]+?\.py):(?P<lineno>\d+)(?::.*)?$",
    re.MULTILINE,
)
_PYTEST_SUMMARY_COUNTS_PATTERN = re.compile(r"=+\s+(?P<body>.+?)\s+in\s+\d+(?:\.\d+)?s\s+=+$")
_COUNT_TOKEN_PATTERN = re.compile(
    r"(?P<count>\d+)\s+"
    r"(?P<kind>failed|passed|error|errors|skipped|xfailed|xpassed|deselected|warnings?|rerun|reruns)"
)
_PLUGIN_XDIST_PATTERN = re.compile(r"\bxdist-(?P<version>[0-9][^,\s`]*)")
_WORKER_ID_PATTERN = re.compile(r"\[(?P<worker>gw\d+)\]")
_FAIL_FAST_OUTPUT_PATTERN = re.compile(r"stopping after\s+(?P<count>\d+)\s+failure(?:s)?\b", re.IGNORECASE)
_FAILURE_HEADING_PATTERN = re.compile(r"^_{3,}\S.*_{3,}\s*$", re.MULTILINE)
_SHORT_SUMMARY_PATTERN = re.compile(r"^=+\s+short test summary info\s+=+$", re.MULTILINE)
_PYTEST_EXECUTABLE_NAMES = {"pytest", "py.test"}
_PYTEST_OPTIONS_WITH_VALUES = {
    "-c",
    "-k",
    "-m",
    "-n",
    "-o",
    "-p",
    "--basetemp",
    "--confcutdir",
    "--dist",
    "--durations",
    "--durations-min",
    "--ignore",
    "--ignore-glob",
    "--junitxml",
    "--log-cli-format",
    "--log-cli-level",
    "--log-date-format",
    "--log-file",
    "--log-file-date-format",
    "--log-file-format",
    "--log-file-level",
    "--log-format",
    "--log-level",
    "--maxfail",
    "--numprocesses",
    "--override-ini",
    "--rootdir",
    "--tb",
    "--timeout",
    "--timeout-method",
}
_PYTEST_NO_ARG_LONG_OPTIONS = {
    "--cache-clear",
    "--capture=no",
    "--color=yes",
    "--color=no",
    "--disable-warnings",
    "--ff",
    "--full-trace",
    "--lf",
    "--maxfail=0",
    "--no-header",
    "--no-summary",
    "--quiet",
    "--setup-show",
    "--showlocals",
    "--tb=auto",
    "--tb=short",
    "--tb=line",
    "--tb=native",
    "--tb=no",
    "--tb=long",
    "--trace",
    "--verbose",
}
_SHARED_GLOBAL_CONCURRENCY_SENSITIVE_PREFIXES = (
    "src/gza/advance_engine.py",
    "src/gza/cli/advance_engine.py",
    "src/gza/cli/advance_executor.py",
    "src/gza/cli/execution.py",
    "src/gza/cli/watch.py",
    "src/gza/concurrency.py",
    "src/gza/db.py",
    "src/gza/main_integration_verify.py",
    "src/gza/recovery_engine.py",
    "src/gza/runner.py",
    "src/gza/review_verdict.py",
    "src/gza/workers.py",
    "tests/conftest.py",
    "tests/test_runner.py",
)
_SHARED_GLOBAL_CONCURRENCY_SENSITIVE_KEYWORDS = (
    "lock",
    "orchestr",
    "schedul",
    "worker",
    "watch",
)
DEFAULT_OFF_TOPIC_STRESS_RUNS = 20
MAX_OFF_TOPIC_STRESS_RUNS = 20


@dataclass(frozen=True)
class VerifyExitStatus:
    raw: str
    code: int | None
    timed_out: bool = False
    launch_failed: bool = False


@dataclass(frozen=True)
class VerifyFailureUnavailable:
    reason: VerifyFailureUnavailableReason
    detail: str | None = None


@dataclass(frozen=True)
class PytestPassFailCounts:
    failed: int | None = None
    passed: int | None = None
    errors: int | None = None
    skipped: int | None = None
    xfailed: int | None = None
    xpassed: int | None = None
    deselected: int | None = None
    warnings: int | None = None
    reruns: int | None = None

    @property
    def total_failures(self) -> int:
        return (self.failed or 0) + (self.errors or 0)


@dataclass(frozen=True)
class PytestXdistMetadata:
    enabled: bool = False
    worker_count: int | None = None
    worker_count_raw: str | None = None
    dist_mode: str | None = None
    plugin_version: str | None = None
    worker_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class FailingNode:
    nodeid: str
    path: str
    outcome: Literal["FAILED", "ERROR"]
    assertion_signature: str | None = None
    failure_path: str | None = None
    failure_line: int | None = None
    traceback_paths: tuple[str, ...] = ()
    trustworthy_attribution: bool = False


@dataclass(frozen=True)
class VerifyFailureSet:
    command: str
    exit_status: VerifyExitStatus
    failing_nodes: tuple[FailingNode, ...] = ()
    pass_fail_counts: PytestPassFailCounts = field(default_factory=PytestPassFailCounts)
    xdist: PytestXdistMetadata = field(default_factory=PytestXdistMetadata)
    unavailable: VerifyFailureUnavailable | None = None

    @property
    def available(self) -> bool:
        return self.unavailable is None


@dataclass(frozen=True)
class NodeDiffScopeClassification:
    nodeid: str
    outcome: NodeDiffScopeOutcome
    scoped_paths: tuple[str, ...]
    matched_changed_paths: tuple[str, ...] = ()
    detail: str | None = None


@dataclass(frozen=True)
class DiffScopeClassification:
    outcome: DiffScopeOutcome
    baseline_mode: LocalTargetBaselineMode | None
    shared_global_paths: tuple[str, ...] = ()
    node_classifications: tuple[NodeDiffScopeClassification, ...] = ()
    detail: str | None = None

    @property
    def available(self) -> bool:
        return self.outcome != "unavailable"


@dataclass(frozen=True)
class LocalTargetBaselinePlanUnavailable:
    reason: LocalTargetBaselineUnavailableReason
    detail: str | None = None


@dataclass(frozen=True)
class LocalTargetBaselinePlan:
    mode: LocalTargetBaselineMode
    command: str
    nodeids: tuple[str, ...]
    target_branch: str
    target_head_sha: str
    target_tree_fingerprint: str
    run_count: int
    relative_cwd: str = "."


@dataclass(frozen=True)
class LocalTargetBaselineSelection:
    plan: LocalTargetBaselinePlan | None = None
    unavailable: LocalTargetBaselinePlanUnavailable | None = None

    @property
    def available(self) -> bool:
        return self.plan is not None and self.unavailable is None


@dataclass(frozen=True)
class LocalTargetBaselineRun:
    plan: LocalTargetBaselinePlan
    worktree_path: str
    results: tuple[ReviewVerifyResult, ...]


def parse_review_verify_failure_set(result: ReviewVerifyResult) -> VerifyFailureSet:
    """Parse persisted pytest verify evidence into structured failure inputs."""
    return parse_pytest_verify_failure(
        command=result.command,
        output=result.output,
        exit_status=result.exit_status,
    )


def parse_pytest_verify_failure(*, command: str, output: str | None, exit_status: str) -> VerifyFailureSet:
    """Parse one verify result conservatively, returning unavailable on ambiguity."""
    parsed_exit_status = parse_verify_exit_status(exit_status)
    normalized_output = (output or "").strip()
    if not looks_like_pytest_verify_command(command, output=normalized_output):
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            unavailable=VerifyFailureUnavailable("not_pytest_command"),
        )

    if not normalized_output:
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            unavailable=VerifyFailureUnavailable("missing_output"),
        )

    pass_fail_counts = extract_pytest_pass_fail_counts(normalized_output)
    xdist = extract_pytest_xdist_metadata(command=command, output=normalized_output)
    fail_fast_enabled = pytest_uses_fail_fast(command=command, output=normalized_output)
    summary_entries = _collect_summary_entries(normalized_output)
    failing_nodes = build_failing_nodes(normalized_output)

    if fail_fast_enabled:
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            pass_fail_counts=pass_fail_counts,
            xdist=xdist,
            unavailable=VerifyFailureUnavailable(
                "fail_fast_enabled",
                detail="pytest command uses -x/--maxfail so the failing-node set cannot be trusted",
            ),
        )

    if not failing_nodes:
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            pass_fail_counts=pass_fail_counts,
            xdist=xdist,
            unavailable=VerifyFailureUnavailable("no_failing_nodes"),
        )

    if not _has_terminal_failure_counts(pass_fail_counts):
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            pass_fail_counts=pass_fail_counts,
            xdist=xdist,
            unavailable=VerifyFailureUnavailable(
                "incomplete_enumeration",
                detail=(
                    "pytest output listed failing nodes in the short summary but did not include "
                    "terminal failed/error counts"
                ),
            ),
        )

    if pass_fail_counts.total_failures > 0 and len(failing_nodes) != pass_fail_counts.total_failures:
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            pass_fail_counts=pass_fail_counts,
            xdist=xdist,
            unavailable=VerifyFailureUnavailable(
                "incomplete_enumeration",
                detail=(
                    f"summary reported {pass_fail_counts.total_failures} failing outcomes "
                    f"but only {len(failing_nodes)} node ids were parsed"
                ),
            ),
        )

    if not summary_entries and pass_fail_counts.total_failures == 0:
        return VerifyFailureSet(
            command=command,
            exit_status=parsed_exit_status,
            pass_fail_counts=pass_fail_counts,
            xdist=xdist,
            unavailable=VerifyFailureUnavailable(
                "incomplete_enumeration",
                detail="pytest output did not contain a trustworthy short summary or failure counts",
            ),
        )

    return VerifyFailureSet(
        command=command,
        exit_status=parsed_exit_status,
        failing_nodes=failing_nodes,
        pass_fail_counts=pass_fail_counts,
        xdist=xdist,
    )


def classify_failure_diff_scope(
    failure_set: VerifyFailureSet,
    *,
    changed_paths: tuple[str, ...] | list[str] | None,
    repo_root: Path | None = None,
) -> DiffScopeClassification:
    """Classify whether failing-node evidence falls inside or outside the local target diff."""
    if not failure_set.available:
        detail = failure_set.unavailable.detail if failure_set.unavailable is not None else None
        return DiffScopeClassification(
            outcome="unavailable",
            baseline_mode=None,
            detail=detail or "verify failure evidence is unavailable",
        )

    if not failure_set.failing_nodes:
        return DiffScopeClassification(
            outcome="unavailable",
            baseline_mode=None,
            detail="no failing nodes were available to scope against the diff",
        )

    normalized_changed_paths = _normalize_changed_paths(changed_paths, repo_root=repo_root)
    if normalized_changed_paths is None:
        return DiffScopeClassification(
            outcome="unavailable",
            baseline_mode=None,
            detail="changed-path diff could not be normalized safely",
        )

    node_classifications = tuple(
        _classify_node_diff_scope(node, normalized_changed_paths, repo_root=repo_root)
        for node in failure_set.failing_nodes
    )
    if any(node.outcome == "unavailable" for node in node_classifications):
        return DiffScopeClassification(
            outcome="unavailable",
            baseline_mode=None,
            node_classifications=node_classifications,
            detail="at least one failing node could not be scoped against the diff",
        )

    if any(node.outcome == "inside_diff" for node in node_classifications):
        return DiffScopeClassification(
            outcome="branch_introduced",
            baseline_mode=None,
            node_classifications=node_classifications,
            detail="at least one failing node touched a changed path",
        )

    shared_paths = tuple(
        path for path in normalized_changed_paths if is_shared_global_or_concurrency_sensitive_path(path)
    )
    baseline_mode: LocalTargetBaselineMode = "stress" if shared_paths else "deterministic_once"
    return DiffScopeClassification(
        outcome="off_topic",
        baseline_mode=baseline_mode,
        shared_global_paths=shared_paths,
        node_classifications=node_classifications,
    )


def is_shared_global_or_concurrency_sensitive_path(path: str) -> bool:
    """Return whether a changed path is too cross-cutting for diff-only intermittent clearance."""
    normalized = _normalize_repo_relative_path(path)
    if normalized is None:
        return True

    if normalized in _SHARED_GLOBAL_CONCURRENCY_SENSITIVE_PREFIXES:
        return True
    if any(
        normalized.startswith(prefix.rstrip("/") + "/")
        for prefix in _SHARED_GLOBAL_CONCURRENCY_SENSITIVE_PREFIXES
    ):
        return True

    lower_name = PurePosixPath(normalized).name.lower()
    if lower_name in {"conftest.py", "pytest.ini", "pyproject.toml", "gza.yaml"}:
        return True
    return any(keyword in lower_name for keyword in _SHARED_GLOBAL_CONCURRENCY_SENSITIVE_KEYWORDS)


def select_local_target_baseline_plan(
    failure_set: VerifyFailureSet,
    diff_scope: DiffScopeClassification,
    *,
    target_branch: str | None,
    target_head_sha: str | None,
    target_tree_fingerprint: str | None,
    stress_runs: int = DEFAULT_OFF_TOPIC_STRESS_RUNS,
    relative_cwd: str = ".",
) -> LocalTargetBaselineSelection:
    """Build a bounded local-target rerun plan for deterministic or intermittent off-topic checks."""
    if diff_scope.outcome == "unavailable":
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "diff_scope_unavailable",
                diff_scope.detail,
            )
        )
    if diff_scope.outcome != "off_topic" or diff_scope.baseline_mode is None:
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "diff_scope_blocked",
                diff_scope.detail or "diff scope remained branch-introduced",
            )
        )

    if not target_branch:
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "missing_target_branch",
                "local target branch is required for detached baseline reruns",
            )
        )
    if not target_head_sha:
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "missing_target_head_sha",
                "local target head SHA is required for detached baseline reruns",
            )
        )
    if not target_tree_fingerprint:
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "missing_target_tree_fingerprint",
                "local target tree fingerprint is required for detached baseline reruns",
            )
        )
    if not failure_set.failing_nodes:
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                "no_failing_nodes",
                "failing-node enumeration must succeed before planning a target rerun",
            )
        )

    command = build_local_target_pytest_command(
        failure_set.command,
        nodeids=tuple(node.nodeid for node in failure_set.failing_nodes),
    )
    if command is None:
        reason: LocalTargetBaselineUnavailableReason = (
            "not_pytest_command"
            if not looks_like_pytest_verify_command(failure_set.command)
            else "unsafe_pytest_command"
        )
        return LocalTargetBaselineSelection(
            unavailable=LocalTargetBaselinePlanUnavailable(
                reason,
                "could not safely construct a targeted non-fail-fast pytest rerun command",
            )
        )

    run_count = 1 if diff_scope.baseline_mode == "deterministic_once" else min(max(stress_runs, 1), MAX_OFF_TOPIC_STRESS_RUNS)
    return LocalTargetBaselineSelection(
        plan=LocalTargetBaselinePlan(
            mode=diff_scope.baseline_mode,
            command=command,
            nodeids=tuple(node.nodeid for node in failure_set.failing_nodes),
            target_branch=target_branch,
            target_head_sha=target_head_sha,
            target_tree_fingerprint=target_tree_fingerprint,
            run_count=run_count,
            relative_cwd=relative_cwd or ".",
        )
    )


def build_local_target_pytest_command(command: str, *, nodeids: tuple[str, ...]) -> str | None:
    """Return a targeted non-fail-fast pytest command or ``None`` when construction is unsafe."""
    if not nodeids:
        return None

    try:
        tokens = shlex.split(command)
    except ValueError:
        return None

    pytest_index = _find_pytest_token_index(tokens)
    if pytest_index is None:
        return None

    rebuilt_tokens = list(tokens[: pytest_index + 1])
    index = pytest_index + 1
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            index += 1
            while index < len(tokens):
                index += 1
            break

        if token == "-x":
            index += 1
            continue
        if token.startswith("--maxfail="):
            index += 1
            continue
        if token == "--maxfail":
            index += 2
            continue

        if token.startswith("--"):
            if "=" in token:
                option_name = token.partition("=")[0]
                if option_name in _PYTEST_OPTIONS_WITH_VALUES or token in _PYTEST_NO_ARG_LONG_OPTIONS:
                    rebuilt_tokens.append(token)
                    index += 1
                    continue
                return None
            if token in _PYTEST_OPTIONS_WITH_VALUES:
                if index + 1 >= len(tokens):
                    return None
                rebuilt_tokens.extend([token, tokens[index + 1]])
                index += 2
                continue
            if token in _PYTEST_NO_ARG_LONG_OPTIONS:
                rebuilt_tokens.append(token)
                index += 1
                continue
            return None

        if token.startswith("-") and token != "-":
            if token in _PYTEST_OPTIONS_WITH_VALUES:
                if index + 1 >= len(tokens):
                    return None
                rebuilt_tokens.extend([token, tokens[index + 1]])
                index += 2
                continue
            if token == "-x":
                index += 1
                continue
            normalized_bundle = _strip_fail_fast_from_short_option_bundle(token)
            if normalized_bundle is None:
                return None
            if normalized_bundle:
                rebuilt_tokens.append(normalized_bundle)
            index += 1
            continue

        # Existing positional selectors widen the rerun scope. Drop them and
        # replace them with the enumerated failing nodes only.
        index += 1

    rebuilt_tokens.append("--maxfail=0")
    rebuilt_tokens.extend(nodeids)
    return shlex.join(rebuilt_tokens)


def run_local_target_baseline_plan(
    plan: LocalTargetBaselinePlan,
    *,
    repo_git: Git,
    worktree_root: Path,
    timeout_seconds: int,
    timeout_grace_seconds: float,
    run_verify_command: Callable[..., ReviewVerifyResult] | None = None,
) -> LocalTargetBaselineRun:
    """Execute a previously planned local-target rerun inside a detached target worktree."""
    from gza.runner import _run_review_verify_command

    verify_runner = run_verify_command or _run_review_verify_command
    _validate_local_target_baseline_plan(plan)
    with detached_local_target_worktree(
        repo_git=repo_git,
        worktree_root=worktree_root,
        target_branch=plan.target_branch,
        target_head_sha=plan.target_head_sha,
    ) as worktree_path:
        worktree_git = repo_git.__class__(worktree_path)
        target_cwd = _resolve_local_target_baseline_cwd(
            worktree_path=worktree_path,
            relative_cwd=plan.relative_cwd,
        )
        if not target_cwd.exists() or not target_cwd.is_dir():
            raise FileNotFoundError(f"local-target baseline cwd does not exist: {target_cwd}")
        results = tuple(
            verify_runner(
                plan.command,
                cwd=target_cwd,
                reviewed_branch=plan.target_branch,
                reviewed_head_sha=plan.target_head_sha,
                timeout_seconds=timeout_seconds,
                timeout_grace_seconds=timeout_grace_seconds,
            )
            for _ in range(plan.run_count)
        )
        return LocalTargetBaselineRun(
            plan=plan,
            worktree_path=str(worktree_git.repo_dir),
            results=results,
        )


def _validate_local_target_baseline_plan(plan: LocalTargetBaselinePlan) -> None:
    if plan.run_count < 1 or plan.run_count > MAX_OFF_TOPIC_STRESS_RUNS:
        raise ValueError(
            "local-target baseline run_count must be between 1 and "
            f"{MAX_OFF_TOPIC_STRESS_RUNS}: {plan.run_count}"
        )


def _resolve_local_target_baseline_cwd(*, worktree_path: Path, relative_cwd: str) -> Path:
    relative_path = Path(relative_cwd)
    if relative_path.is_absolute():
        raise ValueError(f"local-target baseline cwd must be relative: {relative_cwd}")

    target_cwd = (worktree_path / relative_path).resolve()
    worktree_root = worktree_path.resolve()
    try:
        target_cwd.relative_to(worktree_root)
    except ValueError as exc:
        raise ValueError(
            "local-target baseline cwd escapes detached target worktree: "
            f"{relative_cwd}"
        ) from exc
    return target_cwd


@contextmanager
def detached_local_target_worktree(
    *,
    repo_git: Git,
    worktree_root: Path,
    target_branch: str,
    target_head_sha: str,
) -> Iterator[Path]:
    """Yield a clean detached worktree at the immutable local target commit."""
    worktree_root.mkdir(parents=True, exist_ok=True)
    worktree_path = Path(tempfile.mkdtemp(prefix="off-topic-target-", dir=worktree_root))
    try:
        repo_git.worktree_add_existing(worktree_path, target_head_sha, detach=True)
        worktree_git = repo_git.__class__(worktree_path)
        worktree_git.checkout_detached(target_head_sha)
        worktree_git.reset_hard(target_head_sha)
        worktree_git.clean_force()
        yield worktree_path
    finally:
        try:
            repo_git.worktree_remove(worktree_path, force=True)
        finally:
            shutil.rmtree(worktree_path, ignore_errors=True)


def looks_like_pytest_verify_command(command: str, *, output: str | None = None) -> bool:
    """Return whether the command looks like a pytest-based verify invocation."""
    lowered_command = command.lower()
    if _looks_like_verify_command(command, None) and "pytest" in lowered_command:
        return True
    normalized_output = (output or "").lower()
    return "short test summary info" in normalized_output or "collected " in normalized_output


def pytest_command_uses_fail_fast(command: str) -> bool:
    """Return whether the pytest command requested fail-fast behavior."""
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            break
        if token == "-x":
            return True
        if _short_option_bundle_uses_fail_fast(token):
            return True
        if token.startswith("--maxfail="):
            return _maxfail_value_is_positive(token.partition("=")[2])
        if token == "--maxfail" and index + 1 < len(tokens):
            return _maxfail_value_is_positive(tokens[index + 1])
        index += 1
    return False


def pytest_uses_fail_fast(*, command: str, output: str) -> bool:
    """Return whether persisted pytest evidence shows fail-fast behavior was enabled."""
    if pytest_command_uses_fail_fast(command):
        return True
    match = _FAIL_FAST_OUTPUT_PATTERN.search(output)
    return match is not None and int(match.group("count")) > 0


def parse_verify_exit_status(raw: str | None) -> VerifyExitStatus:
    """Parse persisted exit-status metadata into structured fields."""
    normalized = (raw or "").strip()
    if normalized.isdigit():
        return VerifyExitStatus(raw=normalized, code=int(normalized))

    lowered = normalized.lower()
    return VerifyExitStatus(
        raw=normalized,
        code=None,
        timed_out="timed out" in lowered,
        launch_failed="launch failed" in lowered,
    )


def extract_pytest_pass_fail_counts(output: str) -> PytestPassFailCounts:
    """Extract terminal pytest pass/fail counters when present."""
    summary_match = None
    for summary_match in _PYTEST_SUMMARY_COUNTS_PATTERN.finditer(output):
        pass
    if summary_match is None:
        return PytestPassFailCounts()

    counts: dict[str, int] = {}
    for token in _COUNT_TOKEN_PATTERN.finditer(summary_match.group("body")):
        kind = token.group("kind")
        key = "errors" if kind in {"error", "errors"} else "warnings" if kind.startswith("warning") else "reruns" if kind.startswith("rerun") else kind
        counts[key] = int(token.group("count"))
    return PytestPassFailCounts(**counts)


def _has_terminal_failure_counts(counts: PytestPassFailCounts) -> bool:
    return counts.failed is not None or counts.errors is not None


def extract_pytest_xdist_metadata(*, command: str, output: str) -> PytestXdistMetadata:
    """Extract xdist/plugin metadata from the verify command and output."""
    plugin_match = _PLUGIN_XDIST_PATTERN.search(output)
    plugin_version = plugin_match.group("version") if plugin_match else None

    worker_count: int | None = None
    worker_count_raw: str | None = None
    dist_mode: str | None = None
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token == "-n" and index + 1 < len(tokens):
            worker_count_raw = tokens[index + 1]
            if worker_count_raw.isdigit():
                worker_count = int(worker_count_raw)
            index += 1
        elif token.startswith("-n") and token != "-n":
            worker_count_raw = token[2:]
            if worker_count_raw.isdigit():
                worker_count = int(worker_count_raw)
        elif token == "--numprocesses" and index + 1 < len(tokens):
            worker_count_raw = tokens[index + 1]
            if worker_count_raw.isdigit():
                worker_count = int(worker_count_raw)
            index += 1
        elif token.startswith("--numprocesses="):
            worker_count_raw = token.partition("=")[2]
            if worker_count_raw.isdigit():
                worker_count = int(worker_count_raw)
        elif token == "--dist" and index + 1 < len(tokens):
            dist_mode = tokens[index + 1]
            index += 1
        elif token.startswith("--dist="):
            dist_mode = token.partition("=")[2]
        index += 1

    worker_ids = tuple(dict.fromkeys(match.group("worker") for match in _WORKER_ID_PATTERN.finditer(output)))
    enabled = plugin_version is not None or worker_count_raw is not None or bool(worker_ids)
    return PytestXdistMetadata(
        enabled=enabled,
        worker_count=worker_count,
        worker_count_raw=worker_count_raw,
        dist_mode=dist_mode,
        plugin_version=plugin_version,
        worker_ids=worker_ids,
    )


def extract_pytest_failing_nodeids(output: str) -> tuple[str, ...]:
    """Extract ordered failing pytest node ids from output."""
    entries = _collect_summary_entries(output)
    return tuple(entry[1] for entry in entries)


def extract_pytest_traceback_paths(output: str) -> tuple[str, ...]:
    """Extract ordered traceback file paths from pytest output."""
    return tuple(dict.fromkeys(match.group("path") for match in _TRACEBACK_PATH_PATTERN.finditer(output)))


def extract_assertion_signatures(output: str) -> tuple[str, ...]:
    """Extract ordered assertion/failure signatures from pytest output."""
    signatures = [
        normalized
        for match in _ASSERTION_LINE_PATTERN.finditer(output)
        if (normalized := _normalize_signature(match.group("signature"))) is not None
    ]
    return tuple(dict.fromkeys(signatures))


def build_failing_nodes(output: str) -> tuple[FailingNode, ...]:
    """Build typed failing-node records from pytest output."""
    summary_entries = _collect_summary_entries(output)
    fallback_signatures = extract_assertion_signatures(output)
    traceback_locations = tuple(_TRACEBACK_PATH_PATTERN.finditer(output))
    ordered_traceback_paths = tuple(dict.fromkeys(match.group("path") for match in traceback_locations))
    failure_sections = _collect_failure_sections(output)
    nodes: list[FailingNode] = []

    for index, (outcome, nodeid, summary_signature) in enumerate(summary_entries):
        node_path = nodeid.split("::", 1)[0]
        signature = summary_signature
        if signature is None and len(summary_entries) == 1 and len(fallback_signatures) == 1:
            signature = fallback_signatures[0]
        section_locations = failure_sections[index] if index < len(failure_sections) else ()
        failure_path, failure_line, traceback_paths, trustworthy_attribution = _traceback_for_node(
            node_path=node_path,
            section_locations=section_locations,
            traceback_locations=traceback_locations,
            fallback_index=index,
            ordered_traceback_paths=ordered_traceback_paths,
            fallback_path=node_path,
        )
        nodes.append(
            FailingNode(
                nodeid=nodeid,
                path=node_path,
                outcome=outcome,
                assertion_signature=signature,
                failure_path=failure_path,
                failure_line=failure_line,
                traceback_paths=traceback_paths,
                trustworthy_attribution=trustworthy_attribution,
            )
        )
    return tuple(nodes)


def _collect_summary_entries(output: str) -> tuple[tuple[Literal["FAILED", "ERROR"], str, str | None], ...]:
    entries: list[tuple[Literal["FAILED", "ERROR"], str, str | None]] = []
    seen: set[str] = set()
    for line in output.splitlines():
        parsed_line = _parse_summary_entry_line(line)
        if parsed_line is None:
            continue
        outcome, nodeid, signature = parsed_line
        if nodeid in seen:
            continue
        seen.add(nodeid)
        entries.append((outcome, nodeid, signature))
    return tuple(entries)


def _parse_summary_entry_line(line: str) -> tuple[Literal["FAILED", "ERROR"], str, str | None] | None:
    for raw_outcome in ("FAILED", "ERROR"):
        prefix = f"{raw_outcome} "
        if not line.startswith(prefix):
            continue
        remainder = line[len(prefix) :].strip()
        if not remainder:
            return None
        nodeid, separator, raw_signature = remainder.rpartition(" - ")
        if separator and _can_split_summary_signature(nodeid, raw_signature):
            return raw_outcome, nodeid, _normalize_signature(raw_signature)
        return raw_outcome, remainder, None
    return None


def _traceback_for_node(
    *,
    node_path: str,
    section_locations: tuple[re.Match[str], ...],
    traceback_locations: tuple[re.Match[str], ...],
    fallback_index: int,
    ordered_traceback_paths: tuple[str, ...],
    fallback_path: str,
) -> tuple[str | None, int | None, tuple[str, ...], bool]:
    matching_locations = [match for match in section_locations if match.group("path") == node_path]
    if matching_locations:
        first = matching_locations[0]
        paths = tuple(dict.fromkeys(match.group("path") for match in section_locations))
        return first.group("path"), int(first.group("lineno")), paths, True

    if section_locations:
        first = section_locations[0]
        paths = tuple(dict.fromkeys(match.group("path") for match in section_locations))
        return first.group("path"), int(first.group("lineno")), paths, True

    if fallback_index < len(ordered_traceback_paths):
        fallback_trace_path = ordered_traceback_paths[fallback_index]
        for match in traceback_locations:
            if match.group("path") == fallback_trace_path:
                return fallback_trace_path, int(match.group("lineno")), (fallback_trace_path,), True

    if traceback_locations:
        first = traceback_locations[0]
        paths = tuple(dict.fromkeys(match.group("path") for match in traceback_locations))
        return first.group("path"), int(first.group("lineno")), paths, True

    return fallback_path, None, (fallback_path,), False


def _normalize_signature(signature: str | None) -> str | None:
    if signature is None:
        return None
    normalized = " ".join(signature.strip().split())
    return normalized or None


def _normalize_repo_relative_path(path: str | None, *, repo_root: Path | None = None) -> str | None:
    if not isinstance(path, str):
        return None
    normalized = path.strip().replace("\\", "/")
    if not normalized:
        return None
    if normalized.startswith("/"):
        if repo_root is None:
            return None
        try:
            repo_root_posix = PurePosixPath(repo_root.resolve().as_posix())
        except OSError:
            return None
        absolute_path = PurePosixPath(normalized)
        if absolute_path == repo_root_posix:
            return "."
        try:
            normalized = str(absolute_path.relative_to(repo_root_posix))
        except ValueError:
            return None
    if ":/" in normalized:
        return None
    if normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = str(PurePosixPath(normalized))
    if PurePath(normalized).is_absolute():
        return None
    if normalized.startswith("../") or normalized == "..":
        return None
    return normalized


def _normalize_changed_paths(
    paths: tuple[str, ...] | list[str] | None,
    *,
    repo_root: Path | None = None,
) -> tuple[str, ...] | None:
    if paths is None:
        return None
    normalized: list[str] = []
    for path in paths:
        normalized_path = _normalize_repo_relative_path(path, repo_root=repo_root)
        if normalized_path is None:
            return None
        normalized.append(normalized_path)
    return tuple(dict.fromkeys(normalized))


def _classify_node_diff_scope(
    node: FailingNode,
    changed_paths: tuple[str, ...],
    *,
    repo_root: Path | None = None,
) -> NodeDiffScopeClassification:
    scoped_paths = _normalize_node_scope_paths(node, repo_root=repo_root)
    if scoped_paths is None:
        return NodeDiffScopeClassification(
            nodeid=node.nodeid,
            outcome="unavailable",
            scoped_paths=(),
            detail="node had an attributable path outside the repo scope",
        )
    if not scoped_paths:
        return NodeDiffScopeClassification(
            nodeid=node.nodeid,
            outcome="unavailable",
            scoped_paths=(),
            detail="node had no attributable repo-relative paths",
        )

    matched = tuple(path for path in scoped_paths if path in changed_paths)
    return NodeDiffScopeClassification(
        nodeid=node.nodeid,
        outcome="inside_diff" if matched else "outside_diff",
        scoped_paths=scoped_paths,
        matched_changed_paths=matched,
    )


def _normalize_node_scope_paths(
    node: FailingNode,
    *,
    repo_root: Path | None = None,
) -> tuple[str, ...] | None:
    if not node.trustworthy_attribution:
        return ()
    normalized_paths: list[str] = []
    for raw_path in (node.path, node.failure_path, *node.traceback_paths):
        if not isinstance(raw_path, str):
            continue
        normalized_raw_path = raw_path.strip()
        if not normalized_raw_path:
            continue
        normalized = _normalize_repo_relative_path(normalized_raw_path, repo_root=repo_root)
        if normalized is None:
            return None
        normalized_paths.append(normalized)
    return tuple(dict.fromkeys(normalized_paths))


def _find_pytest_token_index(tokens: list[str]) -> int | None:
    for index, token in enumerate(tokens):
        normalized = PurePosixPath(token).name.lower()
        if normalized in _PYTEST_EXECUTABLE_NAMES:
            return index
    return None


def _strip_fail_fast_from_short_option_bundle(token: str) -> str | None:
    if not token.startswith("-") or token.startswith("--"):
        return None
    option_bundle = token[1:]
    if not option_bundle:
        return None
    if option_bundle == "x":
        return ""
    if option_bundle.isalpha():
        filtered = option_bundle.replace("x", "")
        return f"-{filtered}" if filtered else ""
    return token


def _short_option_bundle_uses_fail_fast(token: str) -> bool:
    if not token.startswith("-") or token.startswith("--"):
        return False
    option_bundle = token[1:]
    return len(option_bundle) > 1 and option_bundle.isalpha() and "x" in option_bundle


def _can_split_summary_signature(nodeid: str, raw_signature: str) -> bool:
    normalized_signature = _normalize_signature(raw_signature)
    return normalized_signature is not None and _looks_like_complete_pytest_nodeid(nodeid)


def _looks_like_complete_pytest_nodeid(nodeid: str) -> bool:
    if ".py" not in nodeid:
        return False
    bracket_depth = 0
    for character in nodeid:
        if character == "[":
            bracket_depth += 1
        elif character == "]":
            bracket_depth -= 1
            if bracket_depth < 0:
                return False
    return bracket_depth == 0


def _maxfail_value_is_positive(raw: str) -> bool:
    try:
        return int(raw) > 0
    except ValueError:
        return bool(raw)


def _collect_failure_sections(output: str) -> tuple[tuple[re.Match[str], ...], ...]:
    headings = list(_FAILURE_HEADING_PATTERN.finditer(output))
    if not headings:
        return ()

    summary_match = _SHORT_SUMMARY_PATTERN.search(output)
    summary_start = summary_match.start() if summary_match is not None else len(output)
    sections: list[tuple[re.Match[str], ...]] = []
    for index, heading in enumerate(headings):
        section_start = heading.start()
        next_heading_start = headings[index + 1].start() if index + 1 < len(headings) else summary_start
        section_locations = tuple(_TRACEBACK_PATH_PATTERN.finditer(output, section_start, next_heading_start))
        if section_locations:
            sections.append(section_locations)
    return tuple(sections)
