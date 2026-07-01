"""Host-side behavior conformance monitor helpers and CLI."""

from __future__ import annotations

import argparse
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

from .config import Config, TaskTypeConfig
from .db import (
    SqliteTaskStore,
    Task,
    behavior_check_finding_fingerprint,
)

BEHAVIOR_CHECK_NAME = "gza-behavior-check"
BEHAVIOR_MONITOR_LEASE_NAME = "behavior-monitor"
BEHAVIOR_MONITOR_INTERNAL_TAG = "behavior-monitor"
BEHAVIOR_CHECK_ALLOWED_VERDICTS = frozenset({"HOLDS", "DIVERGES", "UNDETERMINED"})

_APPENDIX_HEADING = "## Machine-readable findings"
_APPENDIX_OPEN_FENCE = "```json"
_APPENDIX_CLOSE_FENCE = "```"


@dataclass(frozen=True)
class BehaviorMonitorEvidence:
    """One machine-readable evidence citation from the behavior-check report."""

    path: str
    line: int | None
    note: str


@dataclass(frozen=True)
class BehaviorMonitorFinding:
    """One parsed machine-readable finding row."""

    assertion_id: str
    verdict: str
    recommendation: str | None
    spec_file: str
    spec_section: str
    summary: str
    evidence: tuple[BehaviorMonitorEvidence, ...]
    report_path: str

    @property
    def dedupe_recommendation(self) -> str:
        if self.verdict == "UNDETERMINED":
            return "undetermined"
        if self.recommendation is None:
            raise ValueError("divergence finding is missing recommendation")
        return self.recommendation

    @property
    def fingerprint(self) -> str:
        return behavior_check_finding_fingerprint(
            check_name=BEHAVIOR_CHECK_NAME,
            assertion_id=self.assertion_id,
            recommendation=self.dedupe_recommendation,
            summary=self.summary,
            spec_file=self.spec_file,
            spec_section=self.spec_section,
        )


@dataclass(frozen=True)
class BehaviorMonitorCycleResult:
    """Operator-facing summary of one monitor pass."""

    dry_run: bool
    check_task_id: str | None
    report_path: str | None
    new_task_ids: tuple[str, ...]
    deduped_count: int
    undetermined_count: int
    suppressed_count: int
    resolved_count: int
    error: str | None = None

    @property
    def successful(self) -> bool:
        return self.error is None


def _build_monitor_task_prompt(*, check_timeout_seconds: int) -> str:
    timeout_minutes = max(1, math.ceil(check_timeout_seconds / 60))
    return (
        "Run /gza-behavior-check against the full behavior spec set.\n\n"
        "Requirements:\n"
        "- Use the tracked behavior specs as the source of truth.\n"
        "- Write the full human-readable report to reviews/<timestamp>-behavior-check.md.\n"
        "- The report MUST end with the required `## Machine-readable findings` JSON appendix.\n"
        "- After writing the report, output only the relative report path, for example "
        "`reviews/20260629080000-behavior-check.md`.\n"
        f"- Keep the full check within the configured timeout budget of about {timeout_minutes} minute(s).\n"
    )


def _config_for_check_timeout(config: Config, *, check_timeout_seconds: int) -> Config:
    task_types = dict(config.task_types)
    existing = task_types.get("internal", TaskTypeConfig())
    task_types["internal"] = replace(
        existing,
        timeout_minutes=max(1, math.ceil(check_timeout_seconds / 60)),
    )
    return replace(config, task_types=task_types)


def _normalize_report_path(raw_output: str) -> str:
    report_path = raw_output.strip()
    if not report_path:
        raise ValueError("behavior monitor check task did not return a report path")
    if Path(report_path).is_absolute():
        raise ValueError("behavior monitor check task returned an absolute report path")
    normalized = Path(report_path)
    if ".." in normalized.parts:
        raise ValueError("behavior monitor check task returned an unsafe report path")
    return normalized.as_posix()


def _extract_machine_readable_findings_body(report_text: str) -> str:
    heading_index = report_text.find(_APPENDIX_HEADING)
    if heading_index < 0:
        raise ValueError("report is missing the machine-readable findings appendix")

    remaining = report_text[heading_index + len(_APPENDIX_HEADING) :]
    open_index = remaining.find(_APPENDIX_OPEN_FENCE)
    if open_index < 0:
        raise ValueError("report is missing the machine-readable findings appendix")

    fenced = remaining[open_index + len(_APPENDIX_OPEN_FENCE) :]
    if fenced.startswith("\r\n"):
        fenced = fenced[2:]
    elif fenced.startswith("\n"):
        fenced = fenced[1:]
    else:
        fenced = fenced.lstrip()

    close_marker = f"\n{_APPENDIX_CLOSE_FENCE}"
    close_index = fenced.find(close_marker)
    if close_index < 0:
        raise ValueError("report is missing the machine-readable findings appendix")
    return fenced[:close_index].strip()


def parse_behavior_check_report(report_text: str) -> list[BehaviorMonitorFinding]:
    """Parse the mandatory machine-readable findings appendix from a report."""
    body = _extract_machine_readable_findings_body(report_text)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError("machine-readable findings appendix is not valid JSON") from exc
    if not isinstance(payload, list):
        raise ValueError("machine-readable findings appendix must be a JSON array")

    findings: list[BehaviorMonitorFinding] = []
    for index, raw_finding in enumerate(payload):
        if not isinstance(raw_finding, dict):
            raise ValueError(f"finding {index + 1} is not a JSON object")
        raw_finding = cast("dict[str, object]", raw_finding)
        evidence_rows = raw_finding.get("evidence")
        evidence: list[BehaviorMonitorEvidence] = []
        if not isinstance(evidence_rows, list):
            raise ValueError(f"finding {index + 1} has invalid evidence")
        for raw_evidence in evidence_rows:
            if not isinstance(raw_evidence, dict):
                raise ValueError(f"finding {index + 1} has malformed evidence")
            raw_evidence = cast("dict[str, object]", raw_evidence)
            line = raw_evidence.get("line")
            evidence.append(
                BehaviorMonitorEvidence(
                    path=str(raw_evidence.get("path", "")),
                    line=line if isinstance(line, int) and not isinstance(line, bool) else None,
                    note=str(raw_evidence.get("note", "")),
                )
            )
        recommendation = raw_finding.get("recommendation")
        if recommendation is not None and not isinstance(recommendation, str):
            raise ValueError(f"finding {index + 1} has invalid recommendation")
        findings.append(
            BehaviorMonitorFinding(
                assertion_id=str(raw_finding.get("assertion_id", "")),
                verdict=str(raw_finding.get("verdict", "")),
                recommendation=recommendation,
                spec_file=str(raw_finding.get("spec_file", "")),
                spec_section=str(raw_finding.get("spec_section", "")),
                summary=str(raw_finding.get("summary", "")),
                evidence=tuple(evidence),
                report_path=str(raw_finding.get("report_path", "")),
            )
        )

    for index, finding in enumerate(findings, start=1):
        if not all(
            (
                finding.assertion_id,
                finding.verdict,
                finding.spec_file,
                finding.spec_section,
                finding.summary,
                finding.report_path,
            )
        ):
            raise ValueError(f"finding {index} is missing required fields")
        if finding.verdict not in BEHAVIOR_CHECK_ALLOWED_VERDICTS:
            raise ValueError(
                f"finding {index} has invalid verdict {finding.verdict!r}; "
                "expected one of HOLDS, DIVERGES, UNDETERMINED"
            )
        if finding.verdict == "DIVERGES" and finding.recommendation not in {
            "code bug",
            "spec gap",
            "ambiguous",
        }:
            raise ValueError(f"finding {index} has invalid divergence recommendation")
        if finding.verdict in {"HOLDS", "UNDETERMINED"} and finding.recommendation is not None:
            raise ValueError(f"finding {index} must not set recommendation for {finding.verdict}")
    return findings


def _build_evidence_block(finding: BehaviorMonitorFinding) -> str:
    if not finding.evidence:
        return "- No code evidence was safely available in the check report."
    return "\n".join(
        (
            f"- `{item.path}:{item.line}` — {item.note}"
            if item.line is not None
            else f"- `{item.path}` — {item.note}"
        )
        for item in finding.evidence
    )


def _followup_task_blueprint(
    finding: BehaviorMonitorFinding,
    *,
    filing_tag: str,
    recurrence_prior_task_id: str | None = None,
    recurrence_generation: int = 1,
) -> tuple[str, tuple[str, ...], str]:
    header_lines = [
        f"Behavior conformance finding: {finding.assertion_id}",
        "",
        f"Classification: {finding.dedupe_recommendation}",
        f"Spec: {finding.spec_file} {finding.spec_section}",
        f"Report: {finding.report_path}",
        "",
        "Summary:",
        finding.summary,
        "",
        "Evidence:",
        _build_evidence_block(finding),
        "",
    ]
    if recurrence_prior_task_id is not None:
        header_lines.extend(
            (
                "Recurrence:",
                f"- Previous linked task: {recurrence_prior_task_id}",
                f"- New generation: {recurrence_generation}",
                "",
            )
        )
    if finding.dedupe_recommendation == "code bug":
        return (
            "implement",
            (filing_tag, "behavior-code-bug", "auto-filed"),
            "\n".join(
                (
                    *header_lines,
                    "The behavior spec is the source of truth for this task.",
                    "Non-goal: do not silently edit the behavior spec to match the current code.",
                    "",
                    "Acceptance criteria:",
                    "- Update the implementation to satisfy the cited behavior-spec clause.",
                    "- Add or update targeted regression coverage for this divergence.",
                    "- Keep the behavior spec unchanged unless a separate validated spec-gap decision requires it.",
                )
            ),
        )
    if finding.dedupe_recommendation == "spec gap":
        return (
            "implement",
            (filing_tag, "behavior-spec-gap", "specs-behavior", "auto-filed"),
            "\n".join(
                (
                    *header_lines,
                    "The report recommends a behavior-spec correction rather than a code-only fix.",
                    "",
                    "Acceptance criteria:",
                    "- Update the relevant behavior spec to resolve the cited gap.",
                    "- Preserve the contract/code distinction explicitly in the spec text.",
                    "- Run the spec-coherence gate after editing specs/behavior/**.",
                )
            ),
        )
    if finding.dedupe_recommendation == "ambiguous":
        return (
            "plan",
            (filing_tag, "behavior-ambiguous", "specs-behavior", "auto-filed"),
            "\n".join(
                (
                    *header_lines,
                    "Decide whether the code, the behavior spec, or both should change.",
                    "",
                    "Acceptance criteria:",
                    "- Produce an implementation-ready plan that resolves the ambiguity.",
                    "- Name the missing decision explicitly and cite the owning behavior spec clause.",
                )
            ),
        )
    return (
        "plan",
        (filing_tag, "behavior-undetermined", "specs-behavior", "auto-filed"),
        "\n".join(
            (
                *header_lines,
                "The conformance check could not safely determine whether code or spec diverges.",
                "",
                "Acceptance criteria:",
                "- Investigate the cited area and determine whether the result is HOLDS or DIVERGES.",
                "- If it diverges, file or continue the concrete repair/spec-gap task with evidence.",
            )
        ),
    )


def _create_followup_task(
    store: SqliteTaskStore,
    finding: BehaviorMonitorFinding,
    *,
    filing_tag: str,
    recurrence_prior_task_id: str | None = None,
    recurrence_generation: int = 1,
) -> Task:
    task_type, tags, prompt = _followup_task_blueprint(
        finding,
        filing_tag=filing_tag,
        recurrence_prior_task_id=recurrence_prior_task_id,
        recurrence_generation=recurrence_generation,
    )
    return store.add(
        prompt=prompt,
        task_type=task_type,
        tags=tags,
        trigger_source="behavior-monitor",
    )


def _run_behavior_check_task(
    config: Config,
    store: SqliteTaskStore,
    *,
    filing_tag: str,
    check_timeout_seconds: int,
) -> tuple[str, str, str]:
    from . import runner as runner_mod

    check_task = store.add(
        prompt=_build_monitor_task_prompt(check_timeout_seconds=check_timeout_seconds),
        task_type="internal",
        tags=(BEHAVIOR_MONITOR_INTERNAL_TAG, filing_tag),
        skip_learnings=True,
        trigger_source="behavior-monitor",
    )
    if check_task.id is None:
        raise RuntimeError("failed to create behavior monitor internal task")

    exit_code = runner_mod.run(
        _config_for_check_timeout(config, check_timeout_seconds=check_timeout_seconds),
        task_id=check_task.id,
    )
    refreshed = store.get(check_task.id)
    if exit_code != 0 or refreshed is None or refreshed.status != "completed":
        raise RuntimeError(f"behavior monitor check task {check_task.id} did not complete successfully")
    report_path = _normalize_report_path(refreshed.output_content or "")
    report_file = config.project_dir / report_path
    if not report_file.exists():
        raise RuntimeError(f"behavior monitor report path does not exist: {report_path}")
    return check_task.id, report_path, report_file.read_text(encoding="utf-8")


def run_behavior_monitor_cycle(
    config: Config,
    store: SqliteTaskStore,
    *,
    filing_tag: str,
    max_new_tasks: int,
    check_timeout_seconds: int,
    dry_run: bool,
    file_undetermined: bool,
) -> BehaviorMonitorCycleResult:
    """Run one behavior-monitor check pass."""
    owner_pid = os.getpid()
    owner_token = uuid.uuid4().hex
    lease = store.try_acquire_project_lease(
        lease_name=BEHAVIOR_MONITOR_LEASE_NAME,
        owner_pid=owner_pid,
        owner_token=owner_token,
    )
    if lease is None:
        return BehaviorMonitorCycleResult(
            dry_run=dry_run,
            check_task_id=None,
            report_path=None,
            new_task_ids=(),
            deduped_count=0,
            undetermined_count=0,
            suppressed_count=0,
            resolved_count=0,
            error="another behavior monitor check is already running for this project",
        )

    try:
        try:
            check_task_id, report_path, report_text = _run_behavior_check_task(
                config,
                store,
                filing_tag=filing_tag,
                check_timeout_seconds=check_timeout_seconds,
            )
            findings = parse_behavior_check_report(report_text)
        except Exception as exc:
            return BehaviorMonitorCycleResult(
                dry_run=dry_run,
                check_task_id=check_task_id if "check_task_id" in locals() else None,
                report_path=report_path if "report_path" in locals() else None,
                new_task_ids=(),
                deduped_count=0,
                undetermined_count=0,
                suppressed_count=0,
                resolved_count=0,
                error=str(exc),
            )

        actionable: list[BehaviorMonitorFinding] = []
        undetermined_count = 0
        for finding in findings:
            if finding.verdict == "DIVERGES":
                actionable.append(finding)
            elif finding.verdict == "UNDETERMINED":
                undetermined_count += 1
                if file_undetermined:
                    actionable.append(finding)

        new_task_ids: list[str] = []
        deduped_count = 0
        suppressed_count = 0
        seen_fingerprints: set[str] = set()

        for finding in actionable:
            observation = store.plan_behavior_finding_observation(
                check_name=BEHAVIOR_CHECK_NAME,
                assertion_id=finding.assertion_id,
                recommendation=finding.dedupe_recommendation,
                summary=finding.summary,
                spec_file=finding.spec_file,
                spec_section=finding.spec_section,
            )
            seen_fingerprints.add(observation.fingerprint)
            if observation.dedupable_finding is not None:
                deduped_count += 1
                if not dry_run:
                    store.upsert_behavior_finding(
                        check_name=BEHAVIOR_CHECK_NAME,
                        assertion_id=finding.assertion_id,
                        recommendation=finding.dedupe_recommendation,
                        summary=finding.summary,
                        spec_file=finding.spec_file,
                        spec_section=finding.spec_section,
                        report_path=finding.report_path,
                    )
                continue
            if len(new_task_ids) >= max_new_tasks:
                suppressed_count += 1
                if not dry_run and observation.existing_finding is not None:
                    store.observe_behavior_finding(
                        check_name=BEHAVIOR_CHECK_NAME,
                        assertion_id=finding.assertion_id,
                        recommendation=finding.dedupe_recommendation,
                        summary=finding.summary,
                        spec_file=finding.spec_file,
                        spec_section=finding.spec_section,
                        report_path=finding.report_path,
                    )
                continue
            if dry_run:
                new_task_ids.append(f"dry-run:{finding.assertion_id}")
                continue
            task = _create_followup_task(
                store,
                finding,
                filing_tag=filing_tag,
                recurrence_prior_task_id=observation.prior_linked_task_id
                if observation.next_generation > 1
                else None,
                recurrence_generation=observation.next_generation,
            )
            assert task.id is not None
            new_task_ids.append(task.id)
            store.upsert_behavior_finding(
                check_name=BEHAVIOR_CHECK_NAME,
                assertion_id=finding.assertion_id,
                recommendation=finding.dedupe_recommendation,
                summary=finding.summary,
                spec_file=finding.spec_file,
                spec_section=finding.spec_section,
                report_path=finding.report_path,
                linked_task_id=task.id,
            )

        resolved_count = 0
        if not dry_run:
            run = store.record_behavior_check_run(
                check_name=BEHAVIOR_CHECK_NAME,
                successful=True,
                report_path=report_path,
            )
            resolved_count = len(
                [
                    finding
                    for finding in store.resolve_absent_behavior_findings(
                        run=run,
                        seen_fingerprints=seen_fingerprints,
                    )
                    if finding.state == "resolved"
                ]
            )

        return BehaviorMonitorCycleResult(
            dry_run=dry_run,
            check_task_id=check_task_id,
            report_path=report_path,
            new_task_ids=tuple(new_task_ids),
            deduped_count=deduped_count,
            undetermined_count=undetermined_count,
            suppressed_count=suppressed_count,
            resolved_count=resolved_count,
        )
    finally:
        store.release_project_lease(
            lease_name=BEHAVIOR_MONITOR_LEASE_NAME,
            owner_token=owner_token,
        )


def _print_cycle_result(result: BehaviorMonitorCycleResult) -> None:
    if result.error is not None:
        print(f"behavior monitor: {result.error}")
        if result.check_task_id is not None:
            print(f"check task: {result.check_task_id}")
        if result.report_path is not None:
            print(f"report: {result.report_path}")
        return

    if result.check_task_id is not None:
        print(f"check task: {result.check_task_id}")
    if result.report_path is not None:
        print(f"report: {result.report_path}")
    mode_label = "would file" if result.dry_run else "filed"
    print(
        f"{mode_label}: {len(result.new_task_ids)} new, "
        f"deduped: {result.deduped_count}, "
        f"undetermined: {result.undetermined_count}, "
        f"suppressed: {result.suppressed_count}, "
        f"resolved: {result.resolved_count}"
    )
    if result.new_task_ids:
        print("tasks: " + ", ".join(result.new_task_ids))


def cmd_behavior_monitor(args: argparse.Namespace) -> int:
    """Run the host-side behavior conformance monitor."""
    config = Config.load(args.project_dir)
    if not config.behavior_monitor.enabled and not bool(getattr(args, "force", False)):
        print(
            "behavior monitor: disabled by config.behavior_monitor.enabled=false; "
            "rerun with --force to override"
        )
        return 1
    store = SqliteTaskStore.from_config(config)
    interval = args.interval if args.interval is not None else config.behavior_monitor.interval_seconds
    filing_tag = args.tag or config.behavior_monitor.tag
    max_new_tasks = (
        args.max_new_tasks
        if args.max_new_tasks is not None
        else config.behavior_monitor.max_new_tasks_per_cycle
    )
    check_timeout_seconds = (
        args.check_timeout
        if args.check_timeout is not None
        else config.behavior_monitor.check_timeout_seconds
    )

    while True:
        result = run_behavior_monitor_cycle(
            config,
            store,
            filing_tag=filing_tag,
            max_new_tasks=max_new_tasks,
            check_timeout_seconds=check_timeout_seconds,
            dry_run=bool(args.dry_run),
            file_undetermined=config.behavior_monitor.file_undetermined,
        )
        _print_cycle_result(result)
        if args.once:
            return 0 if result.successful else 1
        time.sleep(interval)
