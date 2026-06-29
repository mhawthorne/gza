"""Tests for the host-side behavior conformance monitor."""

import threading
from datetime import UTC, datetime
from pathlib import Path

from gza.behavior_monitor import parse_behavior_check_report, run_behavior_monitor_cycle
from gza.config import Config
from gza.db import SqliteTaskStore

from .conftest import invoke_gza, make_store, setup_config

_REPORT = """# Behavior conformance check

## Machine-readable findings

```json
[
  {
    "assertion_id": "LE-§6-IMPROVE-CHAIN",
    "verdict": "DIVERGES",
    "recommendation": "code bug",
    "spec_file": "specs/behavior/lifecycle-engine.md",
    "spec_section": "§6",
    "summary": "Improve chain queries follow the implementation link instead of the review link.",
    "evidence": [
      {
        "path": "src/gza/runner.py",
        "line": 123,
        "note": "Filters by the implementation link, so review-linked retries are missed."
      }
    ],
    "report_path": "reviews/20260629080000-behavior-check.md"
  },
  {
    "assertion_id": "WS-S7-BOUNDED-WORK-CREATION",
    "verdict": "UNDETERMINED",
    "recommendation": null,
    "spec_file": "specs/behavior/watch-supervisor.md",
    "spec_section": "§7",
    "summary": "The relevant queue-shaping path could not be proven from static inspection.",
    "evidence": [],
    "report_path": "reviews/20260629080000-behavior-check.md"
  }
]
```
"""

_REPORT_WITH_BRACKETED_STRINGS = """# Behavior conformance check

## Machine-readable findings

```json
[
  {
    "assertion_id": "LE-§6-IMPROVE-CHAIN",
    "verdict": "DIVERGES",
    "recommendation": "code bug",
    "spec_file": "specs/behavior/lifecycle-engine.md",
    "spec_section": "§6",
    "summary": "Improve chain queries stay pending [stale review] instead of following the review link.",
    "evidence": [
      {
        "path": "src/gza/runner.py",
        "line": 123,
        "note": "The review refresh path is skipped when the blocker note mentions [stale review]."
      }
    ],
    "report_path": "reviews/20260629090000-behavior-check.md"
  }
]
```
"""

_TRUNCATED_REPORT = """# Behavior conformance check

## Machine-readable findings

```json
[
  {
    "assertion_id": "LE-§6-IMPROVE-CHAIN",
    "verdict": "DIVERGES",
    "recommendation": "code bug",
    "spec_file": "specs/behavior/lifecycle-engine.md",
    "spec_section": "§6",
    "summary": "Improve chain queries follow the implementation link instead of the review link.",
    "evidence": [
      {
        "path": "src/gza/runner.py",
        "line": 123,
        "note": "Filters by the implementation link, so review-linked retries are missed."
      }
    ],
    "report_path": "reviews/20260629080000-behavior-check.md"
  }
]
"""


def _config_and_store(tmp_path: Path) -> tuple[Config, SqliteTaskStore]:
    setup_config(tmp_path)
    return Config.load(tmp_path), make_store(tmp_path)


def test_behavior_monitor_disabled_config_exits_without_creating_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        config_path.read_text() + "\nbehavior_monitor:\n  enabled: false\n",
        encoding="utf-8",
    )
    store = make_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    result = invoke_gza("behavior-monitor", "--once", "--project", str(tmp_path))

    assert result.returncode == 1
    assert "behavior_monitor.enabled=false" in result.stdout
    assert store.get_all() == []


def test_behavior_monitor_dry_run_reports_without_filing(tmp_path: Path, monkeypatch) -> None:
    config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    result = invoke_gza("behavior-monitor", "--once", "--dry-run", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "would file: 1 new" in result.stdout
    assert "undetermined: 1" in result.stdout
    assert [task for task in store.get_all() if task.task_type != "internal"] == []


def test_behavior_monitor_force_overrides_disabled_config(tmp_path: Path, monkeypatch) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(
        config_path.read_text() + "\nbehavior_monitor:\n  enabled: false\n",
        encoding="utf-8",
    )
    store = make_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    result = invoke_gza("behavior-monitor", "--once", "--force", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "filed: 1 new" in result.stdout
    assert len([task for task in store.get_all() if task.task_type == "implement"]) == 1


def test_behavior_monitor_successful_cycle_files_followup_from_fake_report(
    tmp_path: Path, monkeypatch
) -> None:
    _config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    result = invoke_gza("behavior-monitor", "--once", "--project", str(tmp_path))

    assert result.returncode == 0
    assert "filed: 1 new" in result.stdout
    followups = [task for task in store.get_all() if task.task_type == "implement"]
    assert len(followups) == 1
    assert followups[0].trigger_source == "behavior-monitor"
    assert "behavior-code-bug" in followups[0].tags
    assert "behavior-conformance" in followups[0].tags
    assert "LE-§6-IMPROVE-CHAIN" in followups[0].prompt
    assert "reviews/20260629080000-behavior-check.md" in followups[0].prompt


def test_parse_behavior_check_report_accepts_brackets_inside_json_strings() -> None:
    findings = parse_behavior_check_report(_REPORT_WITH_BRACKETED_STRINGS)

    assert len(findings) == 1
    assert findings[0].summary.endswith("[stale review] instead of following the review link.")
    assert findings[0].evidence[0].note.endswith("mentions [stale review].")


def test_behavior_monitor_dedupes_across_cycles_with_active_linked_task(
    tmp_path: Path, monkeypatch
) -> None:
    config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    first = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )
    second = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert first.successful
    assert len(first.new_task_ids) == 1
    assert second.successful
    assert second.new_task_ids == ()
    assert second.deduped_count == 1
    assert len([task for task in store.get_all() if task.task_type == "implement"]) == 1


def test_behavior_monitor_cycle_files_and_dedupes_report_with_bracketed_strings(
    tmp_path: Path, monkeypatch
) -> None:
    config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: (
            "gza-1",
            "reviews/20260629090000-behavior-check.md",
            _REPORT_WITH_BRACKETED_STRINGS,
        ),
    )

    first = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )
    second = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert first.successful
    assert len(first.new_task_ids) == 1
    task = store.get(first.new_task_ids[0])
    assert task is not None
    assert "[stale review]" in task.prompt
    assert second.successful
    assert second.new_task_ids == ()
    assert second.deduped_count == 1


def test_behavior_monitor_unknown_verdict_fails_closed_and_keeps_open_finding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, store = _config_and_store(tmp_path)
    store.upsert_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
        report_path="reviews/20260628080000-behavior-check.md",
        linked_task_id="gza-existing",
    )
    malformed_report = _REPORT.replace('"verdict": "DIVERGES"', '"verdict": "BROKEN"', 1)
    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", malformed_report),
    )

    result = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert result.successful is False
    assert "invalid verdict" in (result.error or "")
    finding = store.get_open_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
    )
    assert finding is not None
    assert finding.state == "open"


def test_behavior_monitor_truncated_appendix_fails_closed_before_absent_resolution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, store = _config_and_store(tmp_path)
    original = store.upsert_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
        report_path="reviews/20260628080000-behavior-check.md",
        linked_task_id="gza-existing",
    )
    original_last_seen = original.last_seen

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _TRUNCATED_REPORT),
    )

    result = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert result.successful is False
    assert result.error == "report is missing the machine-readable findings appendix"
    finding = store.plan_behavior_finding_observation(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
    )
    assert finding.existing_finding is not None
    assert finding.existing_finding.state == "open"
    assert finding.existing_finding.last_seen == original_last_seen


def test_behavior_monitor_recurrence_files_new_generation_after_merge(
    tmp_path: Path, monkeypatch
) -> None:
    config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    first = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )
    assert len(first.new_task_ids) == 1
    original = store.get(first.new_task_ids[0])
    assert original is not None
    original.status = "completed"
    original.completed_at = datetime(2026, 6, 29, 9, 0, tzinfo=UTC)
    original.merge_status = "merged"
    original.merged_at = datetime(2026, 6, 29, 9, 5, tzinfo=UTC)
    store.update(original)

    second = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert second.successful
    assert len(second.new_task_ids) == 1
    assert second.new_task_ids[0] != first.new_task_ids[0]
    recurring = store.get(second.new_task_ids[0])
    assert recurring is not None
    assert f"Previous linked task: {first.new_task_ids[0]}" in recurring.prompt
    assert "New generation: 2" in recurring.prompt
    finding = store.get_open_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
    )
    assert finding is not None
    assert finding.linked_task_id == recurring.id
    assert finding.generation == 2


def test_behavior_monitor_suppressed_recurrence_stays_observed_and_can_file_later(
    tmp_path: Path, monkeypatch
) -> None:
    config, store = _config_and_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    first = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )
    assert len(first.new_task_ids) == 1
    original = store.get(first.new_task_ids[0])
    assert original is not None
    original.status = "completed"
    original.completed_at = datetime(2026, 6, 29, 9, 0, tzinfo=UTC)
    original.merge_status = "merged"
    original.merged_at = datetime(2026, 6, 29, 9, 5, tzinfo=UTC)
    store.update(original)

    suppressed = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=0,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert suppressed.successful
    assert suppressed.new_task_ids == ()
    assert suppressed.suppressed_count == 1
    assert suppressed.resolved_count == 0
    observed = store.get_open_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
    )
    assert observed is not None
    assert observed.state == "open"
    assert observed.linked_task_id == first.new_task_ids[0]
    assert observed.generation == 1

    reopened = run_behavior_monitor_cycle(
        config,
        store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    assert reopened.successful
    assert len(reopened.new_task_ids) == 1
    recurring = store.get(reopened.new_task_ids[0])
    assert recurring is not None
    assert f"Previous linked task: {first.new_task_ids[0]}" in recurring.prompt
    assert "New generation: 2" in recurring.prompt
    finding = store.get_open_behavior_finding(
        check_name="gza-behavior-check",
        assertion_id="LE-§6-IMPROVE-CHAIN",
        recommendation="code bug",
        summary="Improve chain queries follow the implementation link instead of the review link.",
        spec_file="specs/behavior/lifecycle-engine.md",
        spec_section="§6",
    )
    assert finding is not None
    assert finding.state == "open"
    assert finding.linked_task_id == recurring.id
    assert finding.generation == 2


def test_behavior_monitor_concurrent_cycles_file_one_followup_under_held_lease(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, first_store = _config_and_store(tmp_path)
    second_store = make_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    entered_gap = threading.Event()
    release_gap = threading.Event()
    first_result: dict[str, object] = {}
    original_plan = first_store.plan_behavior_finding_observation

    def blocking_plan(**kwargs):
        plan = original_plan(**kwargs)
        entered_gap.set()
        assert release_gap.wait(timeout=5)
        return plan

    monkeypatch.setattr(first_store, "plan_behavior_finding_observation", blocking_plan)

    def run_first() -> None:
        first_result["result"] = run_behavior_monitor_cycle(
            config,
            first_store,
            filing_tag=config.behavior_monitor.tag,
            max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
            check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
            dry_run=False,
            file_undetermined=False,
        )

    worker = threading.Thread(target=run_first)
    worker.start()
    assert entered_gap.wait(timeout=5)

    second = run_behavior_monitor_cycle(
        config,
        second_store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    release_gap.set()
    worker.join(timeout=5)
    assert not worker.is_alive()

    first = first_result["result"]
    assert isinstance(first, type(second))
    assert first.successful
    assert len(first.new_task_ids) == 1
    assert second.successful is False
    assert second.error == "another behavior monitor check is already running for this project"
    assert len([task for task in first_store.get_all() if task.task_type == "implement"]) == 1


def test_behavior_monitor_keeps_lease_until_followup_filing_finishes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config, first_store = _config_and_store(tmp_path)
    second_store = make_store(tmp_path)

    monkeypatch.setattr(
        "gza.behavior_monitor._run_behavior_check_task",
        lambda *_args, **_kwargs: ("gza-1", "reviews/20260629080000-behavior-check.md", _REPORT),
    )

    entered_gap = threading.Event()
    release_gap = threading.Event()
    original_upsert = first_store.upsert_behavior_finding
    thread_result: dict[str, object] = {}

    def blocking_upsert(*args, **kwargs):
        entered_gap.set()
        assert release_gap.wait(timeout=5)
        return original_upsert(*args, **kwargs)

    monkeypatch.setattr(first_store, "upsert_behavior_finding", blocking_upsert)

    def run_first() -> None:
        thread_result["result"] = run_behavior_monitor_cycle(
            config,
            first_store,
            filing_tag=config.behavior_monitor.tag,
            max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
            check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
            dry_run=False,
            file_undetermined=False,
        )

    worker = threading.Thread(target=run_first)
    worker.start()
    assert entered_gap.wait(timeout=5)

    second = run_behavior_monitor_cycle(
        config,
        second_store,
        filing_tag=config.behavior_monitor.tag,
        max_new_tasks=config.behavior_monitor.max_new_tasks_per_cycle,
        check_timeout_seconds=config.behavior_monitor.check_timeout_seconds,
        dry_run=False,
        file_undetermined=False,
    )

    release_gap.set()
    worker.join(timeout=5)
    assert not worker.is_alive()

    first = thread_result["result"]
    assert isinstance(first, type(second))
    assert first.successful
    assert second.successful is False
    assert second.error == "another behavior monitor check is already running for this project"
