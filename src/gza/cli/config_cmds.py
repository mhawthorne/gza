"""Configuration, stats, cleanup, init, and skills-install CLI commands."""

import argparse
import copy
import json
import logging
import os
import re
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, cast

from rich.table import Table

from .. import colors as _colors
from ..artifact_paths import (
    InvalidArtifactPathError,
    is_archived_artifact_path,
    is_live_artifact_path,
    resolve_artifact_path,
)
from ..config import Config, ConfigError, _generate_project_id
from ..config_examples import (
    BranchStrategyRender,
    ConfigExampleRenderOptions,
    default_example_path,
    render_config_example,
)
from ..config_schema import CONFIG_KEY_REGISTRY
from ..console import console
from ..db import SqliteTaskStore, Task, TaskArtifact, task_id_numeric_key
from ..git import Git
from ..learnings import DEFAULT_LEARNINGS_WINDOW, regenerate_learnings
from ..log_paths import paired_log_paths, slug_from_log_path
from ..merge_state import resolve_task_merge_state_for_target
from ..task_slug import get_slug_display_text
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import get_review_verdict, get_store, resolve_id

logger = logging.getLogger(__name__)

_INIT_SHARED_DB_PATH = "~/.gza/gza.db"
_PREFLIGHT_ERROR_PATTERN = re.compile(r"(error|model|invalid|unknown|not found)", re.IGNORECASE)
_PREFLIGHT_DETAIL_MAX_LEN = 120


@dataclass(frozen=True)
class _UnmergedCleanupSets:
    task_ids: set[str]
    task_slugs: set[str]
    warnings: tuple[str, ...] = ()


class _UnmergedCleanupCollectionError(RuntimeError):
    """Raised when keep-unmerged preservation evidence cannot be collected safely."""


def _normalize_unmerged_cleanup_sets(
    collected: _UnmergedCleanupSets | tuple[set[str], set[str]],
) -> _UnmergedCleanupSets:
    """Accept legacy tuple stubs and normalize them to the shared result shape."""
    if isinstance(collected, _UnmergedCleanupSets):
        return collected
    task_ids, task_slugs = collected
    return _UnmergedCleanupSets(task_ids=task_ids, task_slugs=task_slugs)


def _collect_unmerged_cleanup_sets(store: SqliteTaskStore, git: Git) -> _UnmergedCleanupSets:
    """Return task IDs and slugs that should be preserved by keep-unmerged cleanup."""
    unmerged_task_ids: set[str] = set()
    unmerged_task_slugs: set[str] = set()
    warnings: list[str] = []
    try:
        default_branch = git.default_branch()
        for task in store.get_all():
            if not task.branch or not task.has_commits:
                continue
            try:
                if (
                    resolve_task_merge_state_for_target(
                        store=store,
                        task=task,
                        git=git,
                        target_branch=default_branch,
                    )
                    != "merged"
                    and not git.is_merged(task.branch, default_branch)
                ):
                    if task.id is not None:
                        unmerged_task_ids.add(task.id)
                    if task.slug:
                        unmerged_task_slugs.add(task.slug)
            except Exception as exc:
                if task.id is not None:
                    unmerged_task_ids.add(task.id)
                if task.slug:
                    unmerged_task_slugs.add(task.slug)
                warning = (
                    "Warning: Preserving task "
                    f"{task.id} during --keep-unmerged because merge-state inspection failed: {exc}"
                )
                warnings.append(warning)
                logger.warning(
                    "Failed to check merge state for task %s branch=%s during cleanup",
                    task.id,
                    task.branch,
                    exc_info=True,
                )
    except Exception as exc:
        logger.warning("Could not collect unmerged tasks during cleanup", exc_info=True)
        raise _UnmergedCleanupCollectionError(
            f"Could not collect unmerged tasks during cleanup: {exc}"
        ) from exc
    return _UnmergedCleanupSets(
        task_ids=unmerged_task_ids,
        task_slugs=unmerged_task_slugs,
        warnings=tuple(warnings),
    )


def _preserve_log_group_for_unmerged_task(
    path: Path,
    *,
    unmerged_task_ids: set[str],
    unmerged_task_slugs: set[str],
) -> bool:
    """Return whether a log group belongs to an unmerged task that should be kept."""
    task_stem = slug_from_log_path(path)
    return task_stem in unmerged_task_ids or task_stem in unmerged_task_slugs


def _iter_task_artifacts(store: SqliteTaskStore) -> list[TaskArtifact]:
    """Return all task artifacts visible in the current project store."""
    artifacts: list[TaskArtifact] = []
    for task in store.get_all():
        if task.id is None:
            continue
        artifacts.extend(store.list_artifacts(task.id))
    return artifacts


def _update_task_artifact_path(store: SqliteTaskStore, artifact: TaskArtifact, *, path: str) -> None:
    """Persist a new relative path for an existing task artifact row."""
    store.add_artifact(
        artifact.task_id,
        kind=artifact.kind,
        label=artifact.label,
        path=path,
        content_type=artifact.content_type,
        byte_size=artifact.byte_size,
        sha256=artifact.sha256,
        created_at=artifact.created_at,
        producer=artifact.producer,
        command=artifact.command,
        status=artifact.status,
        exit_status=artifact.exit_status,
        head_sha=artifact.head_sha,
        metadata=artifact.metadata,
        artifact_id=artifact.id,
    )


def _resolve_clean_days(
    args: argparse.Namespace,
    config: Config,
    *,
    default_days: int,
) -> int:
    """Resolve the retention window for clean/archive operations."""
    if args.days is not None:
        return args.days
    return config.cleanup_days if default_days == 30 else default_days


def _percentile(sorted_vals: list[int], p: float) -> int:
    """Return the value at the p-th percentile (nearest-rank method)."""
    if not sorted_vals:
        return 0
    k = max(0, min(len(sorted_vals) - 1, int(len(sorted_vals) * p / 100 + 0.5) - 1))
    return sorted_vals[k]


@dataclass(frozen=True)
class _ScoreRecord:
    week_label: str
    week_iso: str
    score: int
    reviewer_provider: str | None
    reviewer_model: str | None
    implementer_provider: str | None
    implementer_model: str | None
    planner_provider: str | None
    planner_model: str | None


@dataclass(frozen=True)
class CheckTarget:
    provider: str
    model: str | None
    sources: list[str]


@dataclass(frozen=True)
class CheckResult:
    status: str
    detail: str
    duration_s: float


@dataclass(frozen=True)
class _PreflightLogRecord:
    order: int
    timestamp: datetime | None
    payload: dict[str, Any] | None
    raw_text: str


def _display_provider_model(provider: str | None, model: str | None, *, no_plan: bool = False) -> str:
    if no_plan and provider is None and model is None:
        return "(no-plan)"
    provider_text = provider or "unknown"
    model_text = model or "unknown"
    return f"{provider_text}/{model_text}"


def _round_stat(value: float) -> int | float:
    rounded = round(value, 1)
    if float(rounded).is_integer():
        return int(rounded)
    return rounded


def _score_stats(scores: list[int]) -> dict[str, int | float]:
    if not scores:
        return {"n": 0, "mean": 0.0, "median": 0.0, "p10": 0, "p90": 0, "min": 0, "max": 0}
    ordered = sorted(scores)
    return {
        "n": len(ordered),
        "mean": round(sum(ordered) / len(ordered), 1),
        "median": _round_stat(float(median(ordered))),
        "p10": _percentile(ordered, 10),
        "p90": _percentile(ordered, 90),
        "min": ordered[0],
        "max": ordered[-1],
    }


def _group_score_rows(
    grouped: dict[tuple[Any, ...], list[int]],
    key_names: list[str],
    *,
    min_samples: int = 1,
    sort_mode: str = "count_desc",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, values in grouped.items():
        if len(values) < min_samples:
            continue
        ordered = sorted(values)
        row: dict[str, Any] = {name: part for name, part in zip(key_names, key)}
        row["n"] = len(ordered)
        row["mean"] = round(sum(ordered) / len(ordered), 1)
        row["median"] = _round_stat(float(median(ordered)))
        rows.append(row)
    if sort_mode == "mean_desc":
        rows.sort(key=lambda row: (row["mean"], row["n"]), reverse=True)
    else:
        rows.sort(key=lambda row: (row["n"], row["mean"]), reverse=True)
    return rows


def _build_score_analytics(records: list[_ScoreRecord], pipeline_min_samples: int = 3) -> dict[str, Any]:
    overall = _score_stats([r.score for r in records])

    by_reviewer: dict[tuple[str | None, str | None], list[int]] = defaultdict(list)
    by_implementer: dict[tuple[str | None, str | None], list[int]] = defaultdict(list)
    by_pipeline: dict[
        tuple[str | None, str | None, str | None, str | None, str | None, str | None],
        list[int],
    ] = defaultdict(list)
    by_week: dict[str, list[int]] = defaultdict(list)

    for record in records:
        by_reviewer[(record.reviewer_provider, record.reviewer_model)].append(record.score)
        by_implementer[(record.implementer_provider, record.implementer_model)].append(record.score)
        by_pipeline[
            (
                record.planner_provider,
                record.planner_model,
                record.implementer_provider,
                record.implementer_model,
                record.reviewer_provider,
                record.reviewer_model,
            )
        ].append(record.score)
        by_week[record.week_iso].append(record.score)

    weekly_rows: list[dict[str, Any]] = []
    for week in sorted(by_week.keys())[-8:]:
        week_scores = sorted(by_week[week])
        weekly_rows.append(
            {
                "week": week,
                "n": len(week_scores),
                "mean": round(sum(week_scores) / len(week_scores), 1),
            }
        )

    return {
        "overall": overall,
        "by_reviewer": _group_score_rows(
            grouped=by_reviewer,
            key_names=["provider", "model"],
            min_samples=1,
            sort_mode="count_desc",
        ),
        "by_implementer": _group_score_rows(
            grouped=by_implementer,
            key_names=["provider", "model"],
            min_samples=1,
            sort_mode="count_desc",
        ),
        "by_pipeline": _group_score_rows(
            grouped=by_pipeline,
            key_names=[
                "planner_provider",
                "planner_model",
                "implementer_provider",
                "implementer_model",
                "reviewer_provider",
                "reviewer_model",
            ],
            min_samples=pipeline_min_samples,
            sort_mode="mean_desc",
        ),
        "weekly_trend": weekly_rows,
    }


def _resolve_init_db_path(project_dir: Path, db_path_value: str) -> Path:
    resolved = Path(os.path.expanduser(db_path_value))
    if not resolved.is_absolute():
        resolved = project_dir / resolved
    return resolved.resolve()


def _init_has_global_shared_db_default(project_dir: Path) -> tuple[bool, Path | None]:
    from ..config import DEFAULT_DB_FILE

    user_data, _, _ = Config._load_user_config_data()
    db_path_value = user_data.get("db_path")
    if not isinstance(db_path_value, str) or not db_path_value:
        return False, None

    local_db_path = (project_dir / DEFAULT_DB_FILE).resolve()
    resolved_db_path = _resolve_init_db_path(project_dir, db_path_value)
    if resolved_db_path == local_db_path:
        return False, None
    return True, resolved_db_path


def _normalize_init_db_args(
    args: argparse.Namespace, *, is_interactive: bool
) -> tuple[str | None, str | None, bool] | None:
    """Normalize `gza init` DB flags and emit early CLI-only validation errors."""
    if args.db_path and args.db == "local":
        print("Error: --db-path cannot be used with --db local.", file=sys.stderr)
        return None

    db_mode = args.db
    shared_db_path = args.db_path
    db_choice_from_flags = args.db is not None or args.db_path is not None
    if shared_db_path and db_mode is None:
        db_mode = "shared"

    if not is_interactive and db_mode is None:
        print(
            "Error: --db is required when running gza init non-interactively.\n"
            "Pass --db local (project-local .gza/gza.db) or --db shared [--db-path PATH].",
            file=sys.stderr,
        )
        return None

    return db_mode, shared_db_path, db_choice_from_flags


def _count_section_items(content: str, header_pattern: str) -> int:
    """Count top-level list items under a section matching header_pattern."""
    lines = content.split("\n")
    i = 0
    count = 0
    while i < len(lines):
        if re.match(header_pattern, lines[i].strip(), re.IGNORECASE):
            i += 1
            in_numbered = False
            while i < len(lines):
                line = lines[i]
                stripped = line.strip()
                if stripped == "":
                    i += 1
                    continue
                if re.match(r"^\d+\.\s", stripped):
                    count += 1
                    in_numbered = True
                    i += 1
                elif line.startswith("- "):
                    if in_numbered:
                        i += 1
                    else:
                        item_text = stripped[2:].strip().rstrip(".")
                        if item_text.lower() != "none":
                            count += 1
                        i += 1
                elif line.startswith("  ") or line.startswith("\t"):
                    i += 1
                else:
                    break
        else:
            i += 1
    return count


def _count_review_issues(content: str) -> tuple[int, int]:
    """Parse review markdown and return (blocker_count, followup_count)."""
    if not content:
        return 0, 0
    blockers = len(re.findall(r"^###\s+(?:B\d+[\.\s\u2014\u2013-]|M?\d+[\.\s\u2014\u2013-]|Issue\s+\d+)", content, re.MULTILINE))
    followups = len(re.findall(r"^###\s+(?:F\d+[\.\s\u2014\u2013-]|S\d+[\.\s\u2014\u2013-])", content, re.MULTILINE))
    if blockers == 0:
        blockers = _count_section_items(content, r"^(?:#+\s*)?(?:blockers?|must[- ]?fix(?:\s+issues?)?)$")
    if followups == 0:
        followups = _count_section_items(content, r"^(?:#+\s*)?(?:follow[- ]?ups?|suggestions?)$")
    return blockers, followups


def _cmd_stats_reviews(
    config: "Config",
    store: "SqliteTaskStore",
    start_date: date,
    end_date: date,
    show_issues: bool,
    all_time: bool = False,
    output_json: bool = False,
) -> int:
    """Show review count stats per implementation task."""
    all_tasks = store.get_all()
    tasks_by_id = {t.id: t for t in all_tasks if t.id is not None}

    start_dt = datetime(start_date.year, start_date.month, start_date.day)
    end_dt = datetime(end_date.year, end_date.month, end_date.day) + timedelta(days=1)

    def _task_dt(t: Task) -> datetime | None:
        if t.created_at is None:
            return None
        if t.created_at.tzinfo is not None:
            return t.created_at.astimezone().replace(tzinfo=None)
        return t.created_at

    def find_root_impl(task_id: str, visited: set[str] | None = None) -> str | None:
        """Walk up based_on/depends_on chains to find the root implement task."""
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)
        task = tasks_by_id.get(task_id)
        if task is None:
            return None
        for parent_id in (task.based_on, task.depends_on):
            if parent_id:
                parent = tasks_by_id.get(parent_id)
                if parent and parent.task_type == "implement":
                    root = find_root_impl(parent_id, visited)
                    return root if root is not None else parent_id
        if task.task_type == "implement":
            return task_id
        return None

    def week_label(dt: datetime) -> str:
        monday = dt.date() - timedelta(days=dt.weekday())
        sunday = monday + timedelta(days=6)
        return f"{monday.strftime('%b %d')} - {sunday.strftime('%b %d')}"

    def week_sort_key(label: str) -> date:
        today = date.today()
        parts = label.split(" - ")
        return datetime.strptime(parts[0] + f" {today.year}", "%b %d %Y").date()

    def week_iso_label(dt: datetime) -> str:
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"

    def find_plan_task(task_id: str, visited: set[str] | None = None) -> Task | None:
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)
        task = tasks_by_id.get(task_id)
        if task is None:
            return None
        for parent_id in (task.depends_on, task.based_on):
            if not parent_id:
                continue
            parent = tasks_by_id.get(parent_id)
            if parent is None:
                continue
            if parent.task_type == "plan":
                return parent
            plan = find_plan_task(parent_id, visited)
            if plan is not None:
                return plan
        return None

    # Find review/improve tasks in date range that have a parent link
    ri_tasks = [
        t for t in all_tasks
        if t.task_type in ("review", "improve")
        and t.status == "completed"
        and (t.based_on is not None or t.depends_on is not None)
        and _task_dt(t) is not None
        and start_dt <= _task_dt(t) < end_dt  # type: ignore
    ]

    # Count reviews per root impl, tracking models
    root_reviews: dict[str, list[datetime]] = defaultdict(list)
    root_review_models: dict[str, list[str | None]] = defaultdict(list)
    root_review_tasks: dict[str, list[Task]] = defaultdict(list)
    for ri in ri_tasks:
        parent_id = ri.depends_on or ri.based_on
        if parent_id is None:
            continue
        root = find_root_impl(parent_id)
        if root is None:
            continue
        if ri.task_type == "review":
            dt = _task_dt(ri)
            if dt:
                root_reviews[root].append(dt)
                root_review_models[root].append(ri.model)
                root_review_tasks[root].append(ri)

    # Find all root implement tasks in range (only count completed ones —
    # pending/failed impls can't meaningfully be "reviewed or not")
    root_impls_in_range: list[Task] = []
    seen_roots: set[str] = set()
    for t in all_tasks:
        if t.task_type not in ("implement", "improve"):
            continue
        dt = _task_dt(t)
        if dt is None or dt < start_dt or dt >= end_dt:
            continue
        root = find_root_impl(t.id)  # type: ignore
        if root is not None and root not in seen_roots:
            root_impl = tasks_by_id.get(root)
            if root_impl is not None and root_impl.status == "completed":
                seen_roots.add(root)
                root_impls_in_range.append(root_impl)

    total_impls = len(root_impls_in_range)
    total_improves = 0
    for t in all_tasks:
        if t.task_type != "improve" or t.status != "completed":
            continue
        dt = _task_dt(t)
        if dt is None or dt < start_dt or dt >= end_dt:
            continue
        total_improves += 1
    total_reviews = sum(
        len(dates) for root_id, dates in root_reviews.items() if root_id in seen_roots
    )
    reviewed_impls = {r for r in root_reviews if r in seen_roots}
    review_pct = (len(reviewed_impls) / total_impls * 100) if total_impls else 0

    # Group by week
    week_data: dict[str, dict] = defaultdict(
        lambda: {
            "impls": 0,
            "reviews": 0,
            "reviewed_cycles": [],
            "score_values": [],
        }
    )
    for impl in root_impls_in_range:
        dt = _task_dt(impl)
        if dt:
            week_data[week_label(dt)]["impls"] += 1
    for root_id, review_dates in root_reviews.items():
        if root_id not in seen_roots:
            continue
        root_impl = tasks_by_id.get(root_id)
        if root_impl is None:
            continue
        dt = _task_dt(root_impl)
        if dt is None:
            continue
        wk = week_label(dt)
        week_data[wk]["reviews"] += len(review_dates)
        week_data[wk]["reviewed_cycles"].append(len(review_dates))

    plan_task_by_root: dict[str, Task | None] = {}
    scored_records: list[_ScoreRecord] = []
    for root_id, reviews in root_review_tasks.items():
        if root_id not in seen_roots:
            continue
        root_impl = tasks_by_id.get(root_id)
        if root_impl is None:
            continue
        impl_dt = _task_dt(root_impl)
        if impl_dt is None:
            continue
        planner = plan_task_by_root.setdefault(root_id, find_plan_task(root_id))
        week_lbl = week_label(impl_dt)
        week_iso = week_iso_label(impl_dt)
        for review_task in reviews:
            if review_task.review_score is None:
                continue
            score = int(review_task.review_score)
            week_data[week_lbl]["score_values"].append(score)
            scored_records.append(
                _ScoreRecord(
                    week_label=week_lbl,
                    week_iso=week_iso,
                    score=score,
                    reviewer_provider=review_task.provider,
                    reviewer_model=review_task.model,
                    implementer_provider=root_impl.provider,
                    implementer_model=root_impl.model,
                    planner_provider=planner.provider if planner else None,
                    planner_model=planner.model if planner else None,
                )
            )

    sorted_weeks = sorted(week_data.keys(), key=week_sort_key)
    score_analytics = _build_score_analytics(scored_records)

    all_reviewed_cycles: list[int] = []
    all_scores: list[int] = []
    total_row_impls = 0
    total_row_reviews = 0
    weekly_rows: list[dict[str, Any]] = []

    for wk in sorted_weeks:
        d = week_data[wk]
        total_row_impls += d["impls"]
        total_row_reviews += d["reviews"]
        cycles = sorted(d["reviewed_cycles"])
        all_reviewed_cycles.extend(cycles)
        scores = sorted(d["score_values"])
        all_scores.extend(scores)
        rv_pct = (len(cycles) / d["impls"] * 100) if d["impls"] else 0
        row: dict[str, Any] = {
            "week": wk,
            "impls": d["impls"],
            "reviews": d["reviews"],
            "review_pct": round(rv_pct, 1),
            "reviewed_impls": len(cycles),
            "score_n": len(scores),
            "score_mean": round(sum(scores) / len(scores), 1) if scores else None,
        }
        if cycles:
            row["review_cycles_median"] = int(median(cycles))
            row["review_cycles_p90"] = _percentile(cycles, 90)
            row["review_cycles_max"] = max(cycles)
        else:
            row["review_cycles_median"] = None
            row["review_cycles_p90"] = None
            row["review_cycles_max"] = None
        weekly_rows.append(row)

    total_week_row: dict[str, Any] | None = None

    if len(sorted_weeks) > 1:
        all_reviewed_cycles.sort()
        rv_pct = (len(all_reviewed_cycles) / total_row_impls * 100) if total_row_impls else 0
        total_week_row = {
            "week": "Total",
            "impls": total_row_impls,
            "reviews": total_row_reviews,
            "review_pct": round(rv_pct, 1),
            "reviewed_impls": len(all_reviewed_cycles),
            "score_n": len(all_scores),
            "score_mean": round(sum(all_scores) / len(all_scores), 1) if all_scores else None,
        }
        if all_reviewed_cycles:
            total_week_row["review_cycles_median"] = int(median(all_reviewed_cycles))
            total_week_row["review_cycles_p90"] = _percentile(all_reviewed_cycles, 90)
            total_week_row["review_cycles_max"] = max(all_reviewed_cycles)
        else:
            total_week_row["review_cycles_median"] = None
            total_week_row["review_cycles_p90"] = None
            total_week_row["review_cycles_max"] = None

    # Review count distribution
    dist_rows: list[dict[str, Any]] = []
    if all_reviewed_cycles:
        dist = Counter(all_reviewed_cycles)
        total = len(all_reviewed_cycles)
        for cnt in sorted(dist.keys()):
            pct = dist[cnt] / total * 100
            dist_rows.append(
                {
                    "reviews_per_impl": cnt,
                    "impl_count": dist[cnt],
                    "pct": round(pct, 1),
                }
            )

    # Per-model review iteration stats
    model_cycles: dict[str, list[int]] = defaultdict(list)
    for root_id in reviewed_impls:
        models = root_review_models.get(root_id, [])
        cycle_count = len(root_reviews[root_id])
        model_counts = Counter(m for m in models if m)
        model = model_counts.most_common(1)[0][0] if model_counts else "unknown"
        model_cycles[model].append(cycle_count)

    def _cycle_stats_str(vals: list[int]) -> str:
        return f"{int(median(vals))}/{_percentile(vals, 75)}/{_percentile(vals, 90)}/{max(vals)}"

    def _model_breakdown_table(
        title: str,
        model_label: str,
        data: dict[str, list[int]],
    ) -> Table:
        total = sum(len(v) for v in data.values())
        table = Table(title=title, title_justify="left", title_style="bold")
        table.add_column(model_label)
        table.add_column("Impls", justify="right")
        table.add_column("Pct", justify="right")
        table.add_column("med/p75/p90/max", justify="right")
        for model in sorted(data):
            cycles_sorted = sorted(data[model])
            n = len(cycles_sorted)
            pct = (n / total * 100) if total else 0
            table.add_row(
                model,
                str(n),
                f"{pct:.0f}%",
                _cycle_stats_str(cycles_sorted),
            )
        return table

    cycle_by_reviewer_model_rows: list[dict[str, Any]] = []
    if model_cycles:
        total = sum(len(v) for v in model_cycles.values())
        for model in sorted(model_cycles):
            cycles_sorted = sorted(model_cycles[model])
            n = len(cycles_sorted)
            cycle_by_reviewer_model_rows.append(
                {
                    "model": model,
                    "n": n,
                    "pct": round((n / total * 100) if total else 0, 1),
                    "median": int(median(cycles_sorted)),
                    "p75": _percentile(cycles_sorted, 75),
                    "p90": _percentile(cycles_sorted, 90),
                    "max": max(cycles_sorted),
                }
            )

    # Per-implementer-model review iteration stats
    impl_model_cycles: dict[str, list[int]] = defaultdict(list)
    for root_id in reviewed_impls:
        root_impl = tasks_by_id.get(root_id)
        if root_impl is None:
            continue
        impl_model = root_impl.model or "unknown"
        impl_model_cycles[impl_model].append(len(root_reviews[root_id]))

    cycle_by_implement_model_rows: list[dict[str, Any]] = []
    if impl_model_cycles:
        total = sum(len(v) for v in impl_model_cycles.values())
        for model in sorted(impl_model_cycles):
            cycles_sorted = sorted(impl_model_cycles[model])
            n = len(cycles_sorted)
            cycle_by_implement_model_rows.append(
                {
                    "model": model,
                    "n": n,
                    "pct": round((n / total * 100) if total else 0, 1),
                    "median": int(median(cycles_sorted)),
                    "p75": _percentile(cycles_sorted, 75),
                    "p90": _percentile(cycles_sorted, 90),
                    "max": max(cycles_sorted),
                }
            )

    # Per-pair (implement model × review model) iteration stats
    pair_cycles: dict[tuple[str, str], list[int]] = defaultdict(list)
    for root_id in reviewed_impls:
        root_impl = tasks_by_id.get(root_id)
        if root_impl is None:
            continue
        impl_model = root_impl.model or "unknown"
        rv_models = root_review_models.get(root_id, [])
        rv_counts = Counter(m for m in rv_models if m)
        rv_model = rv_counts.most_common(1)[0][0] if rv_counts else "unknown"
        pair_cycles[(impl_model, rv_model)].append(len(root_reviews[root_id]))

    cycle_by_pair_rows: list[dict[str, Any]] = []
    if pair_cycles:
        total_pairs = sum(len(v) for v in pair_cycles.values())
        overall_median = median(all_reviewed_cycles) if all_reviewed_cycles else 0
        sorted_pairs = sorted(
            pair_cycles.items(),
            key=lambda kv: (median(kv[1]), -len(kv[1])),
        )
        for (impl_m, rv_m), cycles in sorted_pairs:
            cycles_sorted = sorted(cycles)
            n = len(cycles_sorted)
            pct = (n / total_pairs * 100) if total_pairs else 0
            delta = median(cycles_sorted) - overall_median
            cycle_by_pair_rows.append(
                {
                    "implement_model": impl_m,
                    "review_model": rv_m,
                    "n": n,
                    "pct": round(pct, 1),
                    "median": _round_stat(float(median(cycles_sorted))),
                    "p75": _percentile(cycles_sorted, 75),
                    "p90": _percentile(cycles_sorted, 90),
                    "max": max(cycles_sorted),
                    "delta_vs_overall_median": round(delta, 1),
                }
            )

    # Per-model issue counts (--issues mode)
    issue_rows: list[dict[str, Any]] = []
    unparsed_review_ids: list[str] = []
    issue_totals_blockers: list[int] = []
    issue_totals_followups: list[int] = []
    parsed_review_count = 0
    if show_issues:
        review_content = {
            t.id: t.output_content
            for t in all_tasks
            if t.task_type == "review"
            and t.output_content is not None
            and t.id is not None
            and _task_dt(t) is not None
            and start_dt <= _task_dt(t) < end_dt  # type: ignore
        }
        parsed_review_count = len(review_content)
        model_issues: dict[str, list[tuple[int, int]]] = defaultdict(list)
        for ri in ri_tasks:
            if ri.task_type != "review":
                continue
            if ri.id is None:
                continue
            content = review_content.get(ri.id)
            if content is None:
                continue
            must_fix, sugg = _count_review_issues(content)
            if must_fix == 0 and sugg == 0:
                unparsed_review_ids.append(ri.id or "")
            model = ri.model or "unknown"
            model_issues[model].append((must_fix, sugg))
        for model in sorted(model_issues):
            pairs = model_issues[model]
            fixes = sorted(mf for mf, _ in pairs)
            suggs = sorted(sg for _, sg in pairs)
            issue_totals_blockers.extend(fixes)
            issue_totals_followups.extend(suggs)
            issue_rows.append(
                {
                    "model": model,
                    "reviews": len(pairs),
                    "blockers": {
                        "median": int(median(fixes)),
                        "p75": _percentile(fixes, 75),
                        "p90": _percentile(fixes, 90),
                        "max": max(fixes),
                    },
                    "follow_ups": {
                        "median": int(median(suggs)),
                        "p75": _percentile(suggs, 75),
                        "p90": _percentile(suggs, 90),
                        "max": max(suggs),
                    },
                }
            )

    if output_json:
        coverage_payload: dict[str, Any] = {
            "range": {
                "all_time": all_time,
                "start_date": str(start_date),
                "end_date": str(end_date),
            },
            "summary": {
                "implement_tasks": total_impls,
                "improve_tasks": total_improves,
                "review_tasks": total_reviews,
                "reviewed_impls": len(reviewed_impls),
                "review_pct": round(review_pct, 1),
            },
            "weekly": weekly_rows,
            "reviews_per_implementation_distribution": dist_rows,
            "review_cycles_by_review_model": cycle_by_reviewer_model_rows,
            "review_cycles_by_implement_model": cycle_by_implement_model_rows,
            "review_cycles_by_pair": cycle_by_pair_rows,
        }
        if total_week_row is not None:
            coverage_payload["weekly_total"] = total_week_row
        if show_issues:
            coverage_payload["issues"] = {
                "rows": issue_rows,
                "unparsed_review_ids": [rid for rid in unparsed_review_ids if rid],
            }
        payload = {
            "coverage": coverage_payload,
            "scores": score_analytics,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    header_range = "all time" if all_time else f"{start_date} to {end_date}"
    print(f"\nReview stats ({header_range})")
    print(f"Implement tasks: {total_impls}")
    print(f"Improve tasks:   {total_improves}")
    print(f"Review tasks:    {total_reviews}")
    print(f"Reviewed:        {len(reviewed_impls)}/{total_impls} ({review_pct:.0f}%)")

    print(
        f"\n{'Week':<22} {'Impls':>5} {'Rvws':>5} {'Rv%':>5} {'Med':>5} {'P90':>5} {'Max':>5} {'ScN':>5} {'Mean':>7}"
    )
    print("-" * 70)
    for row in weekly_rows:
        med_val = row["review_cycles_median"] if row["review_cycles_median"] is not None else "-"
        p90_val = row["review_cycles_p90"] if row["review_cycles_p90"] is not None else "-"
        max_val = row["review_cycles_max"] if row["review_cycles_max"] is not None else "-"
        score_n = row["score_n"] if int(row["score_n"]) > 0 else "-"
        score_mean = f"{row['score_mean']:.1f}" if row["score_mean"] is not None else "-"
        print(
            f"{row['week']:<22} {int(row['impls']):>5} {int(row['reviews']):>5} "
            f"{float(row['review_pct']):>4.0f}% {med_val:>5} {p90_val:>5} {max_val:>5} "
            f"{score_n:>5} {score_mean:>7}"
        )
    if total_week_row is not None:
        print("-" * 70)
        med_val = total_week_row["review_cycles_median"] if total_week_row["review_cycles_median"] is not None else "-"
        p90_val = total_week_row["review_cycles_p90"] if total_week_row["review_cycles_p90"] is not None else "-"
        max_val = total_week_row["review_cycles_max"] if total_week_row["review_cycles_max"] is not None else "-"
        score_n = total_week_row["score_n"] if int(total_week_row["score_n"]) > 0 else "-"
        score_mean = f"{total_week_row['score_mean']:.1f}" if total_week_row["score_mean"] is not None else "-"
        print(
            f"{'Total':<22} {int(total_week_row['impls']):>5} {int(total_week_row['reviews']):>5} "
            f"{float(total_week_row['review_pct']):>4.0f}% {med_val:>5} {p90_val:>5} {max_val:>5} "
            f"{score_n:>5} {score_mean:>7}"
        )

    if dist_rows:
        print("\nReviews per implementation (reviewed tasks only):")
        for row in dist_rows:
            cnt = int(row["reviews_per_impl"])
            impl_count = int(row["impl_count"])
            pct = float(row["pct"])
            bar = "#" * impl_count
            label = "review" if cnt == 1 else "reviews"
            left = f"{cnt} {label}:"
            print(f"  {left:<12}{impl_count:>3} ({pct:3.0f}%)  {bar}")

    if model_cycles:
        console.print()
        console.print(_model_breakdown_table("Review model", "Review model", model_cycles))

    if impl_model_cycles:
        console.print()
        console.print(
            _model_breakdown_table("Implement model", "Implement model", impl_model_cycles)
        )

    if pair_cycles:
        pair_table = Table(
            title="Implement → review pairs",
            title_justify="left",
            title_style="bold",
        )
        pair_table.add_column("Implement model")
        pair_table.add_column("Review model")
        pair_table.add_column("Impls", justify="right")
        pair_table.add_column("Pct", justify="right")
        pair_table.add_column("med/p75/p90/max", justify="right")
        pair_table.add_column("vs med", justify="right")
        for row in cycle_by_pair_rows:
            delta = float(row["delta_vs_overall_median"])
            delta_str = f"{delta:+.1f}" if delta else "0"
            pair_table.add_row(
                str(row["implement_model"]),
                str(row["review_model"]),
                str(row["n"]),
                f"{float(row['pct']):.0f}%",
                f"{row['median']}/{row['p75']}/{row['p90']}/{row['max']}",
                delta_str,
            )
        console.print()
        console.print(pair_table)

    overall = score_analytics["overall"]
    rc = _colors.RUNNER_COLORS
    console.print()
    console.print(f"[{rc.label}]Score stats (scored reviews only):[/{rc.label}]")
    if int(overall["n"]) == 0:
        console.print(f"  [{rc.value}]No scored reviews in range.[/{rc.value}]")
    else:
        console.print(
            f"  [{rc.label}]Overall:[/{rc.label}] "
            f"[{rc.value}]n={overall['n']}  mean={overall['mean']}  median={overall['median']}  "
            f"p10={overall['p10']}  p90={overall['p90']}  min={overall['min']}  max={overall['max']}[/{rc.value}]"
        )

    reviewer_rows = score_analytics["by_reviewer"]
    if len(reviewer_rows) > 1:
        reviewer_table = Table(title="Score by reviewer provider/model", title_justify="left", title_style="bold")
        reviewer_table.add_column("Provider")
        reviewer_table.add_column("Model")
        reviewer_table.add_column("N", justify="right")
        reviewer_table.add_column("Mean", justify="right")
        reviewer_table.add_column("Median", justify="right")
        for row in reviewer_rows:
            reviewer_table.add_row(
                str(row["provider"] or "unknown"),
                str(row["model"] or "unknown"),
                str(row["n"]),
                f"{float(row['mean']):.1f}",
                str(row["median"]),
            )
        console.print()
        console.print(reviewer_table)

    implementer_rows = score_analytics["by_implementer"]
    if len(implementer_rows) > 1:
        implementer_table = Table(title="Score by implementer provider/model", title_justify="left", title_style="bold")
        implementer_table.add_column("Provider")
        implementer_table.add_column("Model")
        implementer_table.add_column("N", justify="right")
        implementer_table.add_column("Mean", justify="right")
        implementer_table.add_column("Median", justify="right")
        for row in implementer_rows:
            implementer_table.add_row(
                str(row["provider"] or "unknown"),
                str(row["model"] or "unknown"),
                str(row["n"]),
                f"{float(row['mean']):.1f}",
                str(row["median"]),
            )
        console.print()
        console.print(implementer_table)

    pipeline_rows = score_analytics["by_pipeline"]
    if pipeline_rows:
        pipeline_table = Table(title="Score by pipeline", title_justify="left", title_style="bold")
        pipeline_table.add_column("Planner")
        pipeline_table.add_column("Implementer")
        pipeline_table.add_column("Reviewer")
        pipeline_table.add_column("N", justify="right")
        pipeline_table.add_column("Mean", justify="right")
        pipeline_table.add_column("Median", justify="right")
        for row in pipeline_rows:
            pipeline_table.add_row(
                _display_provider_model(
                    row["planner_provider"], row["planner_model"], no_plan=True
                ),
                _display_provider_model(
                    row["implementer_provider"], row["implementer_model"]
                ),
                _display_provider_model(
                    row["reviewer_provider"], row["reviewer_model"]
                ),
                str(row["n"]),
                f"{float(row['mean']):.1f}",
                str(row["median"]),
            )
        console.print()
        console.print(pipeline_table)

    weekly_trend = score_analytics["weekly_trend"]
    if weekly_trend:
        console.print()
        console.print(f"[{rc.label}]Score trend (last 8 weeks):[/{rc.label}]")
        for row in weekly_trend:
            console.print(
                f"  [{rc.label}]{row['week']}:[/{rc.label}] "
                f"[{rc.value}]n={int(row['n'])}  mean={float(row['mean']):.1f}[/{rc.value}]"
            )

    if show_issues:
        print(f"\nParsing issue counts from {parsed_review_count} review(s)...")
        if unparsed_review_ids:
            ids = ", ".join(unparsed_review_ids)
            print(
                f"\nwarning: could not parse issues from {len(unparsed_review_ids)} review(s): {ids}",
                file=sys.stderr,
            )
        if issue_rows:
            def _stats_str_from_row(metric: dict[str, int]) -> str:
                return (
                    f"{metric['median']}/{metric['p75']}/{metric['p90']}/{metric['max']}"
                )

            print("\nIssue counts per review (parsed from markdown)")
            print(f"{'Review model':<35} {'Rvws':>5}  {'Blockers':>16}  {'Follow-ups':>16}")
            print(f"{'':35} {'':>5}  {'med/p75/p90/max':>16}  {'med/p75/p90/max':>16}")
            print("-" * 77)
            for row in issue_rows:
                model = str(row["model"])
                n = int(row["reviews"])
                blockers_metric = row["blockers"]
                follow_ups_metric = row["follow_ups"]
                if not isinstance(blockers_metric, dict) or not isinstance(follow_ups_metric, dict):
                    continue
                blockers_metric_int = {
                    "median": int(blockers_metric.get("median", 0)),
                    "p75": int(blockers_metric.get("p75", 0)),
                    "p90": int(blockers_metric.get("p90", 0)),
                    "max": int(blockers_metric.get("max", 0)),
                }
                follow_ups_metric_int = {
                    "median": int(follow_ups_metric.get("median", 0)),
                    "p75": int(follow_ups_metric.get("p75", 0)),
                    "p90": int(follow_ups_metric.get("p90", 0)),
                    "max": int(follow_ups_metric.get("max", 0)),
                }
                print(
                    f"{model:<35} {n:>5}  "
                    f"{_stats_str_from_row(blockers_metric_int):>16}  "
                    f"{_stats_str_from_row(follow_ups_metric_int):>16}"
                )
            if len(issue_rows) > 1 and issue_totals_blockers and issue_totals_followups:
                issue_totals_blockers.sort()
                issue_totals_followups.sort()
                print("-" * 77)
                print(
                    f"{'Total':<35} {sum(int(r['reviews']) for r in issue_rows):>5}  "
                    f"{int(median(issue_totals_blockers))}/{_percentile(issue_totals_blockers, 75)}/{_percentile(issue_totals_blockers, 90)}/{max(issue_totals_blockers):>16}  "
                    f"{int(median(issue_totals_followups))}/{_percentile(issue_totals_followups, 75)}/{_percentile(issue_totals_followups, 90)}/{max(issue_totals_followups):>16}"
                )

    return 0


def _cmd_stats_iterations(
    config: "Config",
    store: "SqliteTaskStore",
    start_dt: datetime | None,
    end_dt: datetime | None,
    header_range: str,
    last_n: int | None = None,
) -> int:
    """Show per-implementation review/improve iteration rollups."""

    def _normalize_dt(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone().replace(tzinfo=None)
        return dt

    def _task_dt(task: Task) -> datetime | None:
        return _normalize_dt(task.created_at)

    def _activity_dt(task: Task) -> datetime | None:
        # For operational windowing, completed work should be attributed by
        # completion time; incomplete rows fall back to creation time.
        if task.status == "completed":
            completed_dt = _normalize_dt(task.completed_at)
            if completed_dt is not None:
                return completed_dt
        return _task_dt(task)

    def _in_window(dt: datetime | None) -> bool:
        if dt is None:
            return False
        if start_dt is not None and dt < start_dt:
            return False
        if end_dt is not None and dt >= end_dt:
            return False
        return True

    def _cost(task: Task) -> float:
        return task.cost_usd or 0.0

    def _latest_activity_dt(tasks: list[Task]) -> datetime | None:
        activity_dts = [_activity_dt(task) for task in tasks]
        non_null_dts = [dt for dt in activity_dts if dt is not None]
        return max(non_null_dts) if non_null_dts else None

    def _task_label(task: Task) -> str:
        slug_display: str | None = None
        if task.slug:
            slug_display = get_slug_display_text(task.slug, config.project_prefix)
        elif task.prompt:
            slug_display = re.sub(r"[^a-z0-9]+", "-", task.prompt.lower()).strip("-")
        return f"{task.id}  {slug_display}" if slug_display else (task.id or "(unknown)")

    all_tasks = store.get_all()
    impl_tasks = [t for t in all_tasks if t.task_type == "implement" and t.id is not None]
    impl_tasks.sort(
        key=lambda task: (
            _task_dt(task) or datetime.min,
            task_id_numeric_key(task.id),
        ),
        reverse=True,
    )

    excluded_order = ("FAILED", "IN_PROGRESS", "NO_REVIEW")
    excluded_counts: Counter[str] = Counter()
    excluded_cost: float = 0.0

    rows: list[tuple[str, str, int, str, float, int]] = []
    for impl in impl_tasks:
        assert impl.id is not None
        if impl.status == "pending":
            continue
        reviews = store.get_reviews_for_task(impl.id)
        improves = store.get_improve_tasks_by_root(impl.id)

        if not (
            _in_window(_activity_dt(impl))
            or any(_in_window(_activity_dt(review)) for review in reviews)
            or any(_in_window(_activity_dt(improve)) for improve in improves)
        ):
            continue

        completed_reviews = [review for review in reviews if review.status == "completed"]
        completed_improves = [improve for improve in improves if improve.status == "completed"]

        latest_completed_review = next(
            (review for review in reviews if review.status == "completed"),
            None,
        )
        verdict: str = "NO_REVIEW"
        if latest_completed_review is not None:
            verdict = get_review_verdict(config, latest_completed_review) or "UNKNOWN"
        elif impl.status == "failed":
            verdict = "FAILED"
        elif impl.status in ("in_progress", "queued"):
            verdict = "IN_PROGRESS"

        total_cost = (
            _cost(impl)
            + sum(_cost(review) for review in reviews)
            + sum(_cost(improve) for improve in improves)
        )

        if verdict in excluded_order:
            excluded_counts[verdict] += 1
            excluded_cost += total_cost
            continue

        iterations_count = len(completed_reviews)
        run_dt = _latest_activity_dt([impl, *reviews, *improves])
        run_date = run_dt.strftime("%Y-%m-%d") if run_dt is not None else "-"

        rows.append(
            (
                _task_label(impl),
                run_date,
                iterations_count,
                verdict,
                total_cost,
                len(completed_improves),
            )
        )

    if last_n is not None:
        rows = rows[:last_n]

    print(f"\n{header_range}\n")
    task_col_width = 46
    verdict_col_width = 17
    print(
        f"{'Task':<{task_col_width}} {'Last Run Date':<13}  "
        f"{'Iterations':>10}  "
        f"{'Verdict':<{verdict_col_width}} {'Cost':>8}"
    )
    if rows:
        for task_label, run_date, iterations_count, verdict, task_cost, _ in rows:
            if len(task_label) > task_col_width:
                task_label = f"{task_label[:task_col_width - 3]}..."
            print(
                f"{task_label:<{task_col_width}} "
                f"{run_date:<13}  "
                f"{iterations_count:>10}  "
                f"{verdict:<{verdict_col_width}} "
                f"${task_cost:>7.2f}"
            )

    total_tasks = len(rows)
    total_iterations = sum(iterations_count for _, _, iterations_count, _, _, _ in rows)
    total_improves = sum(improves_count for _, _, _, _, _, improves_count in rows)
    approved = sum(1 for r in rows if r[3] == "APPROVED")
    total_cost = sum(task_cost for _, _, _, _, task_cost, _ in rows)
    parts = [
        f"{total_tasks} tasks",
        f"{total_iterations} iterations",
        f"{total_improves} improves",
        f"{approved}/{total_tasks} approved",
        f"${total_cost:.2f} total",
    ]
    print("\n" + "  |  ".join(parts))

    if excluded_counts:
        label_map = {"FAILED": "failed", "IN_PROGRESS": "in-progress", "NO_REVIEW": "no-review"}
        breakdown = ", ".join(
            f"{excluded_counts[v]} {label_map[v]}"
            for v in excluded_order
            if excluded_counts[v]
        )
        print(f"Excluded: {breakdown}  |  ${excluded_cost:.2f}")

    iteration_counts = sorted(iterations_count for _, _, iterations_count, _, _, _ in rows)
    if iteration_counts:
        summary_parts = [
            f"min {min(iteration_counts)}",
            f"p10 {_percentile(iteration_counts, 10)}",
            f"p25 {_percentile(iteration_counts, 25)}",
            f"p50 {_percentile(iteration_counts, 50)}",
            f"p75 {_percentile(iteration_counts, 75)}",
            f"p90 {_percentile(iteration_counts, 90)}",
            f"p99 {_percentile(iteration_counts, 99)}",
            f"max {max(iteration_counts)}",
        ]
        print("Iteration count stats: " + "  |  ".join(summary_parts))

    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show stats analytics subcommands for reviews and iterations."""
    stats_subcommand: str | None = getattr(args, 'stats_subcommand', None)

    if stats_subcommand is None:
        parser = getattr(args, '_stats_parser', None)
        if parser is not None:
            parser.print_help()
        return 0

    config = Config.load(args.project_dir)
    store = get_store(config)

    # reviews subcommand
    if stats_subcommand == "reviews":
        today = date.today()
        all_time: bool = getattr(args, 'all_time', False)
        if all_time:
            start_date_r = date.min
            end_date_r = today
        else:
            raw_end: str | None = getattr(args, 'end_date', None)
            raw_start: str | None = getattr(args, 'start_date', None)
            raw_days: int | None = getattr(args, 'days', None)
            end_date_r = datetime.strptime(raw_end, "%Y-%m-%d").date() if raw_end else today
            if raw_start:
                start_date_r = datetime.strptime(raw_start, "%Y-%m-%d").date()
            elif raw_days:
                start_date_r = end_date_r - timedelta(days=raw_days)
            else:
                start_date_r = end_date_r - timedelta(days=14)
        show_issues: bool = getattr(args, 'issues', False)
        output_json: bool = getattr(args, 'json', False)
        return _cmd_stats_reviews(
            config,
            store,
            start_date_r,
            end_date_r,
            show_issues,
            all_time=all_time,
            output_json=output_json,
        )

    if stats_subcommand == "iterations":
        now = datetime.now()
        today = date.today()
        all_time_i: bool = getattr(args, 'all_time', False)
        raw_hours_i: int | None = getattr(args, 'hours', None)
        raw_end_i: str | None = getattr(args, 'end_date', None)
        raw_start_i: str | None = getattr(args, 'start_date', None)
        raw_days_i: int | None = getattr(args, 'days', None)
        last_n: int | None = getattr(args, 'last', None)

        if last_n is not None and last_n <= 0:
            print("Error: --last must be >= 1", file=sys.stderr)
            return 1
        if raw_hours_i is not None and raw_hours_i <= 0:
            print("Error: --hours must be >= 1", file=sys.stderr)
            return 1
        if raw_days_i is not None and raw_days_i <= 0:
            print("Error: --days must be >= 1", file=sys.stderr)
            return 1
        if all_time_i and any(
            value is not None for value in (raw_hours_i, raw_days_i, raw_start_i, raw_end_i)
        ):
            print(
                "Error: --all cannot be combined with --hours/--days/--start-date/--end-date",
                file=sys.stderr,
            )
            return 1
        if raw_hours_i is not None and any(
            value is not None for value in (raw_days_i, raw_start_i, raw_end_i)
        ):
            print(
                "Error: --hours cannot be combined with --days/--start-date/--end-date",
                file=sys.stderr,
            )
            return 1

        start_dt_i: datetime | None
        end_dt_i: datetime | None
        header_range: str
        if all_time_i:
            start_dt_i = None
            end_dt_i = None
            header_range = "All time"
        elif raw_hours_i is not None:
            end_dt_i = now
            start_dt_i = end_dt_i - timedelta(hours=raw_hours_i)
            header_range = (
                f"Last {raw_hours_i} hours "
                f"({start_dt_i.strftime('%Y-%m-%d %H:%M')} - {end_dt_i.strftime('%Y-%m-%d %H:%M')})"
            )
        else:
            end_date_i = datetime.strptime(raw_end_i, "%Y-%m-%d").date() if raw_end_i else today
            if raw_start_i:
                start_date_i = datetime.strptime(raw_start_i, "%Y-%m-%d").date()
            elif raw_days_i:
                start_date_i = end_date_i - timedelta(days=raw_days_i)
            else:
                start_date_i = end_date_i - timedelta(days=14)
            start_dt_i = datetime(start_date_i.year, start_date_i.month, start_date_i.day)
            end_dt_i = datetime(end_date_i.year, end_date_i.month, end_date_i.day) + timedelta(days=1)
            if raw_days_i:
                header_range = f"Last {raw_days_i} days ({start_date_i} - {end_date_i})"
            else:
                header_range = f"{start_date_i} to {end_date_i}"

        return _cmd_stats_iterations(
            config=config,
            store=store,
            start_dt=start_dt_i,
            end_dt=end_dt_i,
            header_range=header_range,
            last_n=last_n,
        )

    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate the gza.yaml configuration file."""
    is_valid, errors, warnings = Config.validate(args.project_dir)

    # Print warnings first
    for warning in warnings:
        print(f"⚠ Warning: {warning}")

    if is_valid:
        print("✓ Configuration is valid")
        return 0
    else:
        print("✗ Configuration validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1


def _preflight_target_label(task_type: str | None) -> str:
    return f"task-type:{task_type}" if task_type else "default"


def _default_preflight_model(config: Config, provider: str) -> str | None:
    return config.get_model_for_task("", provider)


def resolve_preflight_targets(
    config: Config,
    *,
    provider: str | None = None,
    model: str | None = None,
    task_type: str | None = None,
) -> list[CheckTarget]:
    """Resolve distinct provider/model pairs that preflight should exercise."""
    if task_type is not None or provider is not None or model is not None:
        if task_type is not None:
            resolved_provider = provider or config.get_provider_for_task(task_type)
            resolved_model = config.get_model_for_task(task_type, resolved_provider)
            source = _preflight_target_label(task_type)
        else:
            resolved_provider = provider or config.provider
            resolved_model = _default_preflight_model(config, resolved_provider)
            source = "cli"
        if model is not None:
            resolved_model = model
        return [CheckTarget(provider=resolved_provider, model=resolved_model, sources=[source])]

    task_types = set(config.task_providers.keys()) | set(config.task_types.keys())
    for provider_cfg in config.providers.values():
        task_types.update(provider_cfg.task_types.keys())

    targets: dict[tuple[str, str | None], CheckTarget] = {}

    def add_target(target_provider: str, target_model: str | None, source: str) -> None:
        key = (target_provider, target_model)
        existing = targets.get(key)
        if existing is None:
            targets[key] = CheckTarget(
                provider=target_provider,
                model=target_model,
                sources=[source],
            )
            return
        if source not in existing.sources:
            existing.sources.append(source)

    add_target(config.provider, _default_preflight_model(config, config.provider), "default")
    for resolved_task_type in sorted(task_types):
        resolved_provider = config.get_provider_for_task(resolved_task_type)
        resolved_model = config.get_model_for_task(resolved_task_type, resolved_provider)
        add_target(
            resolved_provider,
            resolved_model,
            _preflight_target_label(resolved_task_type),
        )

    return list(targets.values())


def _truncate_preflight_detail(message: str) -> str:
    compact = " ".join(message.split())
    if len(compact) <= _PREFLIGHT_DETAIL_MAX_LEN:
        return compact
    return f"{compact[: _PREFLIGHT_DETAIL_MAX_LEN - 3]}..."


def _parse_preflight_record_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _iter_preflight_log_records(path: Path, *, start_order: int = 0) -> list[_PreflightLogRecord]:
    if not path.exists():
        return []
    records: list[_PreflightLogRecord] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            payload = None
        payload_dict = payload if isinstance(payload, dict) else None
        records.append(
            _PreflightLogRecord(
                order=start_order + len(records),
                timestamp=_parse_preflight_record_timestamp(payload_dict.get("timestamp")) if payload_dict else None,
                payload=payload_dict,
                raw_text=stripped,
            )
        )
    return records


def _decode_preflight_detail_payload(value: object) -> object:
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return stripped
    return parsed


def _extract_preflight_detail_candidate(payload: object) -> tuple[int, str] | None:
    decoded = _decode_preflight_detail_payload(payload)
    if isinstance(decoded, dict):
        decoded_dict = cast(dict[str, Any], decoded)
        error = decoded_dict.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                detail = message.strip()
                priority = 3 if _PREFLIGHT_ERROR_PATTERN.search(detail) else 2
                return (priority, detail)
            if error:
                detail = json.dumps(error, sort_keys=True)
                priority = 3 if _PREFLIGHT_ERROR_PATTERN.search(detail) else 2
                return (priority, detail)
        for key in ("detail", "provider_output", "message"):
            candidate = _extract_preflight_detail_candidate(decoded_dict.get(key))
            if candidate is not None:
                return candidate
        return None
    if isinstance(decoded, str):
        detail = decoded.strip()
        if not detail:
            return None
        if detail.startswith("Running command:"):
            return (0, detail)
        priority = 3 if _PREFLIGHT_ERROR_PATTERN.search(detail) else 1
        return (priority, detail)
    return None


def _preflight_record_sort_key(timestamp: datetime | None, order: int) -> tuple[int, datetime, int]:
    return (1 if timestamp is not None else 0, timestamp or datetime.min.replace(tzinfo=UTC), order)


def _extract_preflight_failure_detail(log_file: Path, result_error_type: str | None) -> str:
    if result_error_type and result_error_type not in {"config_error", "provider_unavailable"}:
        return _truncate_preflight_detail(result_error_type)

    records = _iter_preflight_log_records(log_file)
    records.extend(
        _iter_preflight_log_records(
            log_file.with_name(f"{log_file.stem}.ops.jsonl"),
            start_order=len(records),
        )
    )
    best_detail: tuple[int, int, datetime, int, str] | None = None
    fallback_detail: tuple[datetime | None, int, str] | None = None
    for record in records:
        candidate = _extract_preflight_detail_candidate(record.payload or record.raw_text)
        if candidate is None:
            continue
        priority, detail = candidate
        if priority <= 0:
            if fallback_detail is None or _preflight_record_sort_key(
                record.timestamp, record.order
            ) > _preflight_record_sort_key(fallback_detail[0], fallback_detail[1]):
                fallback_detail = (record.timestamp, record.order, detail)
            continue
        ranking = (priority, *_preflight_record_sort_key(record.timestamp, record.order), detail)
        if best_detail is None or ranking > best_detail:
            best_detail = ranking
    if best_detail is not None:
        return _truncate_preflight_detail(best_detail[4])
    if fallback_detail is not None:
        return _truncate_preflight_detail(fallback_detail[2])
    return "live round-trip failed"


def _format_preflight_credential_failure(
    target: CheckTarget,
    *,
    quick_check_ok: bool,
    verify_message: str | None,
    use_docker: bool,
    provider_hint: str,
) -> str:
    if verify_message:
        detail = verify_message
    elif not quick_check_ok:
        detail = f"no {target.provider} credentials; {provider_hint}"
    else:
        detail = f"{target.provider} credential preflight failed"

    if use_docker:
        detail = (
            f"{detail} Docker runs need API-key env vars; OAuth/keychain credentials do not propagate into containers."
        )
    return _truncate_preflight_detail(detail)


def run_preflight_target(
    config: Config,
    target: CheckTarget,
    *,
    use_docker: bool,
    work_dir: Path,
    log_file: Path,
) -> CheckResult:
    cfg = copy.copy(config)
    cfg.provider = target.provider
    cfg.model = target.model or ""
    cfg.use_docker = use_docker
    cfg.timeout_minutes = 2
    cfg.max_steps = 3
    cfg.max_turns = 3

    from ..providers.base import get_provider

    provider = get_provider(cfg)
    quick_check_ok = provider.check_credentials()
    verify_result = provider.verify_credentials(cfg, log_file=log_file)
    if not quick_check_ok or not verify_result.ok:
        return CheckResult(
            status="FAIL",
            detail=_format_preflight_credential_failure(
                target,
                quick_check_ok=quick_check_ok,
                verify_message=verify_result.message,
                use_docker=use_docker,
                provider_hint=provider.credential_setup_hint,
            ),
            duration_s=0.0,
        )

    run_result = provider.run(
        cfg,
        "Reply with exactly the word: hello",
        log_file,
        work_dir,
    )
    if run_result.exit_code == 0:
        return CheckResult(
            status="PASS",
            detail=f"{run_result.duration_seconds:.1f}s",
            duration_s=run_result.duration_seconds,
        )

    return CheckResult(
        status="FAIL",
        detail=_extract_preflight_failure_detail(log_file, run_result.error_type),
        duration_s=run_result.duration_seconds,
    )


def cmd_preflight(args: argparse.Namespace) -> int:
    """Run a live provider/model sanity check against resolved config routes."""
    try:
        config = Config.load(args.project_dir)
    except ConfigError as exc:
        print(f"Error: {exc}")
        return 1

    use_docker = config.use_docker if args.preflight_docker is None else args.preflight_docker
    targets = resolve_preflight_targets(
        config,
        provider=args.provider,
        model=args.model,
        task_type=args.task_type,
    )
    mode = "docker" if use_docker else "direct"
    results: list[tuple[CheckTarget, CheckResult]] = []

    with tempfile.TemporaryDirectory(prefix="gza-preflight-") as temp_dir:
        temp_path = Path(temp_dir)
        for index, target in enumerate(targets, start=1):
            model_display = target.model or "(default)"
            print(f"Checking {target.provider} / {model_display} ...")
            result = run_preflight_target(
                config,
                target,
                use_docker=use_docker,
                work_dir=args.project_dir,
                log_file=temp_path / f"preflight-{index}.jsonl",
            )
            results.append((target, result))

    table = Table(title=f"Provider/model preflight ({mode})", title_justify="left", title_style="bold")
    table.add_column("PROVIDER")
    table.add_column("MODEL")
    table.add_column("RESULT")
    table.add_column("DETAIL")
    table.add_column("USED BY")

    passes = 0
    fails = 0
    for target, result in results:
        if result.status == "PASS":
            passes += 1
            result_cell = f"[{_colors.green_success}]PASS[/{_colors.green_success}]"
        else:
            fails += 1
            result_cell = f"[{_colors.red_error}]FAIL[/{_colors.red_error}]"
        table.add_row(
            target.provider,
            target.model or "(default)",
            result_cell,
            result.detail,
            ", ".join(target.sources),
        )

    console.print()
    console.print(table)
    summary_style = _colors.green_success if fails == 0 else _colors.red_error
    noun = "failed" if fails == 1 else "failed"
    console.print()
    console.print(f"[{summary_style}]{passes} passed, {fails} {noun}[/{summary_style}]")
    return 0 if fails == 0 else 1


def _config_to_effective_dict(config: Config) -> dict:
    """Build an effective configuration dict from a loaded Config object."""
    return {
        "project_name": config.project_name,
        "db_path": str(config.db_path),
        "tasks_file": config.tasks_file,
        "log_dir": config.log_dir,
        "use_docker": config.use_docker,
        "docker_image": config.docker_image,
        "docker_volumes": config.docker_volumes,
        "docker_setup_command": config.docker_setup_command,
        "timeout_minutes": config.timeout_minutes,
        "code_task_diff_timeout_medium_threshold": config.code_task_diff_timeout_medium_threshold,
        "code_task_diff_timeout_large_threshold": config.code_task_diff_timeout_large_threshold,
        "code_task_diff_timeout_medium_minutes": config.code_task_diff_timeout_medium_minutes,
        "code_task_diff_timeout_large_minutes": config.code_task_diff_timeout_large_minutes,
        "code_task_diff_timeout_cap_minutes": config.code_task_diff_timeout_cap_minutes,
        "branch_mode": config.branch_mode,
        "advance_off_topic_verify_unblock": config.advance_off_topic_verify_unblock,
        "max_steps": config.max_steps,
        "max_turns": config.max_turns,
        "worktree_dir": config.worktree_dir,
        "work_count": config.work_count,
        "main_checkout_isolate": config.main_checkout_isolate,
        "quiet_period_seconds": config.quiet_period_seconds,
        "watch": {
            "batch": config.watch.batch,
            "poll": config.watch.poll,
            "no_activity_timeout": config.watch.no_activity_timeout,
            "max_idle": config.watch.max_idle,
            "max_iterations": config.watch.max_iterations,
            "recovery_slots": config.watch.recovery_slots,
            "failure_backoff_initial": config.watch.failure_backoff_initial,
            "failure_backoff_max": config.watch.failure_backoff_max,
            "transient_recovery_backoff_max": config.watch.transient_recovery_backoff_max,
            "failure_halt_after": config.watch.failure_halt_after,
            "no_progress_cycles": config.watch.no_progress_cycles,
            "dispatch_start_timeout": config.watch.dispatch_start_timeout,
            "parked_auto_rearm": {
                "enabled": config.watch.parked_auto_rearm.enabled,
                "budget": config.watch.parked_auto_rearm.budget,
                "cooldown_hours": config.watch.parked_auto_rearm.cooldown_hours,
                "require_target_advanced": config.watch.parked_auto_rearm.require_target_advanced,
                "judge_enabled": config.watch.parked_auto_rearm.judge_enabled,
                "judge_cooldown_hours": config.watch.parked_auto_rearm.judge_cooldown_hours,
                "judge_max_parked_tasks": config.watch.parked_auto_rearm.judge_max_parked_tasks,
            },
        },
        "provider": config.provider,
        "task_providers": config.task_providers,
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "chat_text_display_length": config.chat_text_display_length,
        "verify_command": config.verify_command,
        "inner_verify_command": config.inner_verify_command,
        "claude": {
            "fetch_auth_token_from_keychain": config.claude.fetch_auth_token_from_keychain,
            "args": config.claude.args,
        },
        "task_types": {
            task_type: {
                "model": task_cfg.model,
                "reasoning_effort": task_cfg.reasoning_effort,
                "max_steps": task_cfg.max_steps,
                "max_turns": task_cfg.max_turns,
                "timeout_minutes": task_cfg.timeout_minutes,
            }
            for task_type, task_cfg in config.task_types.items()
        },
        "providers": {
            provider_name: {
                "model": provider_cfg.model,
                "reasoning_effort": provider_cfg.reasoning_effort,
                "task_types": {
                    task_type: {
                        "model": task_cfg.model,
                        "reasoning_effort": task_cfg.reasoning_effort,
                        "max_steps": task_cfg.max_steps,
                        "max_turns": task_cfg.max_turns,
                        "timeout_minutes": task_cfg.timeout_minutes,
                    }
                    for task_type, task_cfg in provider_cfg.task_types.items()
                },
            }
            for provider_name, provider_cfg in config.providers.items()
        },
        "branch_strategy": {
            "pattern": config.branch_strategy.pattern if config.branch_strategy else None,
            "default_type": config.branch_strategy.default_type if config.branch_strategy else None,
        },
    }


def _flatten_dict(data: dict, prefix: str = "") -> list[tuple[str, object]]:
    """Flatten nested dictionaries into dotted key paths."""
    flattened: list[tuple[str, object]] = []
    for key, value in data.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flattened.extend(_flatten_dict(value, path))
        else:
            flattened.append((path, value))
    return flattened


def _resolve_source_for_effective_path(path: str, source_map: dict[str, str]) -> str:
    """Resolve source attribution for an effective config path.

    Effective output can contain normalized/derived leaf keys (for example
    ``branch_strategy.pattern``), while ``source_map`` may only track the
    originating parent key (for example ``branch_strategy``).
    """
    if path in source_map:
        return source_map[path]

    parent = path
    while "." in parent:
        parent = parent.rsplit(".", 1)[0]
        if parent in source_map:
            return source_map[parent]

    return "default"


def _project_effective_source_map(effective: dict, source_map: dict[str, str]) -> dict[str, str]:
    """Project raw source_map keys onto effective config paths."""
    projected: dict[str, str] = {}
    for path, _value in _flatten_dict(effective):
        projected[path] = _resolve_source_for_effective_path(path, source_map)
    return projected


def cmd_config(args: argparse.Namespace) -> int:
    """Show effective config with source attribution."""
    config = Config.load(args.project_dir)
    effective = _config_to_effective_dict(config)
    effective_sources = _project_effective_source_map(effective, config.source_map)

    if args.json:
        task_types = ["explore", "plan", "implement", "review", "improve", "internal"]
        model_resolution = {}
        for task_type in task_types:
            provider = config.get_provider_for_task(task_type)
            model = config.get_model_for_task(task_type, provider)
            reasoning_effort = config.get_reasoning_effort_for_task(task_type, provider)
            model_resolution[task_type] = {
                "provider": provider,
                "model": model,
                "reasoning_effort": reasoning_effort,
            }
        payload = {
            "effective": effective,
            "sources": effective_sources,
            "model_resolution": model_resolution,
            "user_config_active": config.user_config_active,
            "user_config_file": (
                Config.user_config_display_path() if config.user_config_file else None
            ),
            "local_overrides_active": config.local_overrides_active,
            "local_override_file": (
                config.local_override_path.name if config.local_override_path else None
            ),
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    print("Effective Configuration")
    print("=" * 50)

    # Show config file/directory sources with symlink detection
    gza_dir = Path(args.project_dir) / ".gza"
    if gza_dir.is_symlink():
        print(f"Data dir: .gza -> {os.readlink(gza_dir)}")

    base_path = Config.config_path(Path(args.project_dir))
    if base_path.is_symlink():
        print(f"Config: {base_path.name} -> {os.readlink(base_path)}")
    else:
        print(f"Config: {base_path.name}")

    if config.user_config_active and config.user_config_file:
        print(f"User config: active ({Config.user_config_display_path()})")
    else:
        print("User config: inactive")

    if config.local_overrides_active and config.local_override_path:
        local_path = Config.local_config_path(Path(args.project_dir))
        if local_path.is_symlink():
            print(f"Local overrides: active ({local_path.name} -> {os.readlink(local_path)})")
        else:
            print(f"Local overrides: active ({local_path.name})")
    else:
        print("Local overrides: inactive")
    print()
    rows = sorted(_flatten_dict(effective), key=lambda item: item[0])
    key_width = max((len(path) for path, _ in rows), default=0)
    val_width = max((len(json.dumps(value)) for _, value in rows), default=0)
    for path, value in rows:
        source = effective_sources.get(path, "default")
        print(f"{path:<{key_width}}  {json.dumps(value):<{val_width}}  [{source}]")

    # Model/reasoning resolution summary
    print()
    print("Model/Reasoning Resolution by Task Type")
    print("=" * 50)
    task_types = ["explore", "plan", "implement", "review", "improve", "internal"]
    model_rows = []
    for task_type in task_types:
        provider = config.get_provider_for_task(task_type)
        model = config.get_model_for_task(task_type, provider)
        reasoning_effort = config.get_reasoning_effort_for_task(task_type, provider)
        model_rows.append(
            (
                task_type,
                provider,
                model or "(provider default)",
                reasoning_effort or "(provider default)",
            )
        )
    type_width = max(len(r[0]) for r in model_rows)
    prov_width = max(len(r[1]) for r in model_rows)
    for task_type, provider, model_display, reasoning_effort_display in model_rows:
        print(f"{task_type:<{type_width}}  {provider:<{prov_width}}  {model_display}  {reasoning_effort_display}")

    return 0


def _format_config_key_default(default: object | None, required: bool) -> str:
    if required:
        return "(required)"
    return json.dumps(default)


def cmd_config_keys(args: argparse.Namespace) -> int:
    """List discoverable configuration keys from the single-source registry."""
    rows = sorted(CONFIG_KEY_REGISTRY, key=lambda spec: spec.key)
    if args.json:
        payload = {
            "keys": [
                {
                    "key": spec.key,
                    "type": spec.value_type,
                    "required": spec.required,
                    "default": spec.default,
                    "description": spec.description,
                }
                for spec in rows
            ]
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    key_width = max(len(spec.key) for spec in rows)
    type_width = max(len(spec.value_type) for spec in rows)
    default_width = max(len(_format_config_key_default(spec.default, spec.required)) for spec in rows)
    print(
        f"{'KEY':<{key_width}}  "
        f"{'TYPE':<{type_width}}  "
        f"{'DEFAULT':<{default_width}}  "
        "DESCRIPTION"
    )
    print(
        f"{'-' * key_width}  "
        f"{'-' * type_width}  "
        f"{'-' * default_width}  "
        f"{'-' * len('DESCRIPTION')}"
    )
    for spec in rows:
        default_display = _format_config_key_default(spec.default, spec.required)
        print(
            f"{spec.key:<{key_width}}  "
            f"{spec.value_type:<{type_width}}  "
            f"{default_display:<{default_width}}  "
            f"{spec.description}"
        )
    return 0


def cmd_config_example(args: argparse.Namespace) -> int:
    """Render generated config example files from the config-key registry."""
    local = bool(args.local)
    rendered = render_config_example(local=local)

    output_path: Path | None = None
    if args.write:
        output_path = default_example_path(local=local)
    elif args.output is not None:
        output_path = args.output

    if args.check and output_path is None:
        output_path = default_example_path(local=local)

    if args.check:
        assert output_path is not None
        if not output_path.exists() or output_path.read_text(encoding="utf-8") != rendered:
            print(f"Config example drift detected: {output_path}", file=sys.stderr)
            print(
                "Run `uv run gza config example --write` and "
                "`uv run gza config example --local --write`, then commit the updated example files.",
                file=sys.stderr,
            )
            return 1
        return 0

    if output_path is not None:
        output_path.write_text(rendered, encoding="utf-8")
        print(output_path)
        return 0

    print(rendered, end="")
    return 0


def _find_removable_workers(registry: WorkerRegistry, store: "SqliteTaskStore") -> "list[WorkerMetadata]":
    """Find worker files that can be safely removed.

    A worker is removable if:
    - Its status is completed or failed (finished normally), OR
    - Its process is no longer running (stale), OR
    - It has a task_id whose DB task is already completed/failed
      (zombie worker — PID may have been reused by another process)

    Reads worker JSON directly and only checks PID liveness when needed,
    avoiding the expensive PID checks for workers we can decide on via DB alone.
    """
    import json as json_lib

    from ..workers import WorkerMetadata
    removable = []
    for metadata_path in registry.workers_dir.glob("w-*.json"):
        try:
            with open(metadata_path) as f:
                data = json_lib.load(f)
            worker = WorkerMetadata.from_dict(data)
        except (OSError, json_lib.JSONDecodeError, KeyError):
            continue

        if worker.status in ("completed", "failed"):
            removable.append(worker)
        elif worker.task_id is not None:
            # Check DB first (cheap) — if the task is done, this is a zombie
            task = store.get(worker.task_id)
            if task and task.status in ("completed", "failed"):
                removable.append(worker)
            elif not registry.is_running(worker.worker_id):
                removable.append(worker)
        elif not registry.is_running(worker.worker_id):
            # No task_id — fall back to PID check
            removable.append(worker)
    return removable


def cmd_clean(args: argparse.Namespace) -> int:
    """Clean up stale worktrees, logs, task artifacts, worker metadata, and archives."""
    import shutil
    from datetime import timedelta

    config = Config.load(args.project_dir)

    # Purge mode: delete previously archived files
    if args.purge:
        return _clean_purge(config, args)

    # Archive mode: move old files to archives directory
    if args.archive:
        return _clean_archive(config, args)

    # Default mode: smart state-based cleanup
    store = get_store(config)
    git = Git(config.project_dir)
    registry = WorkerRegistry(config.workers_path)

    days = _resolve_clean_days(args, config, default_days=30)
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    scope_flags = (args.worktrees, args.workers, args.logs, args.backups)
    no_scope = not any(scope_flags)

    # Track what was cleaned
    cleaned_worktrees: list[tuple[str, str]] = []
    cleaned_logs: list[str] = []
    cleaned_artifacts: list[str] = []
    cleaned_workers = 0
    deleted_backups: list[str] = []
    errors: list[tuple[str, Exception]] = []

    # 1. Lineage-aware worktree cleanup
    if args.worktrees or no_scope:
        from gza.query import build_lineage, resolve_lineage_root, task_time_for_lineage

        worktree_dir = config.worktree_path
        # Collect worktrees to remove (with reasons) before prompting
        pending_worktree_removals: list[tuple[Path, str]] = []
        if worktree_dir.exists():
            try:
                worktrees = git.worktree_list()
                worktree_paths = {Path(wt["path"]) for wt in worktrees if wt.get("path")}

                for worktree_path in worktree_dir.iterdir():
                    if not worktree_path.is_dir():
                        continue

                    if worktree_path not in worktree_paths:
                        # Orphaned directory not in git's worktree list
                        pending_worktree_removals.append((worktree_path, "orphaned"))
                        continue

                    # Git-tracked worktree — check lineage age
                    wt_name = worktree_path.name
                    task = store.get_by_slug(wt_name)
                    if task is None:
                        # No task in DB for this worktree — treat as orphaned
                        pending_worktree_removals.append((worktree_path, "no task in DB"))
                        continue

                    # Resolve lineage and check most recent activity
                    root = resolve_lineage_root(store, task)
                    lineage = build_lineage(store, root)
                    most_recent = max(
                        (task_time_for_lineage(t) for t in lineage),
                        default=datetime.min,
                    )
                    # Make cutoff_time naive if most_recent is naive (DB timestamps may lack tz)
                    cutoff_naive = cutoff_time.replace(tzinfo=None) if most_recent.tzinfo is None else cutoff_time
                    if most_recent < cutoff_naive:
                        pending_worktree_removals.append((worktree_path, f"lineage inactive >{days}d"))
                    # else: lineage still active, skip

            except Exception as e:
                errors.append((str(worktree_dir), e))

        # Confirmation prompt before removing worktrees
        if pending_worktree_removals and not args.dry_run:
            if not args.force:
                print(f"\nWorktrees to remove ({len(pending_worktree_removals)}):")
                for wt_path, reason in pending_worktree_removals:
                    print(f"  - {wt_path.name} ({reason})")
                try:
                    answer = input(f"\nRemove {len(pending_worktree_removals)} worktree(s)? [y/N] ")
                except EOFError:
                    answer = ""
                if answer.strip().lower() != "y":
                    print("Skipped worktree removal.")
                    pending_worktree_removals = []

        # Execute removals
        for worktree_path, reason in pending_worktree_removals:
            if args.dry_run:
                cleaned_worktrees.append((worktree_path.name, reason))
            else:
                try:
                    git.worktree_remove(worktree_path, force=True)
                    # worktree_remove uses check=False, so check if dir still exists
                    if worktree_path.exists():
                        shutil.rmtree(worktree_path)
                    cleaned_worktrees.append((worktree_path.name, reason))
                except OSError as e:
                    errors.append((worktree_path.name, e))

    # 2. Clean up old log files
    if args.logs or no_scope:
        unmerged_task_ids: set[str] = set()
        unmerged_task_slugs: set[str] = set()
        if args.keep_unmerged:
            try:
                unmerged_sets = _normalize_unmerged_cleanup_sets(
                    _collect_unmerged_cleanup_sets(store, git)
                )
            except _UnmergedCleanupCollectionError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            unmerged_task_ids = unmerged_sets.task_ids
            unmerged_task_slugs = unmerged_sets.task_slugs
            for warning in unmerged_sets.warnings:
                print(warning, file=sys.stderr)
        if config.log_path.exists():
            processed_logs: set[Path] = set()
            for log_file in config.log_path.iterdir():
                if not log_file.is_file():
                    continue
                if log_file in processed_logs:
                    continue
                conversation_log, ops_log = paired_log_paths(log_file)
                processed_logs.update({conversation_log, ops_log})

                # Check if this log is for an unmerged task
                if args.keep_unmerged:
                    if _preserve_log_group_for_unmerged_task(
                        log_file,
                        unmerged_task_ids=unmerged_task_ids,
                        unmerged_task_slugs=unmerged_task_slugs,
                    ):
                        continue

                # Check age
                existing_group = [path for path in (conversation_log, ops_log) if path.exists()]
                if existing_group and max(path.stat().st_mtime for path in existing_group) < cutoff_timestamp:
                    if args.dry_run:
                        cleaned_logs.extend(path.name for path in existing_group)
                    else:
                        for path in existing_group:
                            try:
                                path.unlink()
                                cleaned_logs.append(path.name)
                            except OSError as e:
                                errors.append((path.name, e))

        terminal_statuses = {"completed", "failed", "dropped"}
        task_by_id = {task.id: task for task in store.get_all() if task.id is not None}
        for artifact in _iter_task_artifacts(store):
            owner = task_by_id.get(artifact.task_id)
            if owner is None or owner.status not in terminal_statuses:
                continue
            if args.keep_unmerged and artifact.task_id in unmerged_task_ids:
                continue
            try:
                if not is_live_artifact_path(artifact.path):
                    continue
                artifact_path = resolve_artifact_path(config.project_dir, artifact.path)
            except InvalidArtifactPathError as exc:
                print(
                    f"Warning: Skipping artifact {artifact.id} for task {artifact.task_id}: {exc}"
                )
                continue
            age_source = (
                artifact_path.stat().st_mtime
                if artifact_path.exists()
                else artifact.created_at.timestamp()
            )
            if age_source >= cutoff_timestamp:
                continue
            if args.dry_run:
                cleaned_artifacts.append(artifact.path)
                continue
            if artifact_path.exists():
                try:
                    artifact_path.unlink()
                    cleaned_artifacts.append(artifact.path)
                except OSError as e:
                    errors.append((artifact.path, e))
            parent = artifact_path.parent
            while parent != config.project_dir and parent.exists() and parent.is_dir():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent

    # 3. Clean up worker metadata for finished/stale/zombie workers
    if args.workers or no_scope:
        removable = _find_removable_workers(registry, store)
        if args.dry_run:
            cleaned_workers = len(removable)
        else:
            for worker in removable:
                registry.remove(worker.worker_id)
                cleaned_workers += 1

    # 4. Clean up old backup files
    if args.backups or no_scope:
        backups_dir = config.project_dir / ".gza" / "backups"
        if backups_dir.exists():
            for backup_file in backups_dir.iterdir():
                if backup_file.is_file():
                    if backup_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            deleted_backups.append(backup_file.name)
                        else:
                            try:
                                backup_file.unlink()
                                deleted_backups.append(backup_file.name)
                            except OSError as e:
                                errors.append((backup_file.name, e))

    # Report results
    if args.dry_run:
        print("Dry run: would clean up resources")
        print()
    else:
        print("Clean completed")
        print()

    if args.worktrees or no_scope:
        if cleaned_worktrees:
            print(f"Worktrees cleaned: {len(cleaned_worktrees)}")
        else:
            print("Worktrees: nothing to clean")
        print()

    if args.logs or no_scope:
        if cleaned_logs:
            print(f"Logs cleaned: {len(cleaned_logs)}")
            if args.keep_unmerged:
                print("  (kept logs for unmerged tasks)")
        else:
            print("Logs: nothing to clean")
        print()
        if cleaned_artifacts:
            print(f"Artifacts cleaned: {len(cleaned_artifacts)}")
            if args.keep_unmerged:
                print("  (kept artifacts for unmerged tasks)")
        else:
            print("Artifacts: nothing to clean")
        print()

    if args.workers or no_scope:
        print(f"Worker files cleaned: {cleaned_workers}")
        print()

    if args.backups or no_scope:
        if deleted_backups:
            print(f"Backups cleaned: {len(deleted_backups)}")
        else:
            print("Backups: nothing to clean")
        print()

    # Report errors
    if errors:
        print(f"Errors ({len(errors)} items):")
        for item, error in errors:
            print(f"  - {item}: {error}", file=sys.stderr)
        return 1

    return 0


def _clean_purge(config: Config, args: argparse.Namespace) -> int:
    """Delete previously archived files older than N days."""
    from datetime import timedelta

    days = _resolve_clean_days(args, config, default_days=365)
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    archives_dir = config.project_dir / ".gza" / "archives"

    deleted_logs = []
    deleted_artifacts = []
    deleted_workers = []
    errors = []

    # Delete from archives/logs
    archives_logs_dir = archives_dir / "logs"
    if archives_logs_dir.exists():
        for log_file in archives_logs_dir.iterdir():
            if log_file.is_file():
                if log_file.stat().st_mtime < cutoff_timestamp:
                    if args.dry_run:
                        deleted_logs.append(log_file)
                    else:
                        try:
                            log_file.unlink()
                            deleted_logs.append(log_file)
                        except OSError as e:
                            errors.append((log_file, e))

    # Delete from archives/workers
    archives_workers_dir = archives_dir / "workers"
    if archives_workers_dir.exists():
        for worker_file in archives_workers_dir.iterdir():
            if worker_file.is_file():
                if worker_file.stat().st_mtime < cutoff_timestamp:
                    if args.dry_run:
                        deleted_workers.append(worker_file)
                    else:
                        try:
                            worker_file.unlink()
                            deleted_workers.append(worker_file)
                        except OSError as e:
                            errors.append((worker_file, e))

    # Delete from archives/artifacts
    archives_artifacts_dir = archives_dir / "artifacts"
    if archives_artifacts_dir.exists():
        for artifact_file in archives_artifacts_dir.rglob("*"):
            if artifact_file.is_file() and artifact_file.stat().st_mtime < cutoff_timestamp:
                if args.dry_run:
                    deleted_artifacts.append(artifact_file)
                else:
                    try:
                        artifact_file.unlink()
                        deleted_artifacts.append(artifact_file)
                    except OSError as e:
                        errors.append((artifact_file, e))

    # Report results
    if args.dry_run:
        print(f"Dry run: would purge archived files older than {days} days")
        print(f"  - Archived logs: {len(deleted_logs)} files")
        print(f"  - Archived workers: {len(deleted_workers)} files")
        print(f"  - Archived artifacts: {len(deleted_artifacts)} files")
    else:
        print(f"Purged archived files older than {days} days:")
        print(f"  - Archived logs: {len(deleted_logs)} files")
        print(f"  - Archived workers: {len(deleted_workers)} files")
        print(f"  - Archived artifacts: {len(deleted_artifacts)} files")

        if errors:
            print()
            print(f"Errors ({len(errors)} files):")
            for file, error in errors:
                print(f"  - {file.name}: {error}", file=sys.stderr)

    return 0


def _clean_archive(config: Config, args: argparse.Namespace) -> int:
    """Archive old log and worker files to .gza/archives/."""
    import shutil
    from datetime import timedelta

    days = _resolve_clean_days(args, config, default_days=30)
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    archives_dir = config.project_dir / ".gza" / "archives"

    archived_logs = []
    archived_artifacts = []
    archived_workers = []
    deleted_backups = []
    errors = []

    scope_flags = (
        getattr(args, 'worktrees', False),
        getattr(args, 'workers', False),
        getattr(args, 'logs', False),
        getattr(args, 'backups', False),
    )
    no_scope = not any(scope_flags)

    # Archive logs
    if args.logs or no_scope:
        unmerged_task_ids: set[str] = set()
        unmerged_task_slugs: set[str] = set()
        if args.keep_unmerged:
            try:
                unmerged_sets = _normalize_unmerged_cleanup_sets(
                    _collect_unmerged_cleanup_sets(
                        get_store(config),
                        Git(config.project_dir),
                    )
                )
            except _UnmergedCleanupCollectionError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
            unmerged_task_ids = unmerged_sets.task_ids
            unmerged_task_slugs = unmerged_sets.task_slugs
            for warning in unmerged_sets.warnings:
                print(warning, file=sys.stderr)
        store = get_store(config)
        if config.log_path.exists():
            archives_logs_dir = archives_dir / "logs"
            processed_logs: set[Path] = set()
            for log_file in config.log_path.iterdir():
                if log_file.is_file():
                    if log_file in processed_logs:
                        continue
                    conversation_log, ops_log = paired_log_paths(log_file)
                    processed_logs.update({conversation_log, ops_log})
                    if args.keep_unmerged and _preserve_log_group_for_unmerged_task(
                        log_file,
                        unmerged_task_ids=unmerged_task_ids,
                        unmerged_task_slugs=unmerged_task_slugs,
                    ):
                        continue
                    existing_group = [path for path in (conversation_log, ops_log) if path.exists()]
                    if existing_group and max(path.stat().st_mtime for path in existing_group) < cutoff_timestamp:
                        if args.dry_run:
                            archived_logs.extend(existing_group)
                        else:
                            for path in existing_group:
                                try:
                                    archives_logs_dir.mkdir(parents=True, exist_ok=True)
                                    dest = archives_logs_dir / path.name
                                    shutil.move(str(path), str(dest))
                                    archived_logs.append(path)
                                except OSError as e:
                                    errors.append((path, e))

        archives_artifacts_dir = archives_dir / "artifacts"
        task_by_id = {task.id: task for task in store.get_all() if task.id is not None}
        for artifact in _iter_task_artifacts(store):
            owner = task_by_id.get(artifact.task_id)
            if owner is None or owner.status not in {"completed", "failed", "dropped"}:
                continue
            if args.keep_unmerged and artifact.task_id in unmerged_task_ids:
                continue
            try:
                if is_archived_artifact_path(artifact.path):
                    continue
                artifact_path = resolve_artifact_path(config.project_dir, artifact.path)
            except InvalidArtifactPathError as exc:
                print(
                    f"Warning: Skipping artifact {artifact.id} for task {artifact.task_id}: {exc}"
                )
                continue
            age_source = (
                artifact_path.stat().st_mtime
                if artifact_path.exists()
                else artifact.created_at.timestamp()
            )
            if age_source >= cutoff_timestamp:
                continue
            if args.dry_run:
                archived_artifacts.append(artifact_path)
                continue
            if not artifact_path.exists():
                continue
            try:
                destination_dir = archives_artifacts_dir / artifact.task_id
                destination_dir.mkdir(parents=True, exist_ok=True)
                dest = destination_dir / artifact_path.name
                shutil.move(str(artifact_path), str(dest))
                archived_artifacts.append(artifact_path)
                _update_task_artifact_path(
                    store,
                    artifact,
                    path=str(dest.relative_to(config.project_dir).as_posix()),
                )
            except OSError as e:
                errors.append((artifact_path, e))

    # Archive workers
    if args.workers or no_scope:
        if config.workers_path.exists():
            archives_workers_dir = archives_dir / "workers"
            for worker_file in config.workers_path.iterdir():
                if worker_file.is_file():
                    if worker_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            archived_workers.append(worker_file)
                        else:
                            try:
                                archives_workers_dir.mkdir(parents=True, exist_ok=True)
                                dest = archives_workers_dir / worker_file.name
                                shutil.move(str(worker_file), str(dest))
                                archived_workers.append(worker_file)
                            except OSError as e:
                                errors.append((worker_file, e))

    # Delete old backups
    if args.backups or no_scope:
        backups_dir = config.project_dir / ".gza" / "backups"
        if backups_dir.exists():
            for backup_file in backups_dir.iterdir():
                if backup_file.is_file():
                    if backup_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            deleted_backups.append(backup_file)
                        else:
                            try:
                                backup_file.unlink()
                                deleted_backups.append(backup_file)
                            except OSError as e:
                                errors.append((backup_file, e))

    # Report results
    if args.dry_run:
        print(f"Dry run: would archive files older than {days} days")
        print(f"  - Logs: {len(archived_logs)} files")
        print(f"  - Workers: {len(archived_workers)} files")
        print(f"  - Artifacts: {len(archived_artifacts)} files")
        print(f"  - Backups deleted: {len(deleted_backups)} files")
    else:
        print(f"Archived files older than {days} days:")
        print(f"  - Logs: {len(archived_logs)} files")
        print(f"  - Workers: {len(archived_workers)} files")
        print(f"  - Artifacts: {len(archived_artifacts)} files")
        print(f"  - Backups deleted: {len(deleted_backups)} files")

        if errors:
            print()
            print(f"Errors ({len(errors)} files):")
            for file, error in errors:
                print(f"  - {file.name}: {error}", file=sys.stderr)

    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Generate a new gza.yaml configuration file with defaults."""
    from ..config import CONFIG_FILENAME, DEFAULT_DB_FILE, LOCAL_CONFIG_FILENAME

    # Derive project name from directory name
    default_project_name = args.project_dir.name

    config_path = args.project_dir / CONFIG_FILENAME

    if config_path.exists() and not args.force:
        print(f"Error: {CONFIG_FILENAME} already exists at {config_path}")
        print("Use --force to overwrite")
        return 1

    # Check if running interactively (stdin is a TTY)
    is_interactive = sys.stdin.isatty()
    normalized_db_args = _normalize_init_db_args(args, is_interactive=is_interactive)
    if normalized_db_args is None:
        return 1

    db_mode, shared_db_path, db_choice_from_flags = normalized_db_args

    try:
        project_id = _generate_project_id(args.project_dir, default_project_name)
        Config.preflight_init_user_config(
            args.project_dir,
            project_name=default_project_name,
            project_id=project_id,
        )
    except ConfigError as exc:
        print(f"Error: {exc}")
        return 1

    has_global_shared_default, _ = _init_has_global_shared_db_default(args.project_dir)

    if is_interactive:
        # Prompt for branch strategy
        print("Branch naming strategy:")
        print("  1. default     - {project}/{date}-{slug} (e.g., myproj/20260107-add-feature)")
        print("  2. conventional - {type}/{slug} (e.g., feature/add-feature, fix/login-bug)")
        print("  3. simple      - {slug} (e.g., add-feature)")
        print("  4. custom      - Define your own pattern")

        while True:
            choice = input("Choose strategy [1-4, default=1]: ").strip() or "1"
            if choice in ("1", "2", "3", "4"):
                break
            print("Invalid choice. Please enter 1, 2, 3, or 4.")

        if db_mode is None:
            print()
            print("Task database:")
            print(f"  1. local   - {DEFAULT_DB_FILE} (this project only)")
            print(f"  2. shared  - {_INIT_SHARED_DB_PATH} (shared across all your projects & worktrees)")
            while True:
                db_choice = input("Choose [1-2, default=2]: ").strip() or "2"
                if db_choice == "1":
                    db_mode = "local"
                    break
                if db_choice == "2":
                    db_mode = "shared"
                    break
                print("Invalid choice. Please enter 1 or 2.")

        if (
            db_mode == "shared"
            and shared_db_path is None
            and not has_global_shared_default
            and not db_choice_from_flags
        ):
            shared_db_path = input(f"Shared DB path [default={_INIT_SHARED_DB_PATH}]: ").strip() or _INIT_SHARED_DB_PATH
    else:
        # Non-interactive mode: use default branch strategy after early DB validation.
        choice = "1"

    if choice == "1":
        branch_strategy = BranchStrategyRender(mode="comment_default")
    elif choice == "2":
        branch_strategy = BranchStrategyRender(mode="preset", preset="conventional")
    elif choice == "3":
        branch_strategy = BranchStrategyRender(mode="preset", preset="simple")
    else:  # custom
        print("\nCustom pattern variables:")
        print("  {project}  - Project name")
        print("  {task_id}  - Full task ID ({prefix}-{decimal}, e.g. gza-1234)")
        print("  {date}     - Date portion (YYYYMMDD)")
        print("  {slug}     - Slug portion")
        print("  {type}     - Inferred/default type (feature, fix, etc.)")

        while True:
            pattern = input("Enter custom pattern: ").strip()
            if pattern:
                break
            print("Pattern cannot be empty.")

        default_type = input("Default type [default=feature]: ").strip() or "feature"
        branch_strategy = BranchStrategyRender(
            mode="custom",
            pattern=pattern,
            default_type=default_type,
        )

    if db_mode == "shared":
        if shared_db_path is not None:
            db_path_value = shared_db_path
        elif has_global_shared_default:
            db_path_value = None
        else:
            db_path_value = _INIT_SHARED_DB_PATH
    else:
        db_path_value = DEFAULT_DB_FILE

    config_content = render_config_example(
        options=ConfigExampleRenderOptions(
            project_name=default_project_name,
            project_name_enabled=True,
            project_id=project_id,
            project_id_enabled=True,
            db_path=db_path_value,
            branch_strategy=branch_strategy,
        )
    )

    config_path.write_text(config_content)
    print(f"✓ Created {config_path}")

    local_example_path = args.project_dir / f"{LOCAL_CONFIG_FILENAME}.example"
    if not local_example_path.exists() or args.force:
        local_example_path.write_text(render_config_example(local=True), encoding="utf-8")
        print(f"✓ Created {local_example_path}")

    # Initialize the database (Config.load will now work since we have project_name)
    config = Config.load(args.project_dir)
    get_store(config)
    print(f"✓ Initialized database at {config.db_path}")

    return 0


def _sync_one_report(task: "Task", config: Config, store: "SqliteTaskStore", *, dry_run: bool) -> str:
    """Sync a single task's report file from disk to DB.

    Returns a status string: 'synced', 'unchanged', 'missing', or 'no_report'.
    """
    if not task.report_file:
        return "no_report"

    report_path = config.project_dir / task.report_file
    if not report_path.exists():
        return "missing"

    disk_content = report_path.read_text()
    if task.output_content == disk_content:
        return "unchanged"

    if not dry_run:
        task.output_content = disk_content
        store.update(task)
    return "synced"


def cmd_sync_report(args: argparse.Namespace) -> int:
    """Sync report file content from disk into DB output_content."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    dry_run = getattr(args, 'dry_run', False)
    sync_all = getattr(args, 'all', False)

    if not sync_all and args.task_id is None:
        console.print("[red]Error: provide a task_id or use --all[/red]")
        return 1

    if sync_all:
        # Scan all tasks with report files
        history = store.get_history(limit=None)
        tasks_with_reports = [t for t in history if t.report_file]

        if not tasks_with_reports:
            console.print("[dim]No tasks with report files found.[/dim]")
            return 0

        synced = 0
        unchanged = 0
        missing = 0
        prefix = "[dry-run] " if dry_run else ""

        for task in tasks_with_reports:
            status = _sync_one_report(task, config, store, dry_run=dry_run)
            if status == "synced":
                console.print(f"{prefix}[green]Synced {task.id} ({task.report_file})[/green]")
                synced += 1
            elif status == "unchanged":
                unchanged += 1
            elif status == "missing":
                missing += 1

        console.print(f"\n{prefix}{synced} synced, {unchanged} unchanged, {missing} missing")
        return 0

    # Single task mode
    task_id = resolve_id(config, args.task_id)
    found_task = store.get(task_id)
    if not found_task:
        console.print(f"[red]Error: Task {task_id} not found[/red]")
        return 1
    task = found_task

    if not task.report_file:
        console.print(f"[red]Error: Task {task_id} has no report file[/red]")
        return 1

    status = _sync_one_report(task, config, store, dry_run=dry_run)
    prefix = "[dry-run] " if dry_run else ""

    if status == "missing":
        console.print(f"[red]Error: Report file not found: {task.report_file}[/red]")
        return 1
    elif status == "unchanged":
        console.print(f"[dim]Task {task_id} already in sync — no changes needed.[/dim]")
    else:
        console.print(f"{prefix}[green]Synced report for task {task_id} from disk to DB.[/green]")
    return 0


def cmd_learnings(args: argparse.Namespace) -> int:
    """Handle learnings subcommands."""
    config = Config.load(args.project_dir)
    subcommand = args.learnings_command

    if not subcommand:
        print("usage: gza learnings <subcommand>")
        print()
        print("Available subcommands:")
        print("  show     Display the current learnings file")
        print("  update   Regenerate learnings from recent completed tasks")
        return 0

    if subcommand == "show":
        learnings_path = config.project_dir / ".gza" / "learnings.md"
        if not learnings_path.exists():
            print("No learnings file found at .gza/learnings.md")
            return 0
        try:
            content = learnings_path.read_text()
        except OSError as e:
            print(f"Error reading learnings file: {e}", file=sys.stderr)
            return 1
        print(content, end="")
        return 0

    if subcommand == "update":
        store = get_store(config)
        window = args.window if hasattr(args, "window") and args.window is not None else DEFAULT_LEARNINGS_WINDOW
        if window <= 0:
            print("Error: --window must be a positive integer", file=sys.stderr)
            return 1
        result = regenerate_learnings(store, config, window=window)
        print(f"Updated learnings: {result.path.relative_to(config.project_dir)}")
        print(f"  Tasks used: {result.tasks_used}")
        print(f"  Learnings: {result.learnings_count}")
        print(
            "  Delta: "
            f"+{result.added_count} / -{result.removed_count} / ={result.retained_count} "
            f"(churn {result.churn_percent:.1f}%)"
        )
        return 0

    print(f"Unknown learnings subcommand: {subcommand}", file=sys.stderr)
    return 1

def _resolve_skill_install_targets(
    project_dir: Path,
    requested_targets: list[str] | None,
    default_targets: list[str],
) -> list[tuple[str, Path]]:
    """Resolve skill install target names to destination directories."""
    codex_home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).expanduser()
    gemini_home = Path(os.environ.get("GEMINI_HOME", str(Path.home() / ".gemini"))).expanduser()
    target_map = {
        "claude": project_dir / ".claude" / "skills",
        "codex": codex_home / "skills",
        "gemini": gemini_home / "skills",
    }

    targets = requested_targets or default_targets
    if "all" in targets:
        normalized_targets = ["claude", "codex"]
    else:
        normalized_targets = []
        for target in targets:
            if target not in target_map:
                continue
            if target not in normalized_targets:
                normalized_targets.append(target)

    return [(target_name, target_map[target_name]) for target_name in normalized_targets]


def cmd_skills_install(
    args: argparse.Namespace,
    default_targets: list[str],
) -> int:
    """Install gza skills from package to one or more target directories."""
    from ..skills_utils import (
        copy_skill,
        get_available_skills,
        get_bundled_skill_time,
        get_installed_skill_time,
        get_skill_description,
        get_skill_version,
        get_skills_source_path,
        is_skill_outdated,
    )

    public_only = not getattr(args, "dev", False)
    print(f"Skills source: {get_skills_source_path()}")

    # Handle --list flag
    if args.list:
        available = get_available_skills(public_only=public_only)
        if not available:
            print("No skills available")
            return 0

        print("Available skills:")
        for skill in available:
            desc = get_skill_description(skill)
            version = get_skill_version(skill)
            version_str = f" (v{version})" if version else ""
            print(f"  {skill:20} - {desc}{version_str}")
        return 0

    # Determine which skills to install
    available = get_available_skills(public_only=public_only)

    if args.skills:
        # When specific skills are requested, check against public skills
        skills_to_install = []
        for skill in args.skills:
            if skill not in available:
                print(f"Error: Skill '{skill}' not found")
                print(f"Available skills: {', '.join(available)}")
                return 1
            skills_to_install.append(skill)
    else:
        # Install all public skills
        skills_to_install = available

    if not skills_to_install:
        print("No skills to install")
        return 0

    requested_targets = getattr(args, "target", None)
    install_targets = _resolve_skill_install_targets(
        project_dir=args.project_dir,
        requested_targets=requested_targets,
        default_targets=default_targets,
    )

    if not install_targets:
        print("Error: No install targets selected")
        return 1

    any_failed = False

    for target_name, target_dir in install_targets:
        # Create target directory
        target_dir.mkdir(parents=True, exist_ok=True)

        # Install skills
        print(f"Installing {len(skills_to_install)} skill(s) to {target_dir} [{target_name}]...")

        installed = 0
        updated = 0
        skipped = 0
        failed = 0
        update_mode = getattr(args, "update", False)

        for skill in skills_to_install:
            success, message = copy_skill(skill, target_dir, args.force)

            if success:
                print(f"  ✓ {skill}")
                installed += 1
            elif "already exists" in message:
                outdated = is_skill_outdated(skill, target_dir)
                if outdated and update_mode:
                    ok, msg = copy_skill(skill, target_dir, force=True)
                    if ok:
                        print(f"  ↑ {skill} (updated)")
                        updated += 1
                    else:
                        print(f"  ✗ {skill} (update failed: {msg})")
                        failed += 1
                elif outdated:
                    installed_time = get_installed_skill_time(skill, target_dir) or "unknown"
                    bundled_time = get_bundled_skill_time(skill) or "unknown"
                    print(f"  ⊘ {skill} (update available: installed {installed_time}, bundled {bundled_time}, use --update)")
                    skipped += 1
                else:
                    print(f"  ⊘ {skill} (up to date)")
                    skipped += 1
            else:
                print(f"  ✗ {skill} ({message})")
                failed += 1

        # Print summary
        print()
        parts = [f"Installed {installed}"]
        if updated > 0:
            parts.append(f"updated {updated}")
        if skipped > 0:
            parts.append(f"skipped {skipped}")
        if failed > 0:
            parts.append(f"failed {failed}")
            any_failed = True
        print(f"{', '.join(parts)} [{target_name}]")
        print()

    return 1 if any_failed else 0
