"""Configuration, stats, cleanup, init, import, and skills-install CLI commands."""

import argparse
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from rich.table import Table

from .. import colors as _colors
from ..config import Config, _generate_project_id
from ..config_schema import CONFIG_KEY_REGISTRY
from ..console import console
from ..db import SqliteTaskStore, Task, task_id_numeric_key
from ..git import Git
from ..importer import import_tasks, parse_import_file, validate_import
from ..learnings import DEFAULT_LEARNINGS_WINDOW, regenerate_learnings
from ..task_slug import get_slug_display_text
from ..workers import WorkerMetadata, WorkerRegistry
from ._common import get_review_verdict, get_store, resolve_id

logger = logging.getLogger(__name__)


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

    # Per-model review cycle stats
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

    # Per-implementer-model review cycle stats
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

    # Per-pair (implement model × review model) cycle stats
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
            content = review_content.get(ri.id)  # type: ignore
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

    # Build filter from shared query args (default stats view)
    limit: int | None = None if getattr(args, 'all', False) else getattr(args, 'last', 5)
    task_type: str | None = getattr(args, 'type', None)
    days: int | None = getattr(args, 'days', None)
    start_date: str | None = getattr(args, 'start_date', None)
    end_date: str | None = getattr(args, 'end_date', None)

    f = HistoryFilter(
        limit=limit,
        task_type=task_type,
        days=days,
        start_date=start_date,
        end_date=end_date,
    )
    tasks = query_history(store, f)

    if not tasks:
        console.print("No completed, failed, or dropped tasks")
        return 0

    if as_json:
        json_tasks = [
            {
                "id": t.id,
                "task_id": t.slug,
                "status": t.status,
                "task_type": t.task_type,
                "prompt": t.prompt,
                "cost_usd": t.cost_usd,
                "duration_seconds": t.duration_seconds,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            }
            for t in tasks
        ]
        print(json.dumps(json_tasks, indent=2))
        return 0

    # Compute summary from filtered task list
    c = TASK_COLORS
    n_completed = sum(1 for t in tasks if t.status == "completed")
    n_failed = sum(1 for t in tasks if t.status == "failed")
    n_dropped = sum(1 for t in tasks if t.status == "dropped")
    total_cost = sum(t.cost_usd or 0 for t in tasks)
    total_duration = sum(t.duration_seconds or 0 for t in tasks)
    total_steps = sum((get_task_step_count(t) or 0) for t in tasks)
    tasks_with_cost = n_completed + n_failed + n_dropped
    avg_cost = total_cost / tasks_with_cost if tasks_with_cost else 0

    # Section header
    console.print(f"[{c['header']}]Summary[/{c['header']}]")
    console.print("=" * 50)
    dropped_str = f", [{c['failure']}]{n_dropped} dropped[/{c['failure']}]" if n_dropped > 0 else ""
    console.print(
        f"  [{c['label']}]Tasks:[/{c['label']}]       "
        f"  [{c['success']}]{n_completed} completed[/{c['success']}]"
        f", [{c['failure']}]{n_failed} failed[/{c['failure']}]"
        f"{dropped_str}"
    )
    console.print(
        f"  [{c['label']}]Total cost:[/{c['label']}]   [{c['value']}]${total_cost:.2f}[/{c['value']}]"
    )
    console.print(
        f"  [{c['label']}]Total time:[/{c['label']}]   [{c['value']}]{format_duration(total_duration, verbose=True)}[/{c['value']}]"
    )
    console.print(
        f"  [{c['label']}]Total steps:[/{c['label']}]  [{c['value']}]{total_steps}[/{c['value']}]"
    )
    if tasks_with_cost:
        console.print(
            f"  [{c['label']}]Avg cost:[/{c['label']}]     [{c['value']}]${avg_cost:.2f}/task[/{c['value']}]"
        )
    console.print()

    # Task table
    terminal_width = get_terminal_width()
    table_width = int(terminal_width * 0.8)

    # Fixed column widths
    status_width = 8
    id_width = 6
    type_width = 10
    cost_width = 8
    turns_width = 6
    time_width = 8
    len_width = 5

    # Calculate remaining space for prompt column
    fixed_width = status_width + id_width + type_width + cost_width + turns_width + time_width + len_width + 7
    prompt_width = max(20, table_width - fixed_width)

    label = "All" if getattr(args, 'all', False) else f"Last {len(tasks)}"
    console.print(f"[{c['header']}]{label} Tasks[/{c['header']}]")
    console.print("=" * 50)

    # Table header
    console.print(f"{'Status':<{status_width}} {'ID':>{id_width}} {'Type':<{type_width}} {'Cost':>{cost_width}} {'Steps':>{turns_width}} {'Time':>{time_width}} {'Len':>{len_width}}  Prompt")
    console.print("-" * table_width)

    for task in tasks:
        is_ok = task.status == "completed"
        status_str = "✓" if is_ok else "✗"
        status_col = (
            f"[{c['success']}]{status_str:<{status_width}}[/{c['success']}]" if is_ok
            else f"[{c['failure']}]{status_str:<{status_width}}[/{c['failure']}]"
        )
        id_str = f"#{task.id}" if task.id is not None else "-"
        type_str = task.task_type[:type_width] if task.task_type else "-"
        cost_str = f"${task.cost_usd:.4f}" if task.cost_usd is not None else "-"
        resolved_steps = get_task_step_count(task)
        turns_str = str(resolved_steps) if resolved_steps is not None else "-"
        time_str = format_duration(task.duration_seconds, verbose=True) if task.duration_seconds else "-"
        prompt_len = len(task.prompt)
        len_str = str(prompt_len)
        prompt = task.prompt
        if len(prompt) > prompt_width:
            prompt = prompt[:prompt_width - 3] + "..."

        # Use console.print for colorized status; pad manually to preserve alignment
        id_col = f"[{c['task_id']}]{id_str:>{id_width}}[/{c['task_id']}]"
        type_col = f"[{c['stats']}]{type_str:<{type_width}}[/{c['stats']}]"
        prompt_col = f"[{c['prompt']}]{prompt}[/{c['prompt']}]"
        console.print(
            f"{status_col} {id_col} {type_col} {cost_str:>{cost_width}} {turns_str:>{turns_width}} {time_str:>{time_width}} {len_str:>{len_width}}  {prompt_col}"
        )

    console.print()
    console.print(
        f"[{c['label']}]Total for shown:[/{c['label']}] [{c['value']}]${sum(t.cost_usd or 0 for t in tasks):.2f}[/{c['value']}]"
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
        "branch_mode": config.branch_mode,
        "max_steps": config.max_steps,
        "max_turns": config.max_turns,
        "worktree_dir": config.worktree_dir,
        "work_count": config.work_count,
        "main_checkout_isolate": config.main_checkout_isolate,
        "provider": config.provider,
        "task_providers": config.task_providers,
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "chat_text_display_length": config.chat_text_display_length,
        "verify_command": config.verify_command,
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
    """Clean up stale worktrees, old logs, worker metadata, and archives."""
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

    days = args.days if args.days is not None else config.cleanup_days
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    scope_flags = (args.worktrees, args.workers, args.logs, args.backups)
    no_scope = not any(scope_flags)

    # Track what was cleaned
    cleaned_worktrees: list[tuple[str, str]] = []
    cleaned_logs: list[str] = []
    cleaned_workers = 0
    deleted_backups: list[str] = []
    errors: list[tuple[str, Exception]] = []

    # 1. Lineage-aware worktree cleanup
    if args.worktrees or no_scope:
        from gza.query import build_lineage, resolve_lineage_root, task_time_for_lineage

        print("Scanning worktrees...")
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
        print("Scanning logs...")
        if config.log_path.exists():
            # Get list of unmerged tasks if --keep-unmerged is set
            unmerged_task_ids = set()
            if args.keep_unmerged:
                try:
                    # Check completed tasks with branches for unmerged work
                    default_branch = git.default_branch()
                    history = store.get_history(limit=200)
                    for task in history:
                        if task.status == "completed" and task.branch and task.has_commits:
                            try:
                                if task.merge_status != "merged" and not git.is_merged(task.branch, default_branch):
                                    if task.slug:
                                        unmerged_task_ids.add(task.slug)
                            except Exception:
                                logger.warning(
                                    "Failed to check merge state for task %s branch=%s during cleanup",
                                    task.id,
                                    task.branch,
                                    exc_info=True,
                                )
                except Exception as e:
                    logger.warning("Could not collect unmerged tasks during cleanup", exc_info=True)
                    print(f"Warning: Could not fetch unmerged tasks: {e}", file=sys.stderr)

            for log_file in config.log_path.iterdir():
                if not log_file.is_file():
                    continue

                # Check if this log is for an unmerged task
                if args.keep_unmerged:
                    # Extract task_id from log filename (format: YYYYMMDD-slug.log or task-id.log)
                    task_id = log_file.stem
                    if task_id in unmerged_task_ids:
                        continue

                # Check age
                if log_file.stat().st_mtime < cutoff_timestamp:
                    if args.dry_run:
                        cleaned_logs.append(log_file.name)
                    else:
                        try:
                            log_file.unlink()
                            cleaned_logs.append(log_file.name)
                        except OSError as e:
                            errors.append((log_file.name, e))

    # 3. Clean up worker metadata for finished/stale/zombie workers
    if args.workers or no_scope:
        print("Scanning workers...")
        removable = _find_removable_workers(registry, store)
        if removable:
            print(f"Found {len(removable)} worker file(s) to clean up...")
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
            for name, reason in cleaned_worktrees:
                print(f"  - {name} ({reason})")
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

    days = args.days if args.days is not None else 365
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    archives_dir = config.project_dir / ".gza" / "archives"

    deleted_logs = []
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

    # Report results
    if args.dry_run:
        print(f"Dry run: would purge archived files older than {days} days")
        print()
        if deleted_logs:
            print(f"Archived logs ({len(deleted_logs)} files):")
            for log_file in deleted_logs:
                print(f"  - {log_file.name}")
        else:
            print("Archived logs: no files to purge")

        print()
        if deleted_workers:
            print(f"Archived workers ({len(deleted_workers)} files):")
            for worker_file in deleted_workers:
                print(f"  - {worker_file.name}")
        else:
            print("Archived workers: no files to purge")
    else:
        print(f"Purged archived files older than {days} days:")
        print(f"  - Archived logs: {len(deleted_logs)} files")
        print(f"  - Archived workers: {len(deleted_workers)} files")

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

    days = args.days if args.days is not None else 30
    cutoff_time = datetime.now(UTC) - timedelta(days=days)
    cutoff_timestamp = cutoff_time.timestamp()

    archives_dir = config.project_dir / ".gza" / "archives"

    archived_logs = []
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
        if config.log_path.exists():
            archives_logs_dir = archives_dir / "logs"
            for log_file in config.log_path.iterdir():
                if log_file.is_file():
                    if log_file.stat().st_mtime < cutoff_timestamp:
                        if args.dry_run:
                            archived_logs.append(log_file)
                        else:
                            try:
                                archives_logs_dir.mkdir(parents=True, exist_ok=True)
                                dest = archives_logs_dir / log_file.name
                                shutil.move(str(log_file), str(dest))
                                archived_logs.append(log_file)
                            except OSError as e:
                                errors.append((log_file, e))

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
        print()
        if archived_logs:
            print(f"Logs ({len(archived_logs)} files):")
            for log_file in archived_logs:
                print(f"  - {log_file.name}")
        else:
            print("Logs: no files to archive")

        print()
        if archived_workers:
            print(f"Workers ({len(archived_workers)} files):")
            for worker_file in archived_workers:
                print(f"  - {worker_file.name}")
        else:
            print("Workers: no files to archive")

        print()
        if deleted_backups:
            print(f"Backups ({len(deleted_backups)} files):")
            for backup_file in deleted_backups:
                print(f"  - {backup_file.name}")
        else:
            print("Backups: no files to delete")
    else:
        print(f"Archived files older than {days} days:")
        print(f"  - Logs: {len(archived_logs)} files")
        print(f"  - Workers: {len(archived_workers)} files")
        print(f"  - Backups deleted: {len(deleted_backups)} files")

        if errors:
            print()
            print(f"Errors ({len(errors)} files):")
            for file, error in errors:
                print(f"  - {file.name}: {error}", file=sys.stderr)

    return 0


def cmd_init(args: argparse.Namespace) -> int:
    """Generate a new gza.yaml configuration file with defaults."""
    import importlib.resources

    from ..config import CONFIG_FILENAME, LOCAL_CONFIG_FILENAME

    # Derive project name from directory name
    default_project_name = args.project_dir.name

    config_path = args.project_dir / CONFIG_FILENAME

    if config_path.exists() and not args.force:
        print(f"Error: {CONFIG_FILENAME} already exists at {config_path}")
        print("Use --force to overwrite")
        return 1

    # Read the example template from the package
    template = importlib.resources.files("gza").joinpath("gza.yaml.example").read_text()

    # Check if running interactively (stdin is a TTY)
    is_interactive = sys.stdin.isatty()

    if is_interactive:
        # Prompt for branch strategy
        print("Branch naming strategy:")
        print("  1. monorepo    - {project}/{task_id} (e.g., myproj/20260107-add-feature)")
        print("  2. conventional - {type}/{slug} (e.g., feature/add-feature, fix/login-bug)")
        print("  3. simple      - {slug} (e.g., add-feature)")
        print("  4. custom      - Define your own pattern")

        while True:
            choice = input("Choose strategy [1-4, default=1]: ").strip() or "1"
            if choice in ("1", "2", "3", "4"):
                break
            print("Invalid choice. Please enter 1, 2, 3, or 4.")
    else:
        # Non-interactive mode: use default (monorepo)
        choice = "1"

    project_id = _generate_project_id(args.project_dir, default_project_name)

    # Replace project metadata placeholders
    config_content = template.replace("project_name: my-project", f"project_name: {default_project_name}")
    config_content = config_content.replace("# project_id: myproject01", f"project_id: {project_id}")

    # Apply branch strategy based on user's choice
    default_branch_line = "# branch_strategy: monorepo  # Default: {project}/{task_id}"
    if choice == "1":
        pass  # Keep commented-out default from template
    elif choice == "2":
        config_content = config_content.replace(
            default_branch_line,
            "branch_strategy: conventional  # {type}/{slug}",
        )
    elif choice == "3":
        config_content = config_content.replace(
            default_branch_line,
            "branch_strategy: simple  # {slug}",
        )
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
        custom_strategy = f'branch_strategy:\n  pattern: "{pattern}"\n  default_type: {default_type}'
        config_content = config_content.replace(default_branch_line, custom_strategy)

    config_path.write_text(config_content)
    print(f"✓ Created {config_path}")

    local_example_path = args.project_dir / f"{LOCAL_CONFIG_FILENAME}.example"
    if not local_example_path.exists() or args.force:
        local_template = importlib.resources.files("gza").joinpath("gza.local.yaml.example").read_text()
        local_example_path.write_text(local_template)
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


def cmd_import(args: argparse.Namespace) -> int:
    """Import tasks from a YAML file."""
    # Backward compatibility: if the positional argument is a directory,
    # treat it as --project and require an explicit import file.
    if args.file and Path(args.file).is_dir():
        args.project_dir = Path(args.file).resolve()
        args.file = None

    config = Config.load(args.project_dir)
    store = get_store(config)

    # Determine which file to import
    if args.file:
        import_path = Path(args.file)
        if not import_path.is_absolute():
            import_path = config.project_dir / import_path
    else:
        print("Error: No file specified")
        print("Usage: gza import <file> [--dry-run] [--force]")
        return 1

    # Parse the import file
    tasks, default_group, default_spec, parse_errors = parse_import_file(import_path)

    if parse_errors:
        print("Error: Failed to parse import file:")
        for error in parse_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Validate the tasks
    validation_errors = validate_import(tasks, config.project_dir, default_spec)

    if validation_errors:
        print("Error: Validation failed:")
        for error in validation_errors:
            if error.task_index:
                print(f"  Task {error.task_index}: {error.message}")
            else:
                print(f"  {error.message}")
        return 1

    # Import the tasks
    if args.dry_run:
        print(f"Would import {len(tasks)} tasks:")
    else:
        print(f"Importing {len(tasks)} tasks...")

    results, messages = import_tasks(
        store=store,
        tasks=tasks,
        project_dir=config.project_dir,
        dry_run=args.dry_run,
        force=args.force,
    )

    for message in messages:
        print(message)

    # Summary
    if args.dry_run:
        return 0

    created = sum(1 for r in results if not r.skipped)
    skipped = sum(1 for r in results if r.skipped)

    if skipped:
        print(f"Imported {created} tasks ({skipped} skipped)")
    else:
        print(f"Imported {created} tasks")

    return 0



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
