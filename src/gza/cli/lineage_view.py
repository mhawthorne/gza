"""Shared rich renderers for merge-unit lineage views.

`gza lineage` and `gza show` both render lineage through this module so the
grouped and local-window views stay identical. The grouping itself lives in
:mod:`gza.lineage_grouping`; this module owns the colour/status presentation
that belongs in the CLI layer.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime

from rich.markup import escape as rich_escape

import gza.colors as _colors
from gza.config import Config
from gza.console import console
from gza.db import SqliteTaskStore, Task as DbTask
from gza.lineage_grouping import (
    MergeUnitGroup,
    build_merge_unit_group_tree,
    find_group_path,
    format_lineage_summary,
    group_subtree_counts,
)
from gza.query import TaskLineageNode

from ._common import (
    _lineage_tree_prefix,
    format_task_merge_label,
    format_task_status_text,
    get_review_verdict,
    get_task_status_color,
)


def _strip_slug_date_prefix(slug: str) -> str:
    if len(slug) > 9 and slug[8] == "-" and slug[:8].isdigit():
        return slug[9:]
    return slug


def prompt_text(t: DbTask) -> str:
    """Short prompt/slug label shown for plan and implement rows."""
    type_str = t.task_type or "implement"
    if type_str not in {"plan", "plan_improve", "plan_review", "implement"}:
        return ""
    if t.slug:
        value = _strip_slug_date_prefix(t.slug)
    else:
        value = t.prompt.split("\n")[0].strip()
    return value[:60] + "…" if len(value) > 60 else value


def format_utc(value: datetime) -> str:
    ts = value.astimezone(UTC) if value.tzinfo is not None else value
    return f"{ts.strftime('%Y-%m-%d %H:%M:%S')} UTC"


def group_state(
    group: MergeUnitGroup,
    status_color: Callable[[DbTask], str] | None = None,
) -> tuple[str, str]:
    """Return (state text, color) for a merge-unit group header."""
    status_color = status_color or get_task_status_color
    merged_color = _colors.STATUS_COLORS.completed
    unmerged_color = _colors.STATUS_COLORS.unmerged
    t = group.header
    if group.unit is not None:
        state = group.unit.state or "open"
        if state == "merged":
            return "merged", merged_color
        if state == "unmerged":
            return "unmerged", unmerged_color
        return state, status_color(t)
    status_text = format_task_status_text(t)
    merge_label = format_task_merge_label(t)
    if merge_label:
        status_text = f"{status_text} [{merge_label}]"
    return status_text, status_color(t)


def member_annotation(
    config: Config,
    store: SqliteTaskStore,
    member: DbTask,
    status_color: Callable[[DbTask], str] | None = None,
) -> tuple[str, str]:
    """Return (text, color) describing a supporting task within a merge unit."""
    status_color = status_color or get_task_status_color
    lc = _colors.LINEAGE_COLORS
    if member.task_type == "review":
        verdict = get_review_verdict(config, member)
        if verdict:
            return verdict, status_color(member)
    elif member.task_type == "plan_review":
        # Lazy import avoids a cli.query <-> cli.lineage_view import loop.
        from .query import _plan_review_detail

        verdict, _manifest = _plan_review_detail(task=member, config=config, store=store)
        if verdict:
            return verdict, status_color(member)
    elif member.task_type == "improve" and member.depends_on:
        return f"addresses {member.depends_on}", lc.annotation
    if member.task_type == "implement":
        merge_label = format_task_merge_label(member)
        suffix = f" [{merge_label}]" if merge_label else ""
        return f"re-attempt · {format_task_status_text(member)}{suffix}", status_color(member)
    return format_task_status_text(member), status_color(member)


def render_merge_grouped(
    *,
    store: SqliteTaskStore,
    config: Config,
    task_id: str,
    full_tree: TaskLineageNode,
    status_color: Callable[[DbTask], str] | None = None,
) -> None:
    """Render the whole lineage grouped by merge unit (the --full view)."""
    groups = build_merge_unit_group_tree(store, full_tree)
    lc = _colors.LINEAGE_COLORS

    header_rows: list[dict] = []
    member_rows: list[dict] = []

    def _collect(group: MergeUnitGroup, ancestors_last: tuple[bool, ...]) -> None:
        t = group.header
        when = t.completed_at or t.started_at or t.created_at
        state_text, state_color = group_state(group, status_color)
        header_rows.append(
            {
                "prefix": _lineage_tree_prefix(ancestors_last),
                "is_target": t.id == task_id,
                "id": t.id or "-",
                "when": format_utc(when) if when else "-",
                "type": t.task_type or "implement",
                "state": state_text,
                "state_color": state_color,
                "prompt": prompt_text(t),
                "row": len(header_rows) + len(member_rows),
            }
        )
        guide = "".join("    " if flag else "│   " for flag in ancestors_last)
        member_guide = guide + ("│   " if group.children else "    ")
        for member in group.members:
            annotation, annotation_color = member_annotation(config, store, member, status_color)
            member_rows.append(
                {
                    "guide": member_guide,
                    "is_target": member.id == task_id,
                    "type": member.task_type or "task",
                    "id": member.id or "-",
                    "annotation": annotation,
                    "annotation_color": annotation_color,
                    "row": len(header_rows) + len(member_rows),
                }
            )
        for index, child in enumerate(group.children):
            _collect(child, (*ancestors_last, index == len(group.children) - 1))

    for group in groups:
        _collect(group, ())

    if not header_rows:
        return

    prefix_width = max(len(r["prefix"]) for r in header_rows)
    id_width = max([len(r["id"]) for r in header_rows] + [len(r["id"]) for r in member_rows] + [1])
    when_width = max(len(r["when"]) for r in header_rows)
    type_width = max(len(r["type"]) for r in header_rows)
    state_width = max(len(r["state"]) for r in header_rows)
    member_type_width = max([len(r["type"]) for r in member_rows] + [1])
    # Header columns left of the prompt are fixed-width; budget the prompt to the
    # remaining terminal width so long branch slugs don't wrap.
    try:
        term_width = console.size.width
    except Exception:
        term_width = 120
    # prefix + arrow + id + when + type + state, each followed by one space.
    header_used = (
        prefix_width + 1 + 1 + 1 + id_width + 1 + when_width + 1 + type_width + 1 + state_width + 1
    )
    prompt_budget = max(20, term_width - header_used - 1)

    # Re-emit header and member rows in their original interleaved order.
    by_row: dict[int, tuple[str, dict]] = {}
    for r in header_rows:
        by_row[r["row"]] = ("header", r)
    for r in member_rows:
        by_row[r["row"]] = ("member", r)

    for _, (kind, r) in sorted(by_row.items()):
        if kind == "header":
            prefix_part = f"[{lc.connector}]{rich_escape(r['prefix'].ljust(prefix_width))}[/{lc.connector}]"
            arrow_char = "→" if r["is_target"] else " "
            arrow_part = f"[{lc.target_highlight}]{arrow_char}[/{lc.target_highlight}]"
            id_part = f"[{lc.task_id}]{rich_escape(r['id'].ljust(id_width))}[/{lc.task_id}]"
            when_part = f"[{lc.stats}]{rich_escape(r['when'].ljust(when_width))}[/{lc.stats}]"
            type_part = f"[{lc.type_label}]{rich_escape(r['type'].ljust(type_width))}[/{lc.type_label}]"
            state_part = f"[{r['state_color']}]{rich_escape(r['state'].ljust(state_width))}[/{r['state_color']}]"
            pieces = [prefix_part, arrow_part, id_part, when_part, type_part, state_part]
            prompt = r["prompt"]
            if prompt:
                if len(prompt) > prompt_budget:
                    prompt = prompt[: prompt_budget - 1] + "…"
                pieces.append(f"[{lc.prompt}]{rich_escape(prompt)}[/{lc.prompt}]")
            console.print(" ".join(pieces).rstrip())
        else:
            guide_part = f"[{lc.connector}]{rich_escape(r['guide'])}[/{lc.connector}]"
            if r["is_target"]:
                marker = f"[{lc.target_highlight}]→[/{lc.target_highlight}]"
            else:
                marker = f"[{lc.annotation}]·[/{lc.annotation}]"
            type_part = f"[{lc.type_label}]{rich_escape(r['type'].ljust(member_type_width))}[/{lc.type_label}]"
            id_part = f"[{lc.task_id}]{rich_escape(r['id'].ljust(id_width))}[/{lc.task_id}]"
            annotation_part = (
                f"[{r['annotation_color']}]{rich_escape(r['annotation'])}[/{r['annotation_color']}]"
                if r["annotation"]
                else ""
            )
            pieces = [f"{guide_part}{marker}", type_part, id_part]
            if annotation_part:
                pieces.append(annotation_part)
            console.print(" ".join(pieces).rstrip())


def render_local(
    *,
    store: SqliteTaskStore,
    config: Config,
    task_id: str,
    full_tree: TaskLineageNode,
    heading: str,
    status_color: Callable[[DbTask], str] | None = None,
) -> bool:
    """Render a focused window: ancestors, peers, this task and its children.

    Returns False when the task cannot be located in the grouped tree so callers
    can fall back to another rendering.
    """
    status_color = status_color or get_task_status_color
    groups = build_merge_unit_group_tree(store, full_tree)
    path = find_group_path(groups, task_id)
    if not path:
        return False

    lc = _colors.LINEAGE_COLORS
    target = path[-1]
    parent = path[-2] if len(path) >= 2 else None
    ancestors = path[:-1]
    peers = [c for c in parent.children if c is not target] if parent is not None else []

    total_tasks = sum(group_subtree_counts(r)[0] for r in groups)
    total_units = sum(group_subtree_counts(r)[1] for r in groups)

    # Column widths over every group header we will print.
    printable = [*ancestors, target, *peers, *target.children]
    id_width = max((len(g.header.id or "-") for g in printable), default=1)
    type_width = max((len(g.header.task_type or "implement") for g in printable), default=1)
    state_width = max((len(group_state(g, status_color)[0]) for g in printable), default=1)

    def _emit(group: MergeUnitGroup, *, counts: tuple[int, int] | None, note: str) -> None:
        t = group.header
        is_target = t.id == task_id
        state_text, state_color = group_state(group, status_color)
        marker = "→" if is_target else " "
        pieces = [
            f"  [{lc.target_highlight}]{marker}[/{lc.target_highlight}]",
            f"[{lc.task_id}]{rich_escape((t.id or '-').ljust(id_width))}[/{lc.task_id}]",
            f"[{lc.type_label}]{rich_escape((t.task_type or 'implement').ljust(type_width))}[/{lc.type_label}]",
            f"[{state_color}]{rich_escape(state_text.ljust(state_width))}[/{state_color}]",
        ]
        prompt = prompt_text(t)
        if prompt:
            pieces.append(f"[{lc.prompt}]{rich_escape(prompt)}[/{lc.prompt}]")
        if counts is not None:
            ct, cu = counts
            tasks_word = "task" if ct == 1 else "tasks"
            units_word = "unit" if cu == 1 else "units"
            pieces.append(f"[{lc.annotation}]({ct} {tasks_word}, {cu} {units_word})[/{lc.annotation}]")
        if note:
            pieces.append(f"[{lc.annotation}]{rich_escape(note)}[/{lc.annotation}]")
        console.print(" ".join(pieces).rstrip())

    units_word = "merge unit" if total_units == 1 else "merge units"
    console.print(
        f"{heading}  ·  "
        f"[{lc.stats}]{total_tasks} tasks, {total_units} {units_word} in this lineage[/{lc.stats}]"
    )

    if ancestors:
        console.print(f"\n[{lc.type_label}]Ancestors[/{lc.type_label}] (root → parent):")
        for index, group in enumerate(ancestors):
            note = "(root)" if index == 0 else "(parent)" if index == len(ancestors) - 1 else ""
            _emit(group, counts=None, note=note)

    console.print(f"\n[{lc.type_label}]This task[/{lc.type_label}]:")
    if target.header.id != task_id:
        target_member = next((m for m in target.members if m.id == task_id), None)
        member_type = (target_member.task_type if target_member else None) or "task"
        console.print(
            f"  [{lc.annotation}]{rich_escape(task_id)} is a {member_type} within "
            f"merge unit {target.header.id}[/{lc.annotation}]"
        )
    _emit(target, counts=None, note="")
    for member in target.members:
        annotation, annotation_color = member_annotation(config, store, member, status_color)
        mk = (
            f"[{lc.target_highlight}]→[/{lc.target_highlight}]"
            if member.id == task_id
            else f"[{lc.annotation}]·[/{lc.annotation}]"
        )
        type_part = f"[{lc.type_label}]{rich_escape((member.task_type or 'task').ljust(8))}[/{lc.type_label}]"
        id_part = f"[{lc.task_id}]{rich_escape((member.id or '-').ljust(id_width))}[/{lc.task_id}]"
        annotation_part = (
            f"[{annotation_color}]{rich_escape(annotation)}[/{annotation_color}]" if annotation else ""
        )
        console.print(" ".join(p for p in [f"      {mk}", type_part, id_part, annotation_part] if p).rstrip())

    if peers:
        anchor = parent.header.id if parent is not None else "?"
        console.print(f"\n[{lc.type_label}]Peers[/{lc.type_label}] (share {anchor}): {len(peers)}")
        for group in peers:
            _emit(group, counts=group_subtree_counts(group), note="")

    console.print(f"\n[{lc.type_label}]Children[/{lc.type_label}]: {len(target.children)}")
    if not target.children:
        console.print(f"  [{lc.annotation}](none)[/{lc.annotation}]")
    for group in target.children:
        _emit(group, counts=group_subtree_counts(group), note="")
    return True


def lineage_summary_stats(
    store: SqliteTaskStore,
    task: DbTask,
    cache: dict[str | None, tuple[list[MergeUnitGroup], int, int]],
) -> dict[str, object]:
    """Return structured lineage-orientation stats for a feed row.

    Orients a task by the meaningful nodes above it rather than raw counts:
    ``lineage_origin_id``/``lineage_origin_type`` (the plan or explore the work
    derives from), ``lineage_root_impl_id`` (the first implement in the chain),
    and ``lineage_parent_impl_id`` (the implement this one builds on). Also keeps
    ``lineage_root_id`` and whole-lineage totals for scripting. ``cache`` is keyed
    by lineage-root id so a feed builds each root's tree once.
    """
    empty: dict[str, object] = {
        "lineage_root_id": None,
        "lineage_origin_id": None,
        "lineage_origin_type": None,
        "lineage_root_impl_id": None,
        "lineage_parent_impl_id": None,
        "lineage_task_count": 0,
        "lineage_merge_unit_count": 0,
    }
    if task.id is None:
        return empty

    from gza.lineage import resolve_lineage_root
    from gza.query import build_lineage_tree

    root = resolve_lineage_root(store, task)
    cached = cache.get(root.id)
    if cached is None:
        tree = build_lineage_tree(store, root, max_depth=None)
        groups = build_merge_unit_group_tree(store, tree)
        total_tasks = sum(group_subtree_counts(g)[0] for g in groups)
        total_units = sum(group_subtree_counts(g)[1] for g in groups)
        cached = (groups, total_tasks, total_units)
        cache[root.id] = cached
    groups, total_tasks, total_units = cached

    origin_id: str | None = None
    origin_type: str | None = None
    root_impl_id: str | None = None
    parent_impl_id: str | None = None
    path = find_group_path(groups, task.id)
    if path is not None:
        for group in path:  # root → task's group
            header = group.header
            if origin_id is None and header.task_type in {"plan", "plan_improve", "explore"}:
                origin_id, origin_type = header.id, header.task_type
            if root_impl_id is None and header.task_type == "implement":
                root_impl_id = header.id
        owning = path[-1]
        if owning.header.id == task.id:
            # The row heads a merge unit: its parent implement is the prior slice.
            if len(path) >= 2 and path[-2].header.task_type == "implement":
                parent_impl_id = path[-2].header.id
        elif owning.header.task_type == "implement":
            # The row is a supporting task; its implement is the owning unit head.
            parent_impl_id = owning.header.id
        # Drop redundant references so the display shows each node once.
        if root_impl_id == task.id:
            root_impl_id = None
        if parent_impl_id is not None and parent_impl_id == root_impl_id:
            parent_impl_id = None

    return {
        "lineage_root_id": root.id,
        "lineage_origin_id": origin_id,
        "lineage_origin_type": origin_type,
        "lineage_root_impl_id": root_impl_id,
        "lineage_parent_impl_id": parent_impl_id,
        "lineage_task_count": total_tasks,
        "lineage_merge_unit_count": total_units,
    }


def lineage_summary_line(
    store: SqliteTaskStore,
    task: DbTask,
    cache: dict[str | None, tuple[list[MergeUnitGroup], int, int]],
) -> str:
    """Return the one-line lineage-orientation summary for a feed row (history)."""
    if task.id is None:
        return ""
    stats = lineage_summary_stats(store, task, cache)
    lc = _colors.LINEAGE_COLORS
    body = format_lineage_summary(stats)
    return f"    [{lc.annotation}]lineage: {rich_escape(body)}[/{lc.annotation}]"


def lineage_json_payload(
    store: SqliteTaskStore, task_id: str, full_tree: TaskLineageNode
) -> dict:
    """Build the merge-unit grouped lineage as a JSON-serializable dict."""
    groups = build_merge_unit_group_tree(store, full_tree)

    def _task_dict(t: DbTask) -> dict:
        return {
            "id": t.id,
            "task_type": t.task_type,
            "status": t.status,
            "merge_status": t.merge_status,
            "failure_reason": t.failure_reason,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "branch": t.branch,
            "prompt": (t.prompt or "").split("\n", 1)[0][:200],
        }

    def _group_dict(group: MergeUnitGroup) -> dict:
        task_count, unit_count = group_subtree_counts(group)
        return {
            "key": group.key,
            "merge_unit_id": group.unit.id if group.unit is not None else None,
            "merge_unit_state": group.unit.state if group.unit is not None else None,
            "header": _task_dict(group.header),
            "members": [_task_dict(m) for m in group.members],
            "subtree_task_count": task_count,
            "subtree_merge_unit_count": unit_count,
            "children": [_group_dict(c) for c in group.children],
        }

    return {
        "target": task_id,
        "root": groups[0].header.id if groups else None,
        "lineage": [_group_dict(r) for r in groups],
    }


def lineage_json_text(store: SqliteTaskStore, task_id: str, full_tree: TaskLineageNode) -> str:
    """Return the grouped lineage JSON as an indented string."""
    return json.dumps(lineage_json_payload(store, task_id, full_tree), indent=2)
