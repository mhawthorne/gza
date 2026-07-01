"""Tests for the ``gza tv`` command."""

import argparse
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from gza.cli import tv as tv_module
from gza.db import Task

from .conftest import invoke_gza, make_store, setup_config


class _FakeLive:
    """Minimal Rich Live stand-in that records render updates."""

    instance: "_FakeLive | None" = None

    def __init__(self, initial_renderable, **_kwargs):
        self.updates = [initial_renderable]

    def __enter__(self):
        type(self).instance = self
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def update(self, renderable):
        self.updates.append(renderable)


def _set_in_progress(task, started_at: datetime) -> None:
    task.status = "in_progress"
    task.started_at = started_at


def _render_task_ids(tasks, _log_paths, _n_lines, **_kwargs):
    return [task.id for task in tasks]


def _header_text(renderable) -> str:
    header = renderable.renderables[0]
    return header.plain


def test_slot_bounds_defaults_to_min_1_max_4():
    assert tv_module._resolve_slot_bounds(None, None, None) == (1, 4)


def test_slot_bounds_n_equivalent_to_fixed_min_max():
    assert tv_module._resolve_slot_bounds(3, None, None) == (3, 3)
    assert tv_module._resolve_slot_bounds(None, 3, 3) == (3, 3)


def test_slot_bounds_rejects_number_with_min_max():
    with pytest.raises(ValueError, match="cannot be combined"):
        tv_module._resolve_slot_bounds(3, 1, None)


def test_slot_bounds_validation_errors():
    with pytest.raises(ValueError, match="--min"):
        tv_module._resolve_slot_bounds(None, 0, None)
    with pytest.raises(ValueError, match="--max"):
        tv_module._resolve_slot_bounds(None, 3, 2)


def test_tv_rejects_n_with_min_max(capsys, tmp_path: Path):
    setup_config(tmp_path)
    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[],
        number=3,
        min_slots=1,
        max_slots=None,
    )
    rc = tv_module.cmd_tv(args)
    captured = capsys.readouterr()
    assert rc == 1
    assert "cannot be combined" in captured.out


def test_next_slot_count_caps_scales_and_floors_with_padding():
    # Cap at max when running exceeds max.
    assert tv_module._next_slot_count(8, 4, 0, min_slots=1, max_slots=4, can_pad_to_min=True) == (4, 0)
    # Scale to running count between min and max.
    assert tv_module._next_slot_count(3, 3, 0, min_slots=1, max_slots=4, can_pad_to_min=True) == (3, 0)
    # Floor at min when running is below min and fallback padding exists.
    assert tv_module._next_slot_count(1, 2, 4, min_slots=3, max_slots=5, can_pad_to_min=True) == (3, 0)


def test_next_slot_count_edge_case_no_finished_padding_uses_running():
    assert tv_module._next_slot_count(2, 2, 0, min_slots=4, max_slots=6, can_pad_to_min=False) == (2, 0)


def test_next_slot_count_hysteresis_grow_immediately_shrink_after_threshold():
    # Grow on first tick.
    assert tv_module._next_slot_count(4, 2, 0, min_slots=1, max_slots=5, can_pad_to_min=True) == (4, 0)

    # Shrink waits until SHRINK_TICKS consecutive lower ticks.
    slots = 4
    ticks_below = 0
    for _ in range(tv_module.SHRINK_TICKS - 1):
        slots, ticks_below = tv_module._next_slot_count(
            2, slots, ticks_below, min_slots=1, max_slots=5, can_pad_to_min=True
        )
        assert slots == 4
    slots, ticks_below = tv_module._next_slot_count(
        2, slots, ticks_below, min_slots=1, max_slots=5, can_pad_to_min=True
    )
    assert (slots, ticks_below) == (2, 0)


def test_tv_auto_mode_repolls_live_tasks(monkeypatch, tmp_path: Path):
    """Auto-select mode keeps slots full; a just-completed task lingers before falling off.

    When task_1 finishes and task_4 starts, task_1 stays visible for LINGER_TICKS
    ticks (displacing task_2 temporarily), then falls off once the window expires.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_1 = store.add("Task one", task_type="implement")
    task_2 = store.add("Task two", task_type="implement")
    task_3 = store.add("Task three", task_type="implement")
    task_4 = store.add("Task four", task_type="implement")
    assert task_1.id and task_2.id and task_3.id and task_4.id

    _set_in_progress(task_1, base)
    _set_in_progress(task_2, base + timedelta(minutes=1))
    task_3.status = "completed"
    task_3.completed_at = base - timedelta(minutes=1)
    task_4.status = "pending"
    store.update(task_1)
    store.update(task_2)
    store.update(task_3)
    store.update(task_4)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            refreshed_1 = store.get(task_1.id)
            refreshed_4 = store.get(task_4.id)
            assert refreshed_1 is not None and refreshed_4 is not None
            refreshed_1.status = "completed"
            refreshed_1.completed_at = base + timedelta(minutes=2)
            _set_in_progress(refreshed_4, base + timedelta(minutes=3))
            store.update(refreshed_1)
            store.update(refreshed_4)
        elif sleep_calls > tv_module.LINGER_TICKS + 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [task_2.id, task_1.id]
    # task_1 just completed: it lingers (displacing task_2) for LINGER_TICKS ticks
    assert _FakeLive.instance.updates[1] == [task_4.id, task_1.id]
    # After the linger window expires, task_2 reclaims its slot
    assert _FakeLive.instance.updates[-1] == [task_4.id, task_2.id]


def test_tv_auto_mode_backfills_finished_tasks_when_live_count_drops(monkeypatch, tmp_path: Path):
    """Auto-select mode should keep N panels by backfilling with recent finished tasks."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    live_1 = store.add("Live one", task_type="implement")
    live_2 = store.add("Live two", task_type="implement")
    finished_1 = store.add("Finished one", task_type="implement")
    finished_2 = store.add("Finished two", task_type="implement")
    assert live_1.id and live_2.id and finished_1.id and finished_2.id

    _set_in_progress(live_1, base)
    _set_in_progress(live_2, base + timedelta(minutes=1))
    finished_1.status = "completed"
    finished_1.completed_at = base - timedelta(minutes=1)
    finished_2.status = "completed"
    finished_2.completed_at = base - timedelta(minutes=2)
    store.update(live_1)
    store.update(live_2)
    store.update(finished_1)
    store.update(finished_2)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            refreshed_1 = store.get(live_1.id)
            assert refreshed_1 is not None
            refreshed_1.status = "completed"
            refreshed_1.completed_at = base + timedelta(minutes=2)
            store.update(refreshed_1)
        else:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=4)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [live_2.id, live_1.id, finished_1.id, finished_2.id]
    assert _FakeLive.instance.updates[1] == [live_2.id, live_1.id, finished_1.id, finished_2.id]


def test_tv_panel_displays_rebase_log_content_for_completed_task(tmp_path: Path):
    """Completed rebase tasks with logs should render content instead of '(no log available)'."""
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("Completed rebase task", task_type="rebase")
    assert task.id is not None
    task.status = "completed"
    task.slug = "20260424-tv-rebase"
    task.log_file = ".gza/logs/20260424-tv-rebase.log"
    store.update(task)

    log_path = tmp_path / ".gza" / "logs" / "20260424-tv-rebase.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text('{\"type\":\"gza\",\"subtype\":\"info\",\"message\":\"mechanical rebase complete\"}\\n')

    panel = tv_module._build_task_panel(task, log_path, n_lines=4, width=100)
    rendered = panel.renderable.plain
    assert "mechanical rebase complete" in rendered
    assert "(no log available)" not in rendered


def test_tv_panel_title_includes_model_when_present(tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task = store.add("TV model task", task_type="implement")
    assert task.id is not None
    task.status = "completed"
    task.model = "claude-opus-4-8"
    store.update(task)

    panel = tv_module._build_task_panel(task, None, n_lines=4, width=100)

    assert panel.title is not None
    assert "claude-opus-4-8" in panel.title.plain


def test_tv_explicit_ids_stay_fixed(monkeypatch, tmp_path: Path):
    """Explicit task IDs should remain on screen and keep polling after completion."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_1 = store.add("Pinned task one", task_type="implement")
    task_2 = store.add("Pinned task two", task_type="implement")
    task_3 = store.add("Replacement candidate", task_type="implement")
    assert task_1.id and task_2.id and task_3.id

    _set_in_progress(task_1, base)
    _set_in_progress(task_2, base + timedelta(minutes=1))
    task_3.status = "pending"
    store.update(task_1)
    store.update(task_2)
    store.update(task_3)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            refreshed_1 = store.get(task_1.id)
            refreshed_3 = store.get(task_3.id)
            assert refreshed_1 is not None and refreshed_3 is not None
            refreshed_1.status = "completed"
            refreshed_1.completed_at = base + timedelta(minutes=2)
            _set_in_progress(refreshed_3, base + timedelta(minutes=3))
            store.update(refreshed_1)
            store.update(refreshed_3)
        elif sleep_calls >= 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[task_1.id, task_2.id], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [task_1.id, task_2.id]
    assert _FakeLive.instance.updates[1] == [task_1.id, task_2.id]
    assert _FakeLive.instance.updates[2] == [task_1.id, task_2.id]
    assert task_3.id not in _FakeLive.instance.updates[1]


def test_tv_auto_mode_starts_with_recent_finished_tasks(monkeypatch, tmp_path: Path):
    """Auto-select mode should fall back to the most recent finished tasks."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    old_task = store.add("Older finished task", task_type="implement")
    new_task = store.add("Newer finished task", task_type="implement")
    assert old_task.id and new_task.id

    old_task.status = "completed"
    old_task.completed_at = base
    new_task.status = "completed"
    new_task.completed_at = base + timedelta(minutes=1)
    store.update(old_task)
    store.update(new_task)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [new_task.id, old_task.id]


def test_tv_auto_mode_promotes_live_tasks_over_finished_fallback(monkeypatch, tmp_path: Path):
    """Auto-select mode should replace fallback finished tasks once live work appears."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    finished_task = store.add("Finished task", task_type="implement")
    live_task = store.add("Appears later", task_type="implement")
    assert finished_task.id and live_task.id

    finished_task.status = "completed"
    finished_task.completed_at = base
    store.update(finished_task)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            refreshed = store.get(live_task.id)
            assert refreshed is not None
            _set_in_progress(refreshed, base)
            store.update(refreshed)
        else:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=1)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [finished_task.id]
    assert _FakeLive.instance.updates[1] == [live_task.id]


def test_tv_auto_mode_linger_keeps_just_completed_task_visible_when_slots_full(monkeypatch, tmp_path: Path):
    """A task that completes while all slots are in_progress lingers for LINGER_TICKS ticks.

    Scenario: 2 slots, both filled with in_progress tasks.  task_1 completes while
    task_3 starts (keeping in_progress count at 2).  Without linger, task_1 would
    be dropped immediately.  With linger, task_1 stays visible for LINGER_TICKS
    ticks (displacing task_2), then falls off.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_1 = store.add("Short review", task_type="implement")
    task_2 = store.add("Long running", task_type="implement")
    task_3 = store.add("New work", task_type="implement")
    assert task_1.id and task_2.id and task_3.id

    _set_in_progress(task_1, base)
    _set_in_progress(task_2, base + timedelta(minutes=1))
    task_3.status = "pending"
    store.update(task_1)
    store.update(task_2)
    store.update(task_3)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            # task_1 completes; task_3 starts — slots remain full
            refreshed_1 = store.get(task_1.id)
            refreshed_3 = store.get(task_3.id)
            assert refreshed_1 is not None and refreshed_3 is not None
            refreshed_1.status = "completed"
            refreshed_1.completed_at = base + timedelta(minutes=2)
            _set_in_progress(refreshed_3, base + timedelta(minutes=3))
            store.update(refreshed_1)
            store.update(refreshed_3)
        elif sleep_calls > tv_module.LINGER_TICKS + 1:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    # Before completion: both in_progress, newest first
    assert _FakeLive.instance.updates[0] == [task_2.id, task_1.id]
    # Immediately after completion: task_1 lingers even though in_progress fills slots
    assert _FakeLive.instance.updates[1] == [task_3.id, task_1.id]
    # All linger ticks retain task_1
    for update in _FakeLive.instance.updates[1 : tv_module.LINGER_TICKS + 1]:
        assert task_1.id in update, f"task_1 should linger but was absent from {update}"
    # After linger expires, task_2 reclaims its slot
    assert _FakeLive.instance.updates[-1] == [task_3.id, task_2.id]


def test_tv_auto_mode_off_screen_completion_is_not_injected_as_linger(monkeypatch, tmp_path: Path):
    """A task that completes while never on-screen must not be injected via linger.

    Scenario: 3 in_progress tasks but only 2 slots.  task_3 (the oldest, lowest
    priority, never displayed) completes between ticks.  The display should continue
    showing task_1 and task_2 and must NOT inject task_3.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_1 = store.add("Task one", task_type="implement")
    task_2 = store.add("Task two", task_type="implement")
    task_3 = store.add("Task three (off-screen)", task_type="implement")
    assert task_1.id and task_2.id and task_3.id

    # All three in_progress; _auto_select_tasks picks newest-first so task_3 is off-screen
    _set_in_progress(task_3, base)
    _set_in_progress(task_2, base + timedelta(minutes=1))
    _set_in_progress(task_1, base + timedelta(minutes=2))
    store.update(task_3)
    store.update(task_2)
    store.update(task_1)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            # task_3 (off-screen) completes; task_1 and task_2 remain running
            refreshed_3 = store.get(task_3.id)
            assert refreshed_3 is not None
            refreshed_3.status = "completed"
            refreshed_3.completed_at = base + timedelta(minutes=3)
            store.update(refreshed_3)
        elif sleep_calls > tv_module.LINGER_TICKS + 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    # Initial display: 2 slots, newest-first → task_1 and task_2 (task_3 off-screen)
    assert _FakeLive.instance.updates[0] == [task_1.id, task_2.id]
    # After task_3 completes off-screen: it must NOT appear in any update
    for update in _FakeLive.instance.updates[1:]:
        assert task_3.id not in update, (
            f"Off-screen task_3 must not be injected via linger, but appeared in: {update}"
        )


def test_tv_auto_mode_linger_preserves_prior_slot_position(monkeypatch, tmp_path: Path):
    """A lingering task stays in its prior slot rather than jumping to the bottom.

    Scenario: 2 slots, task_A at slot 0 (top, newest) and task_B at slot 1.
    task_A completes.  task_C starts.  With position preservation, task_A
    should remain at slot 0 (top) during its linger window, not move to slot 1.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_a = store.add("Task A (newest)", task_type="implement")
    task_b = store.add("Task B (older)", task_type="implement")
    task_c = store.add("Task C (replacement)", task_type="implement")
    assert task_a.id and task_b.id and task_c.id

    # task_A is newest so it ends up at slot 0 after in_progress.reverse()
    _set_in_progress(task_b, base)
    _set_in_progress(task_a, base + timedelta(minutes=1))
    task_c.status = "pending"
    store.update(task_a)
    store.update(task_b)
    store.update(task_c)

    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            # task_A (slot 0) completes; task_C starts as newest in_progress
            refreshed_a = store.get(task_a.id)
            refreshed_c = store.get(task_c.id)
            assert refreshed_a is not None and refreshed_c is not None
            refreshed_a.status = "completed"
            refreshed_a.completed_at = base + timedelta(minutes=2)
            _set_in_progress(refreshed_c, base + timedelta(minutes=3))
            store.update(refreshed_a)
            store.update(refreshed_c)
        elif sleep_calls >= 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=2)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    # Initial: task_A at slot 0 (newest), task_B at slot 1
    assert _FakeLive.instance.updates[0] == [task_a.id, task_b.id]
    # After task_A completes: task_A lingers at slot 0 (preserved), task_C fills slot 1
    assert _FakeLive.instance.updates[1] == [task_a.id, task_c.id], (
        "Linger task should preserve its prior slot (0/top), not jump to bottom"
    )


def test_auto_select_tasks_linger_preserves_prior_slot_position(tmp_path: Path):
    """_auto_select_tasks places lingering tasks at their prior slot when prev_tasks is given."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_a = store.add("Task A (slot 0)", task_type="implement")
    task_b = store.add("Task B (slot 1)", task_type="implement")
    task_c = store.add("Task C (replacement)", task_type="implement")
    assert task_a.id and task_b.id and task_c.id

    # task_A completed; task_B and task_C are in_progress
    task_a.status = "completed"
    task_a.completed_at = base + timedelta(minutes=2)
    _set_in_progress(task_b, base)
    _set_in_progress(task_c, base + timedelta(minutes=3))
    store.update(task_a)
    store.update(task_b)
    store.update(task_c)

    # prev_tasks: task_A was at slot 0 (top), task_B was at slot 1
    prev = [task_a, task_b]

    result = tv_module._auto_select_tasks(
        store, max_slots=2, linger_ids={task_a.id}, prev_tasks=prev
    )

    assert len(result) == 2
    # task_A should be preserved at position 0, not moved to the bottom
    assert result[0].id == task_a.id, "Linger task_A should remain at slot 0 (prior position)"
    assert result[1].id == task_c.id, "Newest in_progress task_C should fill slot 1"


def test_auto_select_tasks_caps_result_at_max_slots_when_linger_exceeds_budget(tmp_path: Path):
    """_auto_select_tasks must never return more than max_slots items.

    When more tasks complete within the linger window than there are display
    slots, only the most-recently-completed ones (up to max_slots) are kept so
    that _lines_per_panel's height budget is never exceeded.
    """
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task_a = store.add("Task A", task_type="implement")
    task_b = store.add("Task B", task_type="implement")
    task_c = store.add("Task C", task_type="implement")
    assert task_a.id and task_b.id and task_c.id

    # All three tasks are completed (no in_progress tasks at all)
    task_a.status = "completed"
    task_a.completed_at = base + timedelta(minutes=1)
    task_b.status = "completed"
    task_b.completed_at = base + timedelta(minutes=2)
    task_c.status = "completed"
    task_c.completed_at = base + timedelta(minutes=3)
    store.update(task_a)
    store.update(task_b)
    store.update(task_c)

    # All three IDs are lingering simultaneously
    linger_ids = {task_a.id, task_b.id, task_c.id}

    result = tv_module._auto_select_tasks(store, max_slots=2, linger_ids=linger_ids)

    assert len(result) <= 2, f"Expected at most 2 results, got {len(result)}: {[t.id for t in result]}"
    # The two most-recently-completed tasks (C then B) should be preferred
    result_ids = [t.id for t in result]
    assert task_c.id in result_ids, "Most-recently-completed task_c should be selected"
    assert task_b.id in result_ids, "Second-most-recently-completed task_b should be selected"
    assert task_a.id not in result_ids, "Oldest linger task_a should be dropped"


def test_tv_auto_mode_does_not_exit_on_finished_fallback(monkeypatch, tmp_path: Path):
    """Auto-select mode should keep polling when only finished fallback tasks are showing."""
    setup_config(tmp_path)
    store = make_store(tmp_path)

    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    finished_task = store.add("Finished task", task_type="implement")
    assert finished_task.id
    finished_task.status = "completed"
    finished_task.completed_at = base
    store.update(finished_task)

    sleep_calls = 0

    def fake_sleep(seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 3:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_render_all", _render_task_ids)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=1)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [finished_task.id]
    assert _FakeLive.instance.updates[1] == [finished_task.id]
    assert _FakeLive.instance.updates[2] == [finished_task.id]


def test_tv_header_explicit_mode_does_not_claim_min_max(monkeypatch, tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    created = [store.add(f"Pinned task {idx}", task_type="implement") for idx in range(1, 6)]
    task_ids = [task.id for task in created]
    assert all(task_ids)

    def fake_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=task_ids, number=None, min_slots=None, max_slots=None)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    header = _header_text(_FakeLive.instance.updates[0])
    assert "slots: 5 (explicit)" in header
    assert "min" not in header
    assert "max" not in header


def test_tv_header_explicit_mode_running_count_is_global(monkeypatch, tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    pinned_done = store.add("Pinned done", task_type="implement")
    pinned_pending = store.add("Pinned pending", task_type="implement")
    unrelated_running = store.add("Unrelated running", task_type="implement")
    assert pinned_done.id and pinned_pending.id and unrelated_running.id

    pinned_done.status = "completed"
    store.update(pinned_done)
    store.update(pinned_pending)
    _set_in_progress(unrelated_running, datetime(2026, 4, 15, 12, 0, tzinfo=UTC))
    store.update(unrelated_running)

    def fake_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(
        project_dir=tmp_path,
        task_ids=[pinned_done.id, pinned_pending.id],
        number=None,
        min_slots=None,
        max_slots=None,
    )
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    header = _header_text(_FakeLive.instance.updates[0])
    assert "Running: 1" in header
    assert "slots: 2 (explicit)" in header


def test_tv_header_auto_mode_uses_visible_panel_count_when_slots_are_sparse(monkeypatch, tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    task_1 = store.add("Task one", task_type="implement")
    task_2 = store.add("Task two", task_type="implement")
    assert task_1.id and task_2.id
    task_1.status = "completed"
    task_2.status = "completed"
    store.update(task_1)
    store.update(task_2)

    def fake_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=4, min_slots=None, max_slots=None)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    header = _header_text(_FakeLive.instance.updates[0])
    assert "slots: 2 (min 4, max 4)" in header


def test_tv_parser_path_renders_header_for_fixed_slot_flag(monkeypatch, tmp_path: Path):
    setup_config(tmp_path)
    store = make_store(tmp_path)
    in_progress = store.add("Running task", task_type="implement")
    fallback_done = store.add("Fallback done", task_type="implement")
    assert in_progress.id and fallback_done.id

    _set_in_progress(in_progress, datetime(2026, 4, 15, 12, 0, tzinfo=UTC))
    fallback_done.status = "completed"
    store.update(in_progress)
    store.update(fallback_done)

    def fake_sleep(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    result = invoke_gza("tv", "-n", "2", "--project", str(tmp_path))

    assert result.returncode == 0
    assert _FakeLive.instance is not None
    header = _header_text(_FakeLive.instance.updates[0])
    assert "Running: 1" in header
    assert "slots: 2 (min 2, max 2)" in header


def test_lines_per_panel_fits_within_height_budget(monkeypatch):
    # 95% of 20 rows = 19; minus the 1 header row = 18 split across 2 panels,
    # each costing 2 border rows: (18 - 4) // 2 = 7 content lines.
    monkeypatch.setattr(tv_module, "_get_terminal_size", lambda: os.terminal_size((120, 20)))
    n_lines = tv_module._lines_per_panel(2)
    assert n_lines == 7
    # The full render must fit within the budget so screen=True never crops.
    total = tv_module.HEADER_LINES + 2 * (n_lines + 2)
    assert total <= 20 * tv_module.HEIGHT_PERCENT // 100


def test_task_elapsed_in_progress_ticks_against_wall_clock():
    started = datetime.now(UTC) - timedelta(seconds=30)
    task = Task(id="gza-1", prompt="live", status="in_progress", started_at=started)
    elapsed = tv_module._task_elapsed_seconds(task)
    assert elapsed is not None and elapsed >= 30


def test_task_elapsed_failed_freezes_at_completion_not_wall_clock():
    """A failed task with no recorded duration must not keep incrementing."""
    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task = Task(
        id="gza-2",
        prompt="failed",
        status="failed",
        started_at=base,
        completed_at=base + timedelta(seconds=42),
        duration_seconds=None,
    )
    assert tv_module._task_elapsed_seconds(task) == 42.0


def test_task_elapsed_prefers_recorded_duration():
    base = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
    task = Task(
        id="gza-3",
        prompt="done",
        status="completed",
        started_at=base,
        completed_at=base + timedelta(seconds=99),
        duration_seconds=12.5,
    )
    assert tv_module._task_elapsed_seconds(task) == 12.5


def test_task_elapsed_finished_without_timing_is_none():
    task = Task(id="gza-4", prompt="failed", status="failed", started_at=None)
    assert tv_module._task_elapsed_seconds(task) is None


def _explicit_id_args(tmp_path: Path, task_id: str) -> argparse.Namespace:
    return argparse.Namespace(project_dir=tmp_path, task_ids=[task_id], number=None, min_slots=None, max_slots=None)


def test_cmd_tv_skips_refresh_for_displayed_task_without_id(monkeypatch, tmp_path: Path) -> None:
    """Refresh loop should not call store.get(None) when a displayed task has no ID."""
    task = Task(id=None, prompt="Task without id", status="completed")
    store = MagicMock()
    store.get = MagicMock(return_value=task)

    monkeypatch.setattr(tv_module.Config, "load", lambda _project_dir: SimpleNamespace(workers_path=tmp_path / "workers"))
    monkeypatch.setattr(tv_module, "get_store", lambda _config: store)
    monkeypatch.setattr(tv_module, "WorkerRegistry", lambda _path: MagicMock())
    monkeypatch.setattr(tv_module, "resolve_id", lambda _store, _raw_id: "gza-1")
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module, "_render_all", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_sleep", lambda _seconds: (_ for _ in ()).throw(KeyboardInterrupt))

    rc = tv_module.cmd_tv(_explicit_id_args(tmp_path, "gza-1"))

    assert rc == 0
    assert store.get.call_args_list == [call("gza-1")]


def test_cmd_tv_reresolves_missing_log_path_during_refresh(monkeypatch, tmp_path: Path) -> None:
    """Refresh loop should re-resolve log path when previously discovered path is missing."""
    task = Task(id="gza-2", prompt="Task with missing log", status="completed")
    missing_log = tmp_path / "missing.log"
    store = MagicMock()
    store.get = MagicMock(side_effect=[task, task])
    resolve_log = MagicMock(side_effect=[(missing_log, None), (None, None)])
    sleep_calls = 0

    def fake_sleep(_seconds: float) -> None:
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr(tv_module.Config, "load", lambda _project_dir: SimpleNamespace(workers_path=tmp_path / "workers"))
    monkeypatch.setattr(tv_module, "get_store", lambda _config: store)
    monkeypatch.setattr(tv_module, "WorkerRegistry", lambda _path: MagicMock())
    monkeypatch.setattr(tv_module, "resolve_id", lambda _store, _raw_id: "gza-2")
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", resolve_log)
    monkeypatch.setattr(tv_module, "_render_all", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(tv_module, "Live", _FakeLive)
    monkeypatch.setattr(tv_module, "_sleep", fake_sleep)

    rc = tv_module.cmd_tv(_explicit_id_args(tmp_path, "gza-2"))

    assert rc == 0
    assert resolve_log.call_count == 2
    assert store.get.call_args_list == [call("gza-2"), call("gza-2")]
