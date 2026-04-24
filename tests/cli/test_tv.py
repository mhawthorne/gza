"""Tests for the ``gza tv`` command."""

import argparse
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gza.cli import tv as tv_module

from .conftest import make_store, run_gza, setup_config


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
    """Auto-select mode should keep slots full as tasks finish and new ones start."""
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
        else:
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
    assert _FakeLive.instance.updates[1] == [task_4.id, task_2.id]


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

    result = run_gza("tv", "-n", "2", "--project", str(tmp_path))

    assert result.returncode == 0
    assert _FakeLive.instance is not None
    header = _header_text(_FakeLive.instance.updates[0])
    assert "Running: 1" in header
    assert "slots: 2 (min 2, max 2)" in header


def test_lines_per_panel_reserves_header_row(monkeypatch):
    monkeypatch.setattr(tv_module, "_get_terminal_size", lambda: os.terminal_size((120, 20)))
    assert tv_module._lines_per_panel(2) == 6
