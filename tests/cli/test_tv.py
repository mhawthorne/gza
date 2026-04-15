"""Tests for the ``gza tv`` command."""

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

from gza.cli import tv as tv_module

from .conftest import make_store, setup_config


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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=4)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [live_2.id, live_1.id, finished_1.id, finished_2.id]
    assert _FakeLive.instance.updates[1] == [live_2.id, live_1.id, finished_1.id, finished_2.id]


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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

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
    monkeypatch.setattr(tv_module, "_render_all", lambda tasks, _log_paths, _n_lines: [task.id for task in tasks])
    monkeypatch.setattr(tv_module, "_resolve_task_log_path", lambda *_args, **_kwargs: (None, None))
    monkeypatch.setattr(tv_module.time, "sleep", fake_sleep)

    args = argparse.Namespace(project_dir=tmp_path, task_ids=[], number=1)
    rc = tv_module.cmd_tv(args)

    assert rc == 0
    assert _FakeLive.instance is not None
    assert _FakeLive.instance.updates[0] == [finished_task.id]
    assert _FakeLive.instance.updates[1] == [finished_task.id]
    assert _FakeLive.instance.updates[2] == [finished_task.id]
