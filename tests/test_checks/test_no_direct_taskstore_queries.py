from __future__ import annotations

from pathlib import Path

from checks.no_direct_taskstore_queries import check_file

RULE_ID = "no_direct_taskstore_queries"


def _write(tmp_path: Path, relative_path: str, body: str) -> Path:
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


def test_flags_direct_taskstore_query_methods(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "src/gza/api/v0.py",
        "def get_pending(store):\n"
        "    return store.get_pending()\n",
    )
    violations = check_file(path, RULE_ID)
    assert len(violations) == 1
    assert violations[0].method == "get_pending"


def test_allows_query_service_usage(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "src/gza/api/v0.py",
        "def get_pending(service, query):\n"
        "    return service.run(query)\n",
    )
    assert check_file(path, RULE_ID) == []


def test_flags_cmd_queue_and_cmd_next(tmp_path: Path) -> None:
    queue_path = _write(
        tmp_path,
        "src/gza/cli/watch.py",
        "def cmd_queue(store):\n"
        "    return store.get_pending_pickup()\n",
    )
    next_path = _write(
        tmp_path,
        "src/gza/cli/query.py",
        "def cmd_next(store):\n"
        "    return store.get_pending()\n",
    )
    assert len(check_file(queue_path, RULE_ID)) == 1
    assert len(check_file(next_path, RULE_ID)) == 1


def test_respects_noqa(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "src/gza/api/v0.py",
        "def get_history(store):\n"
        f"    return store.get_history()  # noqa: {RULE_ID}\n",
    )
    assert check_file(path, RULE_ID) == []
