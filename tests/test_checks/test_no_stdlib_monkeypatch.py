from __future__ import annotations

from pathlib import Path

import pytest

from checks.no_stdlib_monkeypatch import check_file

RULE_ID = "no_stdlib_monkeypatch"


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "sample_test.py"
    p.write_text("import os\nimport subprocess\nimport sys\nfrom gza.cli import tv as tv_module\n\n" + body)
    return p


@pytest.mark.parametrize(
    "snippet,expected_target",
    [
        ('def test_x(monkeypatch):\n    monkeypatch.setattr(subprocess, "run", lambda *a, **k: None)\n', "subprocess"),
        ('def test_x(monkeypatch):\n    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)\n', "sys.stdout"),
        ('def test_x(monkeypatch):\n    monkeypatch.setattr(tv_module.os, "get_terminal_size", lambda *_: None)\n', "tv_module.os"),
        ('def test_x(monkeypatch):\n    monkeypatch.setattr(tv_module.time, "sleep", lambda *_: None)\n', "tv_module.time"),
    ],
)
def test_flags_stdlib_targets(tmp_path: Path, snippet: str, expected_target: str) -> None:
    path = _write(tmp_path, snippet)
    violations = check_file(path, RULE_ID)
    assert len(violations) == 1
    assert violations[0].target == expected_target
    assert violations[0].rule == RULE_ID


@pytest.mark.parametrize(
    "snippet",
    [
        'def test_x(monkeypatch):\n    monkeypatch.setattr(tv_module, "Live", object)\n',
        'def test_x(monkeypatch):\n    monkeypatch.setattr(tv_module, "_render_all", lambda *_: None)\n',
        'def test_x(monkeypatch):\n    monkeypatch.setenv("FOO", "bar")\n',
        'def test_x(monkeypatch):\n    monkeypatch.chdir("/tmp")\n',
    ],
)
def test_allows_safe_patterns(tmp_path: Path, snippet: str) -> None:
    path = _write(tmp_path, snippet)
    assert check_file(path, RULE_ID) == []


def test_respects_noqa(tmp_path: Path) -> None:
    snippet = (
        "def test_x(monkeypatch):\n"
        f'    monkeypatch.setattr(subprocess, "run", lambda *a, **k: None)  # noqa: {RULE_ID}\n'
    )
    path = _write(tmp_path, snippet)
    assert check_file(path, RULE_ID) == []
