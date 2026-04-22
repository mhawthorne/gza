"""Run all custom repo checks.

Rules auto-register: any module under ``checks/`` that exports a ``check_file``
callable is picked up. The module's basename is used as the rule id (e.g.
``checks/no_stdlib_monkeypatch.py`` → ``no_stdlib_monkeypatch``). Prefix a module
name with ``_`` to mark it as a helper and exclude it from discovery.

Rule contract:
    check_file(path: Path, rule_id: str) -> list[Violation]     # required
    DEFAULT_PATHS: list[Path]                                   # optional

where ``Violation`` is any object with a ``.format() -> str`` method. The runner
owns file enumeration and per-file reporting.

Usage:
    uv run python -m checks                     # each rule scans its DEFAULT_PATHS
    uv run python -m checks <path> [<path>...]  # override: run every rule against these
    uv run python -m checks --rule <id>         # restrict to one rule (repeatable)
    uv run python -m checks -v                  # print PASSED/FAILED per file
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import sys
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from types import ModuleType

import checks

CheckFn = Callable[[Path, str], list]


class Rule:
    __slots__ = ("id", "check_file", "default_paths")

    def __init__(self, rule_id: str, check_file: CheckFn, default_paths: list[Path]) -> None:
        self.id = rule_id
        self.check_file = check_file
        self.default_paths = default_paths


def discover_rules() -> dict[str, Rule]:
    rules: dict[str, Rule] = {}
    for info in pkgutil.iter_modules(checks.__path__):
        if info.name.startswith("_") or info.name == "__main__":
            continue
        module: ModuleType = importlib.import_module(f"checks.{info.name}")
        check_file = getattr(module, "check_file", None)
        if not callable(check_file):
            continue
        defaults = getattr(module, "DEFAULT_PATHS", None) or []
        rules[info.name] = Rule(info.name, check_file, list(defaults))
    return rules


def iter_py_files(paths: list[Path]) -> Iterable[Path]:
    for root in paths:
        if root.is_file() and root.suffix == ".py":
            yield root
        elif root.is_dir():
            yield from sorted(root.rglob("*.py"))


def main(argv: list[str] | None = None) -> int:
    rules = discover_rules()

    parser = argparse.ArgumentParser(prog="checks")
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Override paths to scan. If omitted, each rule uses its DEFAULT_PATHS.",
    )
    parser.add_argument(
        "--rule",
        action="append",
        choices=sorted(rules),
        help="Run only the given rule(s); repeatable. Default: all.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print every (file, rule) with PASSED/FAILED instead of dots.",
    )
    args = parser.parse_args(argv)
    selected = args.rule or sorted(rules)

    started = time.perf_counter()
    total_files = 0
    total_failed = 0
    total_violations = 0
    deferred_output: list[str] = []

    for rule_id in selected:
        rule = rules[rule_id]
        paths = args.paths or rule.default_paths
        if not paths:
            parser.error(f"rule {rule_id!r} has no DEFAULT_PATHS; pass paths explicitly")

        for path in iter_py_files(paths):
            total_files += 1
            violations = rule.check_file(path, rule_id)
            failed = bool(violations)
            if failed:
                total_failed += 1
                total_violations += len(violations)

            if args.verbose:
                status = "FAILED" if failed else "PASSED"
                print(f"{path}::{rule_id} {status}")
                for v in violations:
                    print(f"  {v.format()}")
            else:
                sys.stdout.write("F" if failed else ".")
                sys.stdout.flush()
                if failed:
                    deferred_output.extend(v.format() for v in violations)

    if not args.verbose:
        sys.stdout.write("\n")
        for line in deferred_output:
            print(line)

    elapsed = time.perf_counter() - started
    passed = total_files - total_failed
    summary = f"{passed} passed, {total_failed} failed in {elapsed:.2f}s"
    if total_violations:
        summary += f" ({total_violations} violation(s))"
    print(f"\n{summary}")

    return 1 if total_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
