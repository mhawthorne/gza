"""Rank tests as cull candidates by cost, arc redundancy, and behaviour risk.

``test_redundancy`` answers "how much of the suite is redundant". This answers
"which specific tests should go first, and which must not go at all".

Three inputs are combined:

* per-test wall time, from a timing run (this module can produce one)
* per-test branch arcs, from ``pytest --cov-branch --cov-context=test``
* ``specs/behavior/`` vocabulary, to flag removals that would leave a specified
  behaviour with no test naming it

Two filters separate a safe list from a naive one. Neither is visible to
coverage alone:

* **Joint safety.** "Adds zero unique arcs" only holds one test at a time -- two
  tests that are each other's sole backup for an arc both report zero unique.
  A greedy set cover instead names a subset that reproduces every arc together,
  so everything outside it can go as a group.
* **Behaviour risk.** Coverage cannot tell ``[case-a]`` from ``[case-b]``: same
  lines, different expected outcome. Dropping one while keeping its sibling
  loses a real assertion, so those are held back, as are tests carrying a spec
  term no surviving test carries.

Usage:
    bin/test-cull-candidates --run -- tests/ -q      # time the suite, then rank
    bin/test-cull-candidates --coverage-file .coverage --timings timings.json
    bin/test-cull-candidates --json
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

DEFAULT_PACKAGE_MARKER = "/src/gza/"
DEFAULT_SPEC_DIR = Path("specs/behavior")
# A term must appear this often in the specs before its absence means anything;
# below it, one-off prose words dominate and every list turns into noise.
SPEC_TERM_MIN_USES = 5
_PARAM_SUFFIX = re.compile(r"\[.*\]$")


def _base(node_id: str) -> str:
    """Strip a parametrisation suffix, leaving the shared test function."""
    return _PARAM_SUFFIX.sub("", node_id)


def _normalise_context(context: str) -> str:
    """Fold pytest-cov's ``|setup`` / ``|call`` / ``|teardown`` into one test id."""
    return context.split("|", 1)[0]


@dataclass
class CullReport:
    total_tests: int
    total_seconds: float
    cover_keeps: int
    droppable: list[tuple[float, str]]
    held_variant: list[tuple[float, str]] = field(default_factory=list)
    held_spec_term: list[tuple[float, str]] = field(default_factory=list)
    orphan_terms: dict[str, int] = field(default_factory=dict)

    @property
    def droppable_seconds(self) -> float:
        return sum(seconds for seconds, _ in self.droppable)


# --- pytest plugin -----------------------------------------------------------
# Loaded as ``-p gza.test_cull`` by the timing run below. Only active when
# TIMINGS_ENV names an output path, so importing this module is otherwise inert.
TIMINGS_ENV = "GZA_TEST_CULL_TIMINGS"

_records: list[tuple[str, float]] = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):  # noqa: ANN001, ANN201 - pytest hook
    if not os.environ.get(TIMINGS_ENV):
        yield
        return
    start = time.perf_counter()
    yield
    _records.append((item.nodeid, time.perf_counter() - start))


def pytest_sessionfinish(session, exitstatus):  # noqa: ANN001, ANN201 - pytest hook
    target = os.environ.get(TIMINGS_ENV)
    if target and _records:
        Path(target).write_text(json.dumps(_records), encoding="utf-8")


def run_timing(pytest_args: list[str], output: Path) -> int:
    """Run the suite single-process and write ``[[node_id, seconds], ...]``.

    Single-process on purpose: under xdist each worker loads its own copy of the
    plugin and would overwrite the others' results at session finish.
    """
    command = [
        sys.executable,
        "-m",
        "pytest",
        *pytest_args,
        "-p",
        "no:xdist",
        "-p",
        "no:randomly",
        "-p",
        "gza.test_cull",
    ]
    env = {**os.environ, TIMINGS_ENV: str(output)}
    print(f"timing run: {' '.join(command)}", file=sys.stderr)
    return subprocess.run(command, check=False, env=env).returncode


def load_timings(path: Path) -> dict[str, float]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    return {node_id: float(seconds) for node_id, seconds, *_ in raw}


def load_arcs_by_test(
    coverage_path: Path, package_marker: str
) -> dict[str, set[tuple[str, tuple[int, int]]]]:
    from gza.test_redundancy import load_arcs_by_context

    by_test: dict[str, set] = collections.defaultdict(set)
    for context, arcs in load_arcs_by_context(coverage_path, package_marker).items():
        by_test[_normalise_context(context)].update(arcs)
    return dict(by_test)


def spec_terms(spec_dir: Path, min_uses: int = SPEC_TERM_MIN_USES) -> set[str]:
    counts: collections.Counter[str] = collections.Counter()
    for path in sorted(spec_dir.glob("*.md")):
        for word in re.findall(r"[a-z][a-z_-]{5,}", path.read_text(encoding="utf-8").lower()):
            counts[word.replace("-", "_")] += 1
    return {word for word, count in counts.items() if count >= min_uses}


def build_report(
    timings: dict[str, float],
    arcs_by_test: dict[str, set],
    terms: set[str],
) -> CullReport:
    from gza.test_redundancy import greedy_cover

    keep_ids = set(greedy_cover(arcs_by_test, ())[0])
    droppable_ids = {node_id for node_id in timings if node_id not in keep_ids}
    surviving = set(timings) - droppable_ids

    def terms_of(node_id: str) -> set[str]:
        name = node_id.split("::")[-1].lower()
        return {term for term in terms if term in name}

    surviving_terms: set[str] = set()
    for node_id in surviving:
        surviving_terms |= terms_of(node_id)
    orphaned = {term for node_id in droppable_ids for term in terms_of(node_id)} - surviving_terms

    surviving_bases = {_base(node_id) for node_id in surviving}

    droppable: list[tuple[float, str]] = []
    held_variant: list[tuple[float, str]] = []
    held_spec_term: list[tuple[float, str]] = []
    orphan_counts: collections.Counter[str] = collections.Counter()

    for node_id in sorted(droppable_ids, key=lambda n: -timings[n]):
        row = (timings[node_id], node_id)
        if "[" in node_id and _base(node_id) in surviving_bases:
            held_variant.append(row)
            continue
        hit = terms_of(node_id) & orphaned
        if hit:
            held_spec_term.append(row)
            orphan_counts.update(hit)
            continue
        droppable.append(row)

    return CullReport(
        total_tests=len(timings),
        total_seconds=sum(timings.values()),
        cover_keeps=len(keep_ids),
        droppable=droppable,
        held_variant=held_variant,
        held_spec_term=held_spec_term,
        orphan_terms=dict(orphan_counts.most_common()),
    )


def render(report: CullReport) -> str:
    total = report.total_seconds or 1.0
    lines = [
        "# Test cull candidates",
        f"- tests measured: {report.total_tests}",
        f"- suite wall time: {report.total_seconds:.1f}s",
        f"- coverage-equivalent subset kept: {report.cover_keeps}",
        f"- held back (kept-sibling variant): {len(report.held_variant)}"
        f" ({sum(s for s, _ in report.held_variant):.1f}s)",
        f"- held back (orphans a spec term): {len(report.held_spec_term)}"
        f" ({sum(s for s, _ in report.held_spec_term):.1f}s)",
        f"- **candidates: {len(report.droppable)}"
        f" ({report.droppable_seconds:.1f}s, {100 * report.droppable_seconds / total:.0f}% of runtime)**",
        "",
        "## Cumulative saving",
    ]
    running = 0.0
    marks = {50, 100, 250, 500, 1000, 2000, len(report.droppable)}
    for index, (seconds, _node_id) in enumerate(report.droppable, 1):
        running += seconds
        if index in marks:
            lines.append(f"- drop {index} -> save {running:.1f}s ({100 * running / total:.1f}%)")

    by_file: dict[str, list[float]] = collections.defaultdict(list)
    for seconds, node_id in report.droppable:
        by_file[node_id.split("::")[0]].append(seconds)
    lines += ["", "## Candidates by file"]
    for path, values in sorted(by_file.items(), key=lambda kv: -sum(kv[1]))[:15]:
        lines.append(f"- {sum(values):.1f}s  {len(values)} tests  {path}")

    if report.orphan_terms:
        lines += ["", "## Spec terms that held tests back"]
        for term, count in list(report.orphan_terms.items())[:15]:
            lines.append(f"- `{term}`: {count} tests")

    lines += ["", "## Top candidates"]
    for seconds, node_id in report.droppable[:25]:
        lines.append(f"- {seconds * 1000:.0f}ms  {node_id}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--coverage-file", type=Path, default=Path(".coverage"))
    parser.add_argument("--timings", type=Path, default=Path("test-timings.json"))
    parser.add_argument("--spec-dir", type=Path, default=DEFAULT_SPEC_DIR)
    parser.add_argument("--package-marker", default=DEFAULT_PACKAGE_MARKER)
    parser.add_argument("--run", action="store_true", help="produce the timing file first")
    parser.add_argument("-o", "--output", type=Path, help="write the markdown report here")
    parser.add_argument("--tsv", type=Path, help="write candidates as seconds<TAB>node_id")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    pytest_args = [a for a in args.pytest_args if a != "--"]
    if args.run:
        rc = run_timing(pytest_args or ["tests/"], args.timings)
        if rc != 0:
            print(f"timing run exited {rc}; rankings may be partial", file=sys.stderr)

    if not args.timings.exists():
        parser.error(f"{args.timings} not found; pass --run or --timings")
    if not args.coverage_file.exists():
        parser.error(
            f"{args.coverage_file} not found; run pytest with "
            "--cov=gza --cov-branch --cov-context=test first"
        )

    report = build_report(
        load_timings(args.timings),
        load_arcs_by_test(args.coverage_file, args.package_marker),
        spec_terms(args.spec_dir) if args.spec_dir.is_dir() else set(),
    )

    if args.tsv:
        with args.tsv.open("w", encoding="utf-8") as handle:
            handle.write("seconds\tnode_id\n")
            for seconds, node_id in report.droppable:
                handle.write(f"{seconds:.3f}\t{node_id}\n")

    if args.json:
        print(
            json.dumps(
                {
                    "total_tests": report.total_tests,
                    "total_seconds": report.total_seconds,
                    "cover_keeps": report.cover_keeps,
                    "candidates": [{"seconds": s, "node_id": n} for s, n in report.droppable],
                    "held_variant": [{"seconds": s, "node_id": n} for s, n in report.held_variant],
                    "held_spec_term": [{"seconds": s, "node_id": n} for s, n in report.held_spec_term],
                    "orphan_terms": report.orphan_terms,
                },
                indent=2,
            )
        )
        return 0

    text = render(report)
    if args.output:
        args.output.write_text(text, encoding="utf-8")
        print(f"wrote {args.output}", file=sys.stderr)
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
