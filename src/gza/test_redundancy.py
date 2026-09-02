"""Measure how much of the test suite covers nothing another test already covers.

Runs pytest under branch coverage with one context per test, then reports:

* how many tests contribute no arc that no other test reaches,
* a greedy set-cover subset that reproduces the suite's entire arc coverage,
* what that subset costs, in CPU-seconds, against the full suite.

The subset answers "how much redundancy is there", not "which test to delete".
Greedy cover is biased toward broad tests (its first pick is whatever touches
the most code) and its choice among equally-redundant tests is arbitrary, so
treat the output as a measurement and review candidates by hand.
"""

from __future__ import annotations

import argparse
import collections
import heapq
import json
import re
import statistics
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

_DURATION_LINE = re.compile(r"^(\d+\.\d+)s\s+(?:call|setup|teardown)\s+(\S+)")

# pytest-cov labels contexts "<nodeid>|<phase>"; the phase suffix is dropped so
# setup/call/teardown of one test collapse to the single node it belongs to.
_CONTEXT_PHASE_SUFFIX = "|"


@dataclass
class RedundancyReport:
    """Arc-redundancy measurements for one suite run."""

    tests: int
    arcs: int
    zero_unique_tests: int
    median_unique_arcs: float
    subset: list[str]
    coverage_curve: list[tuple[int, float]] = field(default_factory=list)
    subset_cpu_seconds: float | None = None
    total_cpu_seconds: float | None = None

    @property
    def subset_percent(self) -> float:
        return self.subset_size * 100 / self.tests if self.tests else 0.0

    @property
    def subset_size(self) -> int:
        return len(self.subset)

    def as_dict(self) -> dict[str, object]:
        return {
            "tests": self.tests,
            "arcs": self.arcs,
            "zero_unique_tests": self.zero_unique_tests,
            "median_unique_arcs": self.median_unique_arcs,
            "subset_size": self.subset_size,
            "subset_percent": round(self.subset_percent, 1),
            "subset_cpu_seconds": self.subset_cpu_seconds,
            "total_cpu_seconds": self.total_cpu_seconds,
            "coverage_curve": [{"tests": n, "percent": round(p, 1)} for n, p in self.coverage_curve],
            "subset": self.subset,
        }


def _context_node(context: str) -> str:
    return context.split(_CONTEXT_PHASE_SUFFIX, 1)[0]


def load_arcs_by_context(coverage_path: Path, package_marker: str) -> dict[str, set[tuple[str, tuple[int, int]]]]:
    """Return every measured context mapped to the arcs it covered."""
    import coverage

    data = coverage.CoverageData(str(coverage_path))
    data.read()
    if not data.has_arcs():
        raise SystemExit(
            f"{coverage_path} holds no branch data. Re-run the suite with --cov-branch."
        )
    files = [f for f in data.measured_files() if package_marker in f]
    if not files:
        raise SystemExit(f"No measured files matched {package_marker!r} in {coverage_path}.")

    arcs_by_context: dict[str, set[tuple[str, tuple[int, int]]]] = {}
    for context in data.measured_contexts():
        if not context:
            continue
        data.set_query_context(context)
        covered: set[tuple[str, tuple[int, int]]] = set()
        for path in files:
            arcs = data.arcs(path)
            if arcs:
                covered.update((path, arc) for arc in arcs)
        arcs_by_context[context] = covered
    return arcs_by_context


def greedy_cover(
    arcs_by_context: dict[str, set[tuple[str, tuple[int, int]]]],
    curve_marks: tuple[int, ...],
) -> tuple[list[str], list[tuple[int, float]]]:
    """Pick tests that together reproduce the suite's whole arc coverage.

    Lazy greedy: the heap holds each test keyed by its last known gain, and a
    popped entry is re-pushed when its cached gain is stale. That avoids
    rescoring every candidate on every pick, which is what makes this run in
    seconds rather than minutes on a suite of this size.
    """
    ids = list(arcs_by_context)
    sets = [arcs_by_context[c] for c in ids]
    if not sets:
        return [], []
    total = set().union(*sets)
    remaining = set(total)
    heap = [(-len(s), i) for i, s in enumerate(sets)]
    heapq.heapify(heap)

    chosen: list[str] = []
    accumulated: set[tuple[str, tuple[int, int]]] = set()
    curve: list[tuple[int, float]] = []
    marks = set(curve_marks)
    while remaining and heap:
        negative_gain, index = heapq.heappop(heap)
        gain = len(sets[index] & remaining)
        if gain == 0:
            continue
        if -negative_gain != gain:
            heapq.heappush(heap, (-gain, index))
            continue
        chosen.append(ids[index])
        remaining -= sets[index]
        accumulated |= sets[index]
        if len(chosen) in marks:
            curve.append((len(chosen), len(accumulated) * 100 / len(total)))
    return chosen, curve


def load_durations(durations_path: Path | None) -> dict[str, float]:
    """Parse per-test seconds out of a pytest --durations=0 transcript."""
    totals: dict[str, float] = collections.defaultdict(float)
    if durations_path is None:
        return totals
    for line in durations_path.read_text(errors="replace").splitlines():
        match = _DURATION_LINE.match(line.strip())
        if match:
            totals[match.group(2)] += float(match.group(1))
    return totals


def build_report(
    arcs_by_context: dict[str, set[tuple[str, tuple[int, int]]]],
    durations: dict[str, float],
    curve_marks: tuple[int, ...],
) -> RedundancyReport:
    sets = list(arcs_by_context.values())
    total_arcs = set().union(*sets) if sets else set()

    owners: collections.Counter[tuple[str, tuple[int, int]]] = collections.Counter()
    for covered in sets:
        for arc in covered:
            owners[arc] += 1
    unique_counts = [sum(1 for arc in covered if owners[arc] == 1) for covered in sets]

    subset, curve = greedy_cover(arcs_by_context, curve_marks)

    subset_cpu = total_cpu = None
    if durations:
        kept_nodes = {_context_node(c) for c in subset}
        total_cpu = round(sum(durations.values()), 1)
        subset_cpu = round(sum(v for node, v in durations.items() if node in kept_nodes), 1)

    return RedundancyReport(
        tests=len(arcs_by_context),
        arcs=len(total_arcs),
        zero_unique_tests=sum(1 for count in unique_counts if count == 0),
        median_unique_arcs=float(statistics.median(unique_counts)) if unique_counts else 0.0,
        subset=subset,
        coverage_curve=curve,
        subset_cpu_seconds=subset_cpu,
        total_cpu_seconds=total_cpu,
    )


def render(report: RedundancyReport) -> str:
    lines = [
        "# Test redundancy",
        "",
        f"- tests measured: {report.tests}",
        f"- branch arcs covered: {report.arcs}",
        f"- tests adding zero unique arcs: {report.zero_unique_tests} "
        f"({report.zero_unique_tests * 100 // report.tests if report.tests else 0}%)",
        f"- median unique arcs per test: {report.median_unique_arcs:.0f}",
        f"- coverage-equivalent subset: {report.subset_size} tests ({report.subset_percent:.0f}%)",
    ]
    if report.total_cpu_seconds is not None and report.subset_cpu_seconds is not None:
        redundant = round(report.total_cpu_seconds - report.subset_cpu_seconds, 1)
        lines += [
            f"- subset cost: {report.subset_cpu_seconds:.0f} of {report.total_cpu_seconds:.0f} CPU-seconds",
            f"- redundant remainder: {redundant:.0f} CPU-seconds",
        ]
    if report.coverage_curve:
        lines += ["", "## Coverage by subset size", ""]
        lines += [f"- {n} tests -> {pct:.1f}% of arcs" for n, pct in report.coverage_curve]
    lines += [
        "",
        "The subset reproduces every arc the full suite reaches. It measures how much",
        "redundancy exists; it is not a delete list. Greedy cover favours broad tests and",
        "picks arbitrarily among equally redundant ones, and arcs cannot tell apart two",
        "tests that walk one path with different data.",
        "",
    ]
    return "\n".join(lines)


def run_suite(pytest_args: list[str], coverage_path: Path, durations_path: Path | None) -> int:
    """Run pytest under branch coverage with one context per test."""
    command = [
        sys.executable,
        "-m",
        "pytest",
        *pytest_args,
        "--cov=gza",
        "--cov-branch",
        "--cov-context=test",
        "--cov-report=",
        "-o",
        "addopts=",
    ]
    if durations_path is not None:
        command.append("--durations=0")
    env_note = f"COVERAGE_FILE={coverage_path}"
    print(f"$ {env_note} {' '.join(command)}", flush=True)

    import os

    env = dict(os.environ, COVERAGE_FILE=str(coverage_path))
    if durations_path is None:
        return subprocess.call(command, env=env)
    with open(durations_path, "w", encoding="utf-8") as handle:
        return subprocess.call(command, env=env, stdout=handle, stderr=subprocess.STDOUT)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--coverage-file",
        help="Analyse an existing coverage data file instead of running the suite.",
    )
    parser.add_argument(
        "--durations-file",
        help="pytest --durations=0 transcript used to price the subset in CPU-seconds.",
    )
    parser.add_argument("-o", "--output", help="Write the report to PATH instead of stdout.")
    parser.add_argument("--json", action="store_true", help="Emit the report as JSON.")
    parser.add_argument(
        "--package-marker",
        default="/src/gza/",
        help="Only count files whose path contains this fragment (default: /src/gza/).",
    )
    parser.add_argument(
        "--curve",
        default="100,500,1000,2000,3000,4000",
        help="Comma-separated subset sizes to report coverage for.",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Args after '--' are passed to pytest. Defaults to 'tests/ -n 2 --dist loadscope -q'.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    marks = tuple(int(x) for x in args.curve.split(",") if x.strip())

    with tempfile.TemporaryDirectory() as tmp:
        if args.coverage_file:
            coverage_path = Path(args.coverage_file)
            durations_path = Path(args.durations_file) if args.durations_file else None
        else:
            extra = args.pytest_args
            if extra and extra[0] == "--":
                extra = extra[1:]
            pytest_args = extra or ["tests/", "-n", "2", "--dist", "loadscope", "-q"]
            coverage_path = Path(tmp) / "coverage.data"
            durations_path = Path(args.durations_file or Path(tmp) / "durations.txt")
            rc = run_suite(pytest_args, coverage_path, durations_path)
            if rc != 0:
                print(f"pytest exited {rc}; see {durations_path}", file=sys.stderr)
                return rc

        arcs_by_context = load_arcs_by_context(coverage_path, args.package_marker)
        durations = load_durations(durations_path)
        report = build_report(arcs_by_context, durations, marks)

    text = json.dumps(report.as_dict(), indent=2) if args.json else render(report)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
