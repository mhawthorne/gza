from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SEARCH_DIRS = ("src", "tests", "docs")
WHOLE_WORD_CYCLE = re.compile(r"\bcycle\b")
SELF_PATH = Path(__file__).resolve()

# Intentional survivors:
# - retired `cycle` command rejection coverage
# - based_on chain cycle detection
# - historical docs that refer to removed legacy filenames/commands verbatim
ALLOWED_CYCLE_LINES = {
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "  watch scheduler (urgent → recovery → normal) with a per-cycle recovery cap;"),
    ("docs/internal/sessions/2026-05-30-rebase-loop-and-followups.md", "| gza-3949 | plan | v0.6.0 | queued (deps gza-3947) | Explicit 3-tier watch scheduler: urgent → recovery → normal, with per-cycle recovery cap |"),
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "mislabeled as `GIT_ERROR`, and nothing stopped the cycle — so ~95 identical dead"),
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "  watch cycle. Forever."),
    ("docs/internal/stats-subcommands.md", "Ports the functionality of the former `bin/review-cycle-stats.py` script into the CLI. Shows per-implementation-task review iteration stats, weekly groupings, iteration-count distribution, and per-model issue counts."),
    ("docs/internal/stats-subcommands.md", "- `bin/review-cycle-stats.py` was removed in the same PR this subcommand was added. Use `gza stats reviews` instead."),
    ("src/gza/lineage.py", '                return None, f"{label} task {task.id} has a cycle in its based_on chain"'),
    ("src/gza/runner.py", '                "Slug override cycle detected for task #%s while walking based_on chain: "'),
    ("src/gza/runner.py", "            # Walk up the based_on chain, with cycle detection"),
    ("tests/cli/test_execution.py", '        result = invoke_gza("cycle", "testproject-1", "--dry-run", "--project", str(tmp_path))'),
    ("tests/cli/test_execution.py", '        assert "invalid choice: \'cycle\'" in result.stderr'),
    ("tests/cli/test_main.py", '        """Removed `cycle` command should now fail at parser validation."""'),
    ("tests/cli/test_main.py", '        result = invoke_gza("cycle", "testproject-1", "--dry-run", "--project", str(tmp_path))'),
    ("tests/cli/test_main.py", '        assert "invalid choice: \'cycle\'" in result.stderr'),
    ("tests/test_runner.py", '            "Slug override cycle detected for task #gza-child while walking based_on chain: "'),
    ("tests/test_runner.py", '        """When the based_on chain contains a cycle, fail with a clear error instead of looping forever."""'),
    ("tests/test_runner.py", "        # Introduce cycle: A.based_on = B (A -> B -> A)"),
    ("tests/test_runner.py", "        # Task C: based_on B, same_branch=True — will walk B -> A -> B (cycle)"),
}


def test_cycle_term_is_restricted_to_intentional_legacy_and_graph_cases() -> None:
    matches: set[tuple[str, str]] = set()
    for directory in SEARCH_DIRS:
        for path in (ROOT / directory).rglob("*"):
            if not path.is_file():
                continue
            if path.resolve() == SELF_PATH:
                continue
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue
            relative_path = path.relative_to(ROOT).as_posix()
            for line in path.read_text(errors="ignore").splitlines():
                if WHOLE_WORD_CYCLE.search(line):
                    matches.add((relative_path, line))

    assert matches == ALLOWED_CYCLE_LINES
