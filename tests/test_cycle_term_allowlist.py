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
    ("docs/configuration.md", "| `--[no-]auto-restart-on-drift` | When installed `gza` code changes while watch is running, re-exec at the next cycle boundary to load the new code without waiting for running or pending work to drain (default: enabled) |"),
    ("docs/configuration.md", "When watch detects that the installed `gza` package fingerprint has changed since startup, it logs the drift immediately and, by default, re-execs itself at the next cycle boundary without waiting for running or pending work to drain. Detached workers keep running, and the replacement watch process reconciles them after it auto-resumes. The re-exec is treated as a continuation of the already-approved watch session, so it skips the first-pass confirmation prompt. Pass `--no-auto-restart-on-drift` to keep the manual-restart warning instead."),
    ("docs/configuration.md", "`watch.parked_auto_rearm.enabled` turns on a conservative watch-owned blind parked auto-rearm phase. It runs after watch finishes the direct non-worker lifecycle phase for the current pass and before worker dispatch planning, so same-cycle slot reuse still goes through the ordinary shared watch planner instead of a separate executor."),
    ("docs/configuration.md", "`watch.slot_settle_seconds` bounds how long `gza watch` gives a selected worker launch to settle during the current pass. Only live-running proof or a live registered worker consumes a slot in that window. If the launch reaches a terminal outcome before any live proof, watch logs that no-slot outcome, releases the provisional launch budget for the same cycle, and keeps scanning for another runnable candidate instead of leaving capacity idle. Launches with neither live proof nor terminal evidence in the window are logged as undispatched and likewise do not advance no-progress accounting."),
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "  watch scheduler (urgent → recovery → normal) with a per-cycle recovery cap;"),
    ("docs/internal/sessions/2026-05-30-rebase-loop-and-followups.md", "| gza-3949 | plan | v0.6.0 | queued (deps gza-3947) | Explicit 3-tier watch scheduler: urgent → recovery → normal, with per-cycle recovery cap |"),
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "mislabeled as `GIT_ERROR`, and nothing stopped the cycle — so ~95 identical dead"),
    ("docs/incidents/2026-05-30-rebase-noop-loop.md", "  watch cycle. Forever."),
    ("docs/internal/advance-workflow.md", "At the start of each watch pass, watch also fingerprints the installed `gza` Python package on disk. If the package contents drift from what the process started with, watch emits one loud `WARNING` line for that newly observed fingerprint. In the default mode, the warning explicitly says watch will re-exec itself at the next cycle boundary without waiting for running or pending work to drain; detached workers stay alive and the replacement process reconciles them after it auto-resumes, skipping the first-pass confirmation gate because the session was already approved. `--no-auto-restart-on-drift` switches back to the warn-only manual-restart message."),
    ("docs/internal/stats-subcommands.md", "Ports the functionality of the former `bin/review-cycle-stats.py` script into the CLI. Shows per-implementation-task review iteration stats, weekly groupings, iteration-count distribution, and per-model issue counts."),
    ("docs/internal/stats-subcommands.md", "- `bin/review-cycle-stats.py` was removed in the same PR this subcommand was added. Use `gza stats reviews` instead."),
    ("src/gza/cli/main.py", '        help="Re-exec watch at the next cycle boundary when the installed gza code changes (default: enabled)",'),
    ("src/gza/cli/watch.py", '            "at the next cycle boundary to load new code"'),
    ("src/gza/lineage.py", '                return None, f"{label} task {task.id} has a cycle in its based_on chain"'),
    ("src/gza/runner.py", '                "Slug override cycle detected for task #%s while walking based_on chain: "'),
    ("src/gza/runner.py", "            # Walk up the based_on chain, with cycle detection"),
    ("src/gza/skills/gza-system-triage/SKILL.md", "If many failures — **including retries** — cluster in a short window on infra reasons, treat them as **one root cause**, not N findings. The right fix is a watch **cycle-level precondition gate** that pauses the whole cycle (recovery lane included) so nothing burns retries while the precondition is broken. Before proposing it, check whether detection/gating already exists (e.g. docker-daemon-crash detection); if it does, point at it rather than re-proposing. A scheduled task cannot fix this — with scarce slots it would deadlock behind a recovery lane that never drains."),
    ("src/gza/skills/gza-system-triage/SKILL.md", "- **The systemic fix**, stated as a *precondition or rule change*, and its shape: a code change to file, a cycle-level gate, or a `/gza-task-fix` per-task handoff."),
    ("tests/cli/test_execution.py", '        result = invoke_gza("cycle", "testproject-1", "--dry-run", "--project", str(tmp_path))'),
    ("tests/cli/test_execution.py", '        assert "invalid choice: \'cycle\'" in result.stderr'),
    ("tests/cli/test_main.py", '        """watch help/docs should describe next-cycle-boundary drift restart without drain gating."""'),
    ("tests/cli/test_main.py", '        """Removed `cycle` command should now fail at parser validation."""'),
    ("tests/cli/test_main.py", '        assert "Re-exec watch at the next cycle boundary when the installed gza code changes" in help_text'),
    ("tests/cli/test_main.py", '        assert "re-exec at the next cycle boundary to load the new code without waiting for running or pending work to drain" in docs_text'),
    ("tests/cli/test_main.py", '        assert "re-exec itself at the next cycle boundary without waiting for running or pending work to drain" in internal_docs_text'),
    ("tests/cli/test_watch.py", '    """Watch should advertise next-cycle-boundary re-exec once per new drift fingerprint."""'),
    ("tests/cli/test_watch.py", '            "WARNING   installed gza changed since watch started -- watch will re-exec at the next cycle boundary to load new code"'),
    ("tests/cli/test_watch.py", '    """Pending drift should restart watch at the next cycle boundary regardless of queue state."""'),
    ("tests/cli/test_watch.py", '    """Watch should re-exec itself at the first cycle boundary where drift is detected."""'),
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
