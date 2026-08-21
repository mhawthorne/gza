#!/usr/bin/env python3
"""Move project_id='gzaserver' rows from the global shared DB into the core gza DB.

Run with no watch/gza process touching either DB. Usage:
    python3 migrate_gzaserver.py            # dry run (counts only)
    python3 migrate_gzaserver.py --apply
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import time
from pathlib import Path

SRC = Path.home() / ".gza" / "gza.db"
DST = Path.home() / "work" / "supreme" / "gza" / ".gza" / "gza.db"
PID = "gzaserver"

# Ordered parents-before-children so foreign keys stay satisfiable.
TABLES = [
    "projects",
    "project_sequences",
    "project_leases",
    "tasks",
    "task_tags",
    "task_comments",
    "task_artifacts",
    "merge_units",
    "merge_unit_tasks",
    "run_steps",
    "run_substeps",
    "behavior_check_findings",
    "parked_task_rearms",
    "watch_progress_observations",
    "watch_recovery_backoffs",
    "main_verify_remediation_attempts",
    "main_verify_remediation_consumed_task_ids",
]


def key_col(conn: sqlite3.Connection, table: str) -> str:
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if "project_id" in cols:
        return "project_id"
    if table == "projects" and "id" in cols:
        return "id"
    raise SystemExit(f"{table}: no project key column")


def columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def main() -> int:
    apply = "--apply" in sys.argv
    for p in (SRC, DST):
        if not p.exists():
            raise SystemExit(f"missing DB: {p}")

    src = sqlite3.connect(f"file:{SRC}?mode=ro", uri=True)
    dst_ro = sqlite3.connect(f"file:{DST}?mode=ro", uri=True)

    sv_src = src.execute("select max(version) from schema_version").fetchone()[0]
    sv_dst = dst_ro.execute("select max(version) from schema_version").fetchone()[0]
    if sv_src != sv_dst:
        raise SystemExit(f"schema_version mismatch: src={sv_src} dst={sv_dst}")

    # Tables whose rows hang off a task; both DBs already contain some orphans
    # (450 in the core DB alone), so copying orphaned children would add new
    # dangling references. Skip them and report the count instead.
    TASK_CHILDREN = {
        "task_tags": "task_id",
        "task_comments": "task_id",
        "task_artifacts": "task_id",
        "merge_unit_tasks": "task_id",
        "run_steps": "run_id",
        "run_substeps": "run_id",
    }

    plan = []
    for t in TABLES:
        kc = key_col(src, t)
        # Column order differs between DBs for some tables; only the set matters
        # because rows are copied with an explicit, name-based column list.
        if set(columns(src, t)) != set(columns(dst_ro, t)):
            raise SystemExit(f"{t}: column sets differ between DBs")
        where = f"{kc}=?"
        if t in TASK_CHILDREN:
            fk = TASK_CHILDREN[t]
            where += (
                f" AND EXISTS (SELECT 1 FROM tasks p"
                f" WHERE p.project_id={t}.{kc} AND p.id={t}.{fk})"
            )
        n_all = src.execute(f"select count(*) from {t} where {kc}=?", (PID,)).fetchone()[0]
        n_src = src.execute(f"select count(*) from {t} where {where}", (PID,)).fetchone()[0]
        if n_all != n_src:
            print(f"  (skipping {n_all - n_src} orphaned {t} rows with no parent task)")
        n_dst = dst_ro.execute(f"select count(*) from {t} where {kc}=?", (PID,)).fetchone()[0]
        if n_dst:
            raise SystemExit(f"{t}: destination already has {n_dst} {PID} rows — aborting")
        plan.append((t, kc, n_src, where))
        print(f"{t:45s} {n_src:6d}")

    total = sum(n for _, _, n, _ in plan)
    print(f"{'TOTAL':45s} {total:6d}")
    src.close()
    dst_ro.close()
    if not apply:
        print("\ndry run — re-run with --apply")
        return 0

    stamp = time.strftime("%Y%m%d-%H%M%S")
    for p in (SRC, DST):
        bak = p.with_name(f"{p.name}.bak-gzaserver-migrate-{stamp}")
        shutil.copy2(p, bak)
        print(f"backup: {bak}")

    baseline = sqlite3.connect(f"file:{DST}?mode=ro", uri=True)
    baseline_violations = len(baseline.execute("PRAGMA foreign_key_check").fetchall())
    baseline.close()
    print(f"destination has {baseline_violations} pre-existing FK violations (left as-is)")

    conn = sqlite3.connect(DST)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("ATTACH DATABASE ? AS src", (str(SRC),))
    try:
        # Defer FK enforcement to COMMIT: the copy order above is parents-first,
        # but rows arrive table-at-a-time so intermediate states look dangling.
        conn.execute("PRAGMA defer_foreign_keys=ON")
        conn.execute("BEGIN IMMEDIATE")
        # These three tables key on a global AUTOINCREMENT id rather than a
        # per-project one, so source ids collide with ids already in the core DB.
        # Shift them past the destination's high-water mark, and shift
        # run_substeps.step_id by the same amount so it still points at its step.
        offsets = {}
        for t in ("task_artifacts", "run_steps", "run_substeps"):
            offsets[t] = conn.execute(f"select coalesce(max(id), 0) from main.{t}").fetchone()[0]

        for t, kc, n, where in plan:
            cols = [c for c in columns(conn, t)]
            src_exprs = []
            for c in cols:
                if c == "id" and t in offsets:
                    src_exprs.append(f'"{c}" + {offsets[t]}')
                elif c == "step_id" and t == "run_substeps":
                    src_exprs.append(f'"{c}" + {offsets["run_steps"]}')
                else:
                    src_exprs.append(f'"{c}"')
            col_list = ",".join(f'"{c}"' for c in cols)
            conn.execute(
                f"INSERT INTO main.{t} ({col_list}) "
                f"SELECT {','.join(src_exprs)} FROM src.{t} WHERE {where}",
                (PID,),
            )
        for t, kc, n, where in plan:
            got = conn.execute(f"select count(*) from main.{t} where {kc}=?", (PID,)).fetchone()[0]
            if got != n:
                raise SystemExit(f"{t}: copied {got}, expected {n} — rolling back")
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if len(violations) > baseline_violations:
            for v in violations[:20]:
                print("FK violation:", v)
            raise SystemExit(
                f"{len(violations) - baseline_violations} NEW FK violations — rolling back"
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("DETACH DATABASE src")
        conn.close()

    print(f"\ncopied {total} rows into {DST}")
    print("source rows left in place; delete them once the new DB is confirmed good.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
