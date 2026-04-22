# Web UI

## Overview

A local, single-user web UI for browsing gza state, monitoring live task runs, and answering the question **"what should I work on next?"**. Ships in the same repo as gza, started with `gza ui start` / stopped with `gza ui stop`. Read-only for v1 — does not drive gza (no task creation, resume, merge, or edit from the browser).

## Motivation

The CLI is great for driving gza but poor for *observing* it. Today, answering "what needs my attention?" means running several commands (`gza unmerged`, `gza log`, `gza queue ls`, checking for max-turns failures) and stitching the results in your head. A dashboard collapses that into a glance. Tagging (e.g. `v0.5`) lets the user slice tasks by milestone, which the CLI makes tedious.

## Goals

- Surface "what needs attention" on a home dashboard.
- Let the user tag tasks with flat, multi-valued labels and filter by them.
- Tail live task runs in the browser.
- Browse past tasks, specs, plans, and explore-task results read-only.
- Ship in ~1 week of focused work.

## Non-Goals (v1)

- Multi-user, auth, remote access.
- Editing tasks, specs, plans, or prompts from the browser.
- Creating, resuming, merging, or queueing tasks from the browser.
- Mobile layout polish.
- Auto-starting the daemon on `gza` invocations.

## Architecture

```
┌─────────────────┐       ┌──────────────────────┐       ┌───────────────┐
│ Next.js app     │──────▶│ FastAPI backend      │──────▶│ gza sqlite DB │
│ (React, static  │  HTTP │ (uvicorn, local-only)│       │ + git repos   │
│  build served   │       │                      │       │ + log files   │
│  by FastAPI)    │       │                      │       │               │
└─────────────────┘       └──────────────────────┘       └───────────────┘
         ▲                          │
         │ SSE / WS for live logs   │
         └──────────────────────────┘
```

- **Backend:** FastAPI (matches gza's Python stack). Reuses existing gza Python APIs for DB access — does not open sqlite directly.
- **Frontend:** Next.js (per user preference). Static export served by FastAPI at `/` to avoid a second dev-server in production. In dev, Next.js dev server proxies API calls to FastAPI.
- **Process model:** `gza ui start` forks a uvicorn process, writes `pid` + `port` to the gza state dir, picks a random free port, opens the browser at `http://localhost:<port>`. `gza ui stop` reads the pid file and terminates.
- **Same repo:** lives under `src/gza/ui/` (backend) and `frontend/` (Next.js). Monorepo-style. Ships with gza releases.

## Data Model Changes

### Tags

New sqlite table:

```sql
CREATE TABLE task_tags (
  task_id TEXT NOT NULL,
  tag     TEXT NOT NULL,
  PRIMARY KEY (task_id, tag),
  FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX idx_task_tags_tag ON task_tags(tag);
```

- Flat namespace, multi-valued per task.
- Applied to tasks only (not specs — a task that implements a spec is good enough).
- Migration handled by gza's existing migration system.

### Tags vs. queues

- **Queues** stay as they are: execution buckets (what `gza work` pulls from).
- **Tags** are orthogonal labels for slicing/filtering (e.g. `v0.5`, `frontend`, `bug`).
- Commands that take a queue-like filter (e.g. `gza watch`) MAY accept tags interchangeably in a later iteration — out of scope for v1 UI, tracked as follow-up.

## Pages

### 1. Home / Dashboard (`/`)

Four sections, each a ranked list with task cards linking to the task detail page:

1. **Recent failures** — tasks that errored in the last N days.
2. **Ready to merge** — successful tasks with unmerged branches.
3. **Needs manual intervention** — tasks that hit max-iterations.
4. **Completed plans / explore tasks with no follow-up** — terminal plan/explore tasks that did not spawn a child task.

Each card shows: task id, type, title/prompt excerpt, age, tags, status pill. v1 is state-surfacing only — no "suggested actions" panel.

### 2. Tasks list (`/tasks`)

- Table of all tasks, filterable by: status, type, tag (multi-select), queue, date range.
- Sortable by created/updated/started time.
- Tag chips on each row.

### 3. Task detail (`/tasks/:id`)

- Metadata: id, type, status, queue, tags, branch, parent/child links, timings.
- Prompt (rendered markdown).
- Log viewer: tail-style, virtualised. If task is running, streams new lines via SSE (see live monitoring below). Reuses the same backend log source as `gza log -f` / `gza tv`.
- Diff viewer: read-only view of the task's branch vs. its base. Uses `git diff` output rendered client-side.
- Tag editor: add/remove tags. This is the **one** write operation permitted in v1, because tagging is the killer feature.

### 4. Specs / Plans (`/specs`, `/plans`)

Cut from v1 per user decision (the one thing to drop if time-constrained). Stub routes only, with a "coming soon" note. Follow-up work.

### 5. Live monitoring

Integrated into the task detail page rather than a separate screen. Dashboard also shows a small "running now" strip at the top listing currently-executing tasks (1–10 concurrent typical) with turn count + elapsed time, each linking to its detail page.

Log streaming piggybacks on whatever unified logging work lands from the existing pending tasks — the UI consumes the same stream.

## API Surface (sketch)

```
GET  /api/dashboard             → { failures, ready_to_merge, max_iter, orphan_plans, running }
GET  /api/tasks?tag=&status=... → paginated list
GET  /api/tasks/:id             → full task record
GET  /api/tasks/:id/log         → historical log (paginated)
GET  /api/tasks/:id/log/stream  → SSE stream of new log lines
GET  /api/tasks/:id/diff        → unified diff text
POST /api/tasks/:id/tags        → { add: [...], remove: [...] }
GET  /api/tags                  → all known tags with counts
```

All endpoints bind to `127.0.0.1` only. No auth (single-user local).

## CLI Surface

```
gza ui start        # fork daemon, open browser, print URL
gza ui stop         # kill daemon
gza ui status       # print pid, port, uptime
gza ui open         # open browser to running daemon (no-op if not running)
```

State file: `<gza_state_dir>/ui.pid` containing `{pid, port, started_at}`.

## Scope Cuts & Phasing

v1 (one week target):
- `gza ui start/stop/status/open`
- Dashboard with the four sections + running strip
- Tasks list + filtering by tag
- Task detail: metadata, prompt, log (historical + live), diff, tag editor
- Tags schema + migration + API

Post-v1 (follow-ups):
- Specs / plans browsing and viewing
- Suggested next actions on dashboard
- Tag/queue interop in `gza watch` and similar
- In-browser editing of specs/plans
- Driving gza from the UI (create, resume, merge)

## Open Questions

1. Log streaming source — should it wait for the pending unified logging work, or read files directly now and migrate later?
2. Dashboard "recent" windows — fixed (e.g. 7 days) or user-configurable?
3. Diff viewer — server-render with a library, or ship raw unified diff and let the client highlight?
4. Next.js: static export vs. running a Node server alongside FastAPI? Static export is simpler; pick unless a feature demands SSR.
