# Server (HTTP API + Web UI)

## Overview

A local, single-user gza server: an HTTP API over gza's data plus the web UI it serves. The API is the durable identity — the server-rendered UI is its first client, and a future JavaScript frontend calls the same API over HTTP. Lives **in this repo** as a monorepo subproject with its own `gza.yaml`, depending on gza core and consuming its Python query/edit APIs (never raw sqlite). Phase 1 is a task browser and curation tool; observability (dashboard, live logs, diffs) is phase 2.

## Motivation

The CLI is great for driving gza but poor for *browsing* it. The primary workflow: scan a large tag bucket (e.g. everything tagged `v0.6`), read task prompts and plans as rendered markdown, and promote a handful into the next release tag (e.g. `v0.6.0`). Doing that through `gza search` + `gza show` + `gza retag` is tedious; a browser page collapses it into search → read → retag.

## Packaging & Layout

- Monorepo subproject in this repo (e.g. `server/`), with its own `gza.yaml` below the repo root. Gza's existing subproject support gives it an enforced write boundary (`PROJECT_SCOPE_VIOLATION` outside the subtree) and its **own `verify_command`** — core's verify surface is untouched by server work, and vice versa.
- Server tasks are filed into the server subproject; core tasks into the root project. Watch is per-project — run a watch per subproject as needed; a multi-project watch is a possible later enhancement, not a prerequisite.
- Gza core takes no web dependencies. The server's deps (FastAPI/Flask, uvicorn, markdown) belong to the subproject's own package (installable as an optional extra, e.g. `gza[server]`).
- A future JavaScript frontend becomes a sibling subproject (e.g. `frontend/`) with its own `gza.yaml` and node/TS verify command, its build served as static files by the Python server. Phase 1 does not require it.

## Process model

- The server is its **own process** (`gza server start` / `stop` / `status` / `open`), never embedded in watch: watch re-execs on self-merges under the editable install, and a request-handler bug must not be able to take down the scheduler. Phase 1 needs no live process at all — browsing/curation works with watch stopped.
- The backend imports gza's Python APIs (task store, query filters, retag/edit semantics), so schema, migrations, and edit invariants stay enforced in one place. It resolves data via the standard shared-DB config, so it sees all projects.
- For phase-2 liveness it reads the same state watch writes (DB + log files) — watch-adjacent, not watch-embedded.
- Server-rendered Python (templates, markdown renderer, htmx-level interactivity) for phase 1; no frontend build toolchain until phase 2 features demand one.
- All endpoints bind to `127.0.0.1` only. No auth (single-user local).

## Watch introspection

The server never talks to the watch process directly — no socket, no RPC. Two channels cover everything:

- **State lives in the DB.** Task statuses, the worker registry (`gza ps`), merge units, and queue lanes are already written by watch and readable through the same gza APIs the CLI uses. The server answers "what's running / merged / parked" from the DB alone, whether or not watch is up.
- **Narrative lives in a structured event log.** The one watch-only information today is prose in `.gza/watch.log` (scheduling decisions, cycle summaries). Watch should emit a schema'd JSONL event stream (cycle start/end, task dispatched, merge attempted + result, park reason); the server tails it for timeline/live views, and the human-readable log becomes a rendering of the same events. This subsumes the existing watch-log skimmability/timestamp work.

A watch-embedded API was considered and rejected: watch re-execs itself on self-merge cycles, so its endpoint would die at exactly the interesting moments. Connection draining before re-exec is buildable, but it buys lifecycle negotiation, port discovery, and API versioning between two processes that already share a database — all to deliver data the DB and log directory provide passively. If the server later needs to *command* watch (pause, drain, bump — phase 2+), the mechanism is a control row in the DB that watch polls, not HTTP into watch.

## Phase 1 — browse, read, curate

Target: the release-curation workflow end to end.

### Tasks list

- Search by prompt text; filter by tag (multi-select), status, type — same semantics as `gza search`, including untagged selection.
- Sortable by created/updated time; tag chips on each row.

### Task detail

- Metadata: id, type, status, tags, branch, parent/child links, timings.
- Prompt rendered as markdown, with **edit** (pending tasks, matching `gza edit` rules).
- For plan tasks: the plan markdown rendered, with **edit** (writes back through the same path the CLI uses).

### Tag editing

- Add, remove, and replace tags on a task — mirroring `gza retag` semantics (`add`/`remove`/`replace OLD NEW`).
- Bulk retag across the current filtered set, with a confirm step showing the matched tasks.

## Phase 2 — observe

Previously the v1 scope; now follow-up work:

- **Dashboard**: recent failures, ready to merge, needs manual intervention, completed plans/explores with no follow-up; "running now" strip with turn count + elapsed time.
- **Live log streaming**: tail a running task in the browser (SSE), sharing the backend log source of `gza log -f` / `gza tv`.
- **Diff viewer**: read-only view of a task's branch vs. its base.
- **Specs/plans browsing** as a standalone section.
- **Driving gza from the UI**: create, resume, merge, queue.
- **JavaScript frontend** as a sibling subproject if/when the interactive surface outgrows server-rendered pages.

## Open Questions

1. Subproject directory name (`server/`?) and package name.
2. Log streaming source (phase 2) — wait for unified logging work, or read files directly and migrate later?
3. Diff viewer (phase 2) — server-render with a library, or ship raw unified diff and highlight client-side?
