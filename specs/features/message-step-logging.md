# Message-Step Logging Spec

## Summary

Adopt a step-first execution log model where each provider message is a top-level `step`, and tool activity is recorded as substeps under that step.

This replaces turn-centric labeling (for example `T1.13`) as the primary user-facing timeline while keeping optional compatibility metadata for legacy traces.

## Problem

Current logs interleave status messages and tool events under a turn/event label (`T1.x`). In providers like Codex, runs often have a single turn containing many meaningful actions, which makes turn labels low-value and confusing.

Users reason about execution as:

1. What message did the provider produce?
2. What tools ran during that message?
3. What outcome did that message produce?

## Goals

1. Make provider messages the canonical, provider-agnostic unit of progression (`step`).
2. Preserve detailed tool observability as nested substeps/events.
3. Keep timeline readability high in CLI/log output.
4. Support backward compatibility with existing turn-based traces.
5. Allow deterministic migration from existing `T1.x` records.

## Non-goals

1. Redesign provider transport protocols.
2. Remove all legacy turn metadata in one release.
3. Normalize semantic meaning of every provider event type.

## Definitions

- **Step**: One provider message that is surfaced in the run timeline.
- **Substep**: A machine event associated with a step (tool call/result/error, status note).
- **Legacy turn**: Historical provider cycle ID (for example `T1`) retained as compatibility metadata.

## Event Model

### Top-level step model

Each step MUST represent exactly one provider message and include:

- `step_id` (string): stable run-local ID, for example `S1`, `S2`.
- `step_index` (int): monotonic counter per run, starting at `1`.
- `provider` (string): `codex`, `claude`, `gemini`, etc.
- `message_role` (string): usually `assistant`.
- `message_text` (string, nullable): user-visible text content.
- `started_at` (RFC3339 timestamp).
- `completed_at` (RFC3339 timestamp, nullable until complete).
- `outcome` (enum): `completed`, `failed`, `interrupted`, `timeout`.
- `summary` (string, nullable): compact machine-generated outcome summary.

### Substep model

Substeps are ordered children of a step and include:

- `substep_id` (string): stable step-local ID, for example `S3.1`, `S3.2`.
- `substep_index` (int): monotonic within parent step.
- `type` (enum):
  - `tool_call`
  - `tool_output`
  - `tool_error`
  - `tool_retry`
  - `status_update`
  - `artifact`
- `source` (string): `runner`, `provider`, `tool`.
- `payload` (json): typed data by substep type.
- `timestamp` (RFC3339 timestamp).

### Tool event linkage

Tool activity SHOULD be linkable as a lifecycle chain:

- `tool_call` contains `call_id`, tool name, arguments.
- `tool_output`/`tool_error` reference the same `call_id`.
- Retries emit new `tool_call` with `retry_of_call_id`.

## Numbering and Display

Primary display key:

- Step: `S<n>`
- Substep: `S<n>.<m>`

Examples:

- `S4  Provider message: "I’m updating tests now..."`
- `S4.1 tool_call Bash: rg -n ...`
- `S4.2 tool_output exit=0 ...`

Turn labels (`T1`, `T1.13`) MUST NOT be the primary display key.

## Mapping from Legacy `T1.x`

Given legacy events in sequence order:

1. Create a new step whenever an event is a provider message/status line.
2. Attach subsequent tool events to the most recent open step.
3. If tool events appear before any provider message, attach them to synthetic step `S1` with summary `"Pre-message tool activity"`.
4. Preserve original turn/event IDs in compatibility fields.

Example mapping:

- `[T1.13] I’m updating tests ...` -> `S5` (`message_text` populated)
- `[T1.21] -> Bash rg -n ...` -> `S5.1` (`type=tool_call`)
- command result event -> `S5.2` (`type=tool_output`)

## Storage Schema (Logical)

### `run_steps`

- `id` (pk)
- `run_id` (fk)
- `step_index` (int, indexed)
- `step_id` (text, unique per run)
- `provider` (text)
- `message_role` (text)
- `message_text` (text, nullable)
- `started_at` (datetime)
- `completed_at` (datetime, nullable)
- `outcome` (text)
- `summary` (text, nullable)
- `legacy_turn_id` (text, nullable)
- `legacy_event_id` (text, nullable)

### `run_substeps`

- `id` (pk)
- `run_id` (fk)
- `step_id` (fk -> run_steps.id)
- `substep_index` (int)
- `substep_id` (text, unique per step)
- `type` (text)
- `source` (text)
- `call_id` (text, nullable)
- `payload_json` (text)
- `timestamp` (datetime)
- `legacy_turn_id` (text, nullable)
- `legacy_event_id` (text, nullable)

## API Contract

### Emit step

`emit_step(message)` creates a new step and returns `step_ref`.

### Emit substep

`emit_substep(step_ref, type, payload)` appends to the step.

### Finalize step

`finalize_step(step_ref, outcome, summary)` sets completion data.

Runners/providers MUST NOT emit tool events without an active step reference; if unavoidable, create a synthetic step.

## CLI / UX Requirements

1. Timeline views (`work`, task logs, debug output) should show only `S<n>` as primary anchors.
2. Tool details should be collapsible/secondary where UI supports it.
3. Compact mode can omit substeps and show per-step summary only.
4. Verbose/debug mode should include full substep list with `S<n>.<m>` labels.

## Backward Compatibility

1. Continue reading legacy turn-only logs.
2. During transition, write both step records and legacy fields where available.
3. Provide deterministic projection from old events to new step/substep records.
4. Deprecate turn-first rendering after one release window once step logging is stable.

## Testing Plan

1. Step creation from provider message events across Codex/Claude/Gemini adapters.
2. Tool call/output/error correctly nested under the initiating step.
3. Synthetic step creation when tools arrive before provider message.
4. Deterministic migration from a fixture of `T1.x` events.
5. CLI output in compact vs verbose mode uses `S<n>`/`S<n>.<m>` labels.
6. Backward compatibility reader handles turn-only historical logs.

## Rollout Plan

1. Add schema and writer APIs (`run_steps`, `run_substeps`).
2. Update provider adapters to emit message-backed steps.
3. Update tool instrumentation to emit substeps against current step.
4. Ship dual-read + dual-write compatibility mode.
5. Switch CLI to step-first rendering.
6. Remove turn-first rendering after deprecation period.

## Open Questions

1. Should non-message system events ever create top-level steps, or always be substeps?
2. Should we cap stored `tool_output` payload size and spill large payloads to artifacts?
3. Should step summaries be provider-authored text only, or allow runner-generated synthesis?
