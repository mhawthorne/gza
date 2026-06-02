# Max Steps Abstraction Spec

## Summary

Replace provider-specific "turn" budgeting with a provider-agnostic `max_steps` budget in gza. A "step" is the smallest stable unit of agent progress per provider.

This avoids mismatches where one provider reports many turns and another reports one turn with many sub-events.

## Problem

`max_turns` currently assumes all providers expose comparable turn events. In practice:

- Claude exposes turns in a way that maps well to `max_turns`.
- Codex often emits one `turn.started`/`turn.completed` pair with many `item.completed` events, making `max_turns` ineffective.
- Provider-level differences leak into user-facing behavior and failure handling.

## Goals

1. Add `max_steps` as the canonical execution budget across providers.
2. Preserve existing behavior for Claude as closely as possible.
3. Make Codex budget enforcement effective and predictable.
4. Keep backward compatibility with `max_turns` during migration.
5. Expose step metrics consistently in task stats/history.

## Non-goals

1. Perfectly normalize "effort" across providers.
2. Redesign provider JSON parsing beyond step accounting.
3. Remove all turn metrics immediately (deprecate first).

## Definitions

- **Step**: Provider-specific unit of progress used for budget enforcement.
- **Reported steps**: Provider-native count if the provider supplies one.
- **Computed steps**: gza-derived count from parsed stream events.

## Config Changes

### New fields

- `max_steps` (int, default: 50): global budget.
- `task_types.<type>.max_steps` (int): per-task-type override.
- `defaults.max_steps` (int): optional defaults section variant.

### Backward compatibility

- Keep reading `max_turns` and `task_types.<type>.max_turns`.
- Resolution order during migration:
  1. task-specific override (future, if added)
  2. `task_types.<type>.max_steps`
  3. global `max_steps`
  4. `task_types.<type>.max_turns`
  5. global `max_turns`
  6. default 50
- Warn when `max_turns` is used without `max_steps`:
  - "`max_turns` is deprecated; use `max_steps`."

## Provider Step Mapping

### Claude

- Step unit: assistant turn/message (existing turn semantics).
- Enforcement:
  - Continue passing provider-native `--max-turns` using resolved `max_steps`.
  - Keep computed dedupe logic by assistant message ID.

### Codex

- Step unit: `item.completed` events.
- Optional refinement:
  - Count all item types by default.
  - Consider future flag to count subset (`agent_message` + `command_execution`) if needed.
- Enforcement:
  - Increment computed step counter on each `item.completed`.
  - If counter exceeds resolved `max_steps`, mark `error_type = "max_steps"` and terminate process.
  - Keep existing timeout fallback.

### Gemini

- Step unit: existing closest proxy used today (tool/action event).
- Enforcement:
  - same `max_steps` threshold logic.

## Runtime Behavior

1. Resolve effective `max_steps` at task start (respect task type override).
2. Provider parser updates computed step count as stream events arrive.
3. When computed steps > budget:
  - mark run result error type `max_steps`
  - stop provider process when possible
  - surface clear message in CLI output
4. Stats should always include:
  - `num_steps_reported` (nullable)
  - `num_steps_computed` (nullable)
5. Retain turn fields during migration for compatibility.

## Failure Semantics

- New canonical failure reason: `MAX_STEPS`.
- Maintain mapping compatibility:
  - old `MAX_TURNS` should still be recognized in existing data paths.

## UI/Console Output

- Replace "Turns" with "Steps" in summary lines when step metrics are present.
- During migration, optionally show:
  - `Steps: X (legacy turns: Y)` only if both are available and differ.

## Data Model / DB

Add nullable columns:

- `num_steps_reported`
- `num_steps_computed`

Migration requirements:

1. Add columns with safe defaults (NULL).
2. Keep existing turn columns.
3. Update read/write paths and task stats rendering.

## Testing Plan

### Config

1. `max_steps` parsing and precedence over `max_turns`.
2. task-type `max_steps` precedence.
3. deprecation warning when using only `max_turns`.

### Providers

1. Claude uses resolved `max_steps` for provider invocation.
2. Codex increments computed steps on `item.completed`.
3. Codex triggers `max_steps` on over-budget streams.
4. Gemini step counting remains stable.

### Runner / Stats

1. Failed run on `max_steps` is stored with correct reason.
2. Console displays step metrics.
3. Backward compatibility with existing turn-only records.

## Rollout Plan

1. Introduce `max_steps` + compatibility layer.
2. Switch runner/console to step-first display.
3. Keep turn fields for at least one release window.
4. Announce deprecation and eventual removal of `max_turns`.

## Open Questions

1. Should Codex step counting include all `item.completed` types or only actionable types?
2. Should provider-specific overrides be introduced now (e.g., `codex.max_steps_unit`)?
3. Should `gza validate` emit warnings or errors when both `max_steps` and `max_turns` are set inconsistently?
