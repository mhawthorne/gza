# Task Provider Routing Spec

## Summary

Add top-level task-type provider routing so gza can use one provider for implementation tasks and another for review tasks, while keeping model tuning inside provider-scoped config.

Introduce:

- `task_providers` (new): maps task type to provider.
- Keep `providers.<provider>.task_types.<task_type>` for provider-local model/max-steps overrides.

## Problem

Today, provider selection is global (or per-task explicit via task metadata/CLI flags). This makes mixed-provider workflows awkward, especially:

- `implement` on provider A
- `review` on provider B

without manual per-task overrides.

## Goals

1. Route providers by task type using config only.
2. Preserve existing provider-scoped model/max-steps behavior.
3. Keep per-task explicit overrides highest priority.
4. Avoid overloading legacy top-level `task_types` semantics.
5. Make migration from legacy config predictable and safe.

## Non-goals

1. Removing legacy top-level `task_types` or `model` immediately.
2. Changing task DB schema for this feature.
3. Introducing per-task-type provider selection via environment variables in v1.

## Proposed Configuration

```yaml
provider: claude  # global fallback

task_providers:
  implement: claude
  improve: claude
  review: codex

providers:
  claude:
    model: claude-sonnet-4-5
    task_types:
      implement:
        model: claude-opus-4-1
  codex:
    model: o4-mini
    task_types:
      review:
        model: o4-mini
```

Valid task types: `task`, `explore`, `plan`, `implement`, `review`, `improve`.
Valid providers: `claude`, `codex`, `gemini`.

## Resolution Semantics

### Provider resolution (new)

For a task `t`:

1. `t.provider` (task-level override from DB / CLI `--provider`)
2. `task_providers.<t.task_type>`
3. top-level `provider` (already merged with `GZA_PROVIDER`)

### Model resolution (unchanged, provider-aware)

Using effective provider from above:

1. `t.model` (task-level override from DB / CLI `--model`)
2. `providers.<effective_provider>.task_types.<task_type>.model`
3. `providers.<effective_provider>.model`
4. `task_types.<task_type>.model` (legacy fallback)
5. `model` / `defaults.model` / `GZA_MODEL` (legacy fallback)
6. provider runtime default

### Max-steps resolution (unchanged, provider-aware)

Using effective provider from provider resolution:

1. `providers.<effective_provider>.task_types.<task_type>.max_steps`
2. `providers.<effective_provider>.task_types.<task_type>.max_turns` (legacy)
3. `task_types.<task_type>.max_steps` (legacy fallback)
4. `task_types.<task_type>.max_turns` (legacy fallback)
5. global `max_steps` / `max_turns` fallback chain

## Validation Rules

1. `task_providers` must be a dictionary if present.
2. Keys in `task_providers` must be valid task types.
3. Values in `task_providers` must be known providers.
4. Unknown keys in `task_providers` should be validation warnings (or errors, consistent with existing unknown-field policy).
5. If `task_providers.<type>=X` and legacy `task_types.<type>.model` appears incompatible with `X`, emit compatibility error/warning consistent with existing model-provider mismatch behavior.

## CLI / Runtime Behavior

1. No new CLI flags required for v1.
2. Existing `--provider` continues to override routing for that task.
3. Auto-created review tasks and `gza review` tasks should naturally resolve through `task_providers.review` unless task-level provider is explicitly set.
4. `gza retry`/`gza resume` preserve task-level provider/model when present, so behavior is stable across reruns.

## Backward Compatibility

1. Existing configs without `task_providers` behave unchanged.
2. Existing top-level `task_types` remains supported as model/max-steps fallback.
3. Existing provider-scoped config precedence remains unchanged except for provider selection source.
4. `GZA_PROVIDER` remains a global override by replacing top-level `provider`; task-level provider still wins.

## Documentation Changes

Update:

1. `docs/configuration.md`:
   - add `task_providers` to config table
   - add provider-routing precedence
   - include mixed-provider example (`implement` vs `review`)
2. `gza.yaml` template and `src/gza/gza.yaml.example` with commented example.

## Testing Plan

### Config parsing/validation

1. Parses valid `task_providers`.
2. Rejects invalid task types.
3. Rejects unknown providers.
4. Handles missing `task_providers` as no-op.

### Resolution behavior

1. `task.provider` overrides `task_providers`.
2. `task_providers` overrides global `provider`.
3. Model selection uses effective provider from `task_providers`.
4. Max-steps selection uses effective provider from `task_providers`.

### Workflow behavior

1. `implement` task runs with routed provider.
2. `gza review <id>` created review runs with `task_providers.review`.
3. Auto-created review (`create_review`) runs with `task_providers.review`.
4. Retry/resume preserve explicit task overrides.

## Rollout Plan

1. Add config field + validation + resolution support.
2. Add docs/examples.
3. Ship with compatibility notes in release notes.
4. Optionally add `gza validate` warning suggesting `task_providers` for multi-provider workflows when manual per-task provider overrides are detected repeatedly.

## Open Questions

1. Should unknown task types in `task_providers` be hard errors or warnings?
2. Should we add an env var override for `task_providers` (likely no in v1)?
3. Should `gza review` support an explicit `--provider/--model` at creation time for convenience (separate enhancement)?
