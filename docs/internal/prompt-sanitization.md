# Provider-Facing Prompt Sanitization

gza now applies a prompt-normalization pass at the provider boundary for `review` and `improve` runs.

## Scope

- Sanitization is applied only to the assembled prompt string passed into `provider.run(...)`.
- Task rows in SQLite are unchanged (`task.prompt`, report content, and summaries remain canonical source data).
- Current rollout covers `review` and `improve` task types only.

## Initial Risky-Term Map

To minimize false positives, replacements require both a trigger term and a nearby task/safety context:

- `bypass*` + (`sandbox|guardrail|policy|safety|restriction|constraint`) -> `work within`
- `kill*` + (`process|task|run|session|job|agent`) -> `terminate`
- `interrupted` + (`task|run|session|execution|agent|job`) -> `paused`
- `override*` + (`rule|policy|instruction|constraint|guardrail|safety|sandbox`) -> `adjust`

Implementation notes:

- Replacements are case-insensitive.
- Fenced code blocks are preserved verbatim to avoid rewriting command/code examples.
- If context terms are not present, no replacement is performed.
