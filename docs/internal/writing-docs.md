# Writing & maintaining docs

How to write gza's user-facing docs (`README.md`, `docs/`). Follow this whenever you add or edit them.

## Style

- Simple language. Short words.
- Bullets over paragraphs.
- Fewest words that work. Cut preamble and recap.
- One idea per paragraph. One clause per bullet.
- Show a command, not a description of the command.

## Scope: document the workflow, not the surface

- Document **the canonical workflow** — the path the maintainer actually uses. Breadth is the enemy; there is too much to document everything.
- The happy path lives in `README.md` and `docs/quickstart.md`.
- The exhaustive reference (every command, flag, config key) lives in `docs/configuration.md`. Link to it; don't inline it.
- If a capability isn't part of the everyday workflow, a one-line mention plus a link is enough.

## The canonical workflow (what the top-level docs should teach)

1. **Install skills** — `gza skills-install`. Shape tasks by talking to the agent; ask for a plan task when the work needs design, else let the agent recommend a path.
2. **Add tasks, tagged by horizon** — tag each task by when it's needed (e.g. `v0.5.0` = critical/for-this-release, `v0.6.0` = soon, `v0.7.0` = later).
3. **Watch, scoped to a tag** — `gza watch --tag <tag>`. Tune `--batch` (concurrency) and `--poll` (loop delay) to backlog size and system stability: more concurrency when things are stable or the backlog is deep (e.g. overnight).
4. **Review & merge** — watch drives units through the lifecycle and merges once code review approves (no `BLOCKER`). Review is the human gate.
5. **Investigate stuck tasks** — read `gza log` output to see what stalled and why.

## Don't

- Don't restate the same thing in prose after a bullet list.
- Don't document flags nobody uses in the top-level docs.
- Don't let README grow into a reference manual — push detail down to `docs/`.
