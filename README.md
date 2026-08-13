# GZA

![Gza - Liquid Swords](/docs/assets/gza-liquid-swords-400w.jpg)

A personal software factory - queues coding tasks for background agents to plan, implement, review, and test in parallel, then auto-merge what passes.

## Key dependencies

- [uv](https://docs.astral.sh/uv/) — for installing GZA and its Python dependencies
- A LLM provider CLI (at least one required):
   - [Claude Code](https://claude.ai/download)
   - [Codex](https://github.com/openai/codex)
- [Docker](https://www.docker.com/) (optional) — runs each task in an isolated container
   - optional, but there are likely more bugs in the non-Docker path as I rarely use it.


## Workflow overview

**Add tasks** — any of:
- `gza add "..."` on the CLI (`--tag` to group tasks by horizon)
- the `/gza-task-add` skill
- conversationally with Claude or Codex, once you've installed the skills (`gza skills-install`)

**Run them** — two modes:
- **Hands-on:** `gza iterate <task> --max-iterations N` drives one task through its review/improve loop; `gza merge <task>` when it's good.
- **Async:** `gza watch --tag <tag>` implements, reviews, and merges matching tasks in a loop.
 

## Further reading

- [Quick Start](docs/quickstart.md) — get started, quickly.
- [Configuration](docs/configuration.md) — every command, option, and setting.
- [Examples](docs/examples/) — parallel workers, bulk import, and plan-implement-review patterns.
