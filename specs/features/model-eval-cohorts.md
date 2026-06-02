# Model Evaluation Cohorts

A sampling-based evaluation mode that runs multiple model/provider configurations
on the same logical task and lets a winner be picked — scored, judged, or
chosen by a human. The goal is to compare planner × implementer × reviewer
combinations on real work without the cost of running every task through
multiple configurations.

## Problem

Today gza runs each task on a single configuration. The derived review score
(specs/../gza-0000mw) and the per-model analytics (specs/../gza-0000mx) give
useful per-reviewer aggregates, but they can't answer "does Claude-plan +
Codex-impl + Claude-review beat Codex-plan + Claude-impl + Codex-review on the
same task?" because the different configurations never see the same input.

Absolute scoring by an LLM judge is known to be unstable across runs and
incomparable across judges (grade compression, calibration drift). Pairwise
preference aggregation — the technique behind Chatbot Arena — is the reliable
alternative: force the judge to pick a winner between two outputs, aggregate
thousands of preferences into a ranking via Bradley-Terry or Elo. That needs a
controlled setup where two configurations produce outputs from identical
inputs.

Running every task on N configurations doubles or triples cost. The goal is a
**toggle** that collects a bounded number of samples when enabled and reverts
to single-configuration mode when disabled or exhausted.

## Scope and non-goals

### In scope

- A DB-backed eval mode with a sample budget that auto-disables when exhausted.
- Task **cohorts**: when eval mode is active, a new plan/implement/review task
  is expanded into N sibling tasks, one per configured variant. Each sibling is
  a first-class task in the DB with its own ID, branch, log, and worktree.
- Selection of a winner per cohort via three flavors:
  1. **Score-based** (cheap, deterministic): the sibling with the highest
     `review_score` wins. Works for reviews today; extension to plans/impls
     requires a comparable score for those types and is a follow-up.
  2. **Judge-based** (expensive, more reliable): a judge task runs pairwise
     comparisons on the cohort's outputs and records preferences.
  3. **Human pick** (always available): `gza eval choose <cohort> <winner>`.
- Downstream lineage resolution: `gza implement <plan-id>`, auto-created
  reviews, improve tasks, etc. resolve to the winning sibling of a cohort when
  one exists.
- Determinism for retry/resume: a retry of a cohort sibling stays on that
  sibling's specific configuration. Eval mode never fires on retries.

### Non-goals

- **Fan-out within a single task.** Each configuration runs as its own task.
  There is no "one task, multiple model calls" mode.
- **Per-task opt-in.** Eval is a mode toggle, not a flag on `gza add` or
  `gza work`. Users turn it on for a window, collect samples, turn it off.
- **Automatic merging of winning diffs.** Winners feed downstream lineage
  resolution, but the existing `gza merge` flow still runs normally on the
  winning sibling's branch.
- **A scoring system for plans and implements.** `review_score` exists today;
  plan/impl scoring is a follow-up. Until then, score-based selection only
  works for review cohorts and the other types need judge-based or human
  selection.
- **Backfilling historical cohorts.** Pre-feature tasks have no cohort.
- **Cross-repo evaluation.** One project directory, one eval state.
- **Judge prompt template design.** Noted as a follow-up; this spec defines
  the interface the judge plugs into but not the prompt itself.

## Data model

### New columns on `tasks`

| column | type | nullable | meaning |
|---|---|---|---|
| `eval_cohort_id` | TEXT | yes | Foreign key to `eval_cohorts.id`. NULL for non-eval tasks. |
| `eval_config` | TEXT (JSON) | yes | The configuration this sibling was created with, e.g. `{"provider": "codex", "model": "gpt-5.4"}`. NULL for non-eval tasks. Siblings within a cohort have distinct `eval_config` values. |
| `eval_winner` | INTEGER (bool) | yes | 1 if this sibling was picked as the cohort winner, 0 otherwise, NULL for non-eval tasks. |

### New table `eval_cohorts`

| column | type | meaning |
|---|---|---|
| `id` | TEXT PRIMARY KEY | Cohort identifier, e.g. `cohort-20260410-aa`. |
| `created_at` | TEXT (ISO) | When the cohort was created. |
| `task_type` | TEXT | `plan`, `implement`, or `review`. |
| `logical_prompt` | TEXT | The single prompt that was expanded into siblings, preserved for later comparison. |
| `depends_on` | TEXT | Inherited from the logical task (e.g. the impl being reviewed, or the plan being implemented). NULL when the logical task had no dependency. |
| `based_on` | TEXT | Same inheritance, for lineage. |
| `selection_method` | TEXT | `score`, `judge`, `human`, or NULL (not yet selected). |
| `winner_task_id` | TEXT | The winning sibling's task ID. NULL until selection. |
| `judge_task_id` | TEXT | If selection method is `judge`, the task ID of the judge task that produced the preferences. NULL otherwise. |
| `eval_run_id` | TEXT | Foreign key to `eval_runs.id`. |

### New table `eval_runs`

One row per "turn the mode on" session.

| column | type | meaning |
|---|---|---|
| `id` | TEXT PRIMARY KEY | `eval-20260410-abc`. |
| `started_at` | TEXT (ISO) | When the run was enabled. |
| `ended_at` | TEXT (ISO) | When the run was disabled (explicitly or by budget exhaustion). NULL while active. |
| `configs_json` | TEXT (JSON) | Array of configuration objects, e.g. `[{"provider":"claude","model":"claude-opus-4-6"},{"provider":"codex","model":"gpt-5.4"}]`. |
| `samples_target` | INTEGER | How many cohorts the user asked for. |
| `samples_collected` | INTEGER | How many cohorts have been created under this run. |
| `apply_to_types` | TEXT (JSON) | Array subset of `["plan","implement","review"]`. Default `["review"]` because it's the cheapest and the only one with a comparable score. |
| `selection_method` | TEXT | Default selection method for cohorts in this run (`score`, `judge`, `human`). |

**At most one `eval_runs` row with `ended_at IS NULL` at a time.** Enforced at
the application level (the CLI refuses to start a new run while one is active).

### New table `eval_preferences`

Populated by judge-based selection. One row per pairwise comparison.

| column | type | meaning |
|---|---|---|
| `cohort_id` | TEXT | FK to `eval_cohorts`. |
| `winner_task_id` | TEXT | The sibling picked. |
| `loser_task_id` | TEXT | The sibling rejected. |
| `judge_model` | TEXT | For later calibration — e.g. `claude-opus-4-6`. |
| `rationale` | TEXT | Short reason the judge gave. |

Rankings (Bradley-Terry / Elo) are computed on demand from this table in the
analytics path, not stored.

## Control flow

### Turning eval on

```
gza eval start \
    --samples 10 \
    --types review \
    --config 'claude:claude-opus-4-6' \
    --config 'codex:gpt-5.4' \
    --selection score
```

- Creates an `eval_runs` row with `ended_at = NULL`.
- Refuses to start if another run is already active.
- Validates the configs by instantiating providers (same check used by
  `gza work --provider` today).
- Prints the run ID and a one-line reminder of how to stop it.

### Turning eval off

```
gza eval stop           # explicit
```

- Sets `ended_at` on the active run and prints a summary:
  `Collected 7/10 cohorts. Run gza eval results <run-id> for details.`

Eval also auto-stops when `samples_collected == samples_target`.

### Cohort creation trigger

Eval mode intercepts task creation at a single point in `SqliteTaskStore.add()`
(the path every `gza` command eventually funnels through). After the normal
insert, it checks:

1. Is there an active `eval_runs` row?
2. Does the new task's `task_type` appear in the run's `apply_to_types`?
3. Is the task a fresh task, not a retry or resume? (retries have `based_on`
   pointing at a terminal task of the same type; resumes reuse an existing
   task ID entirely).
4. Is `samples_collected < samples_target`?

If all four are true, the store opens a transaction that:

- Creates a new `eval_cohorts` row.
- Marks the just-inserted task as sibling #1 (`eval_cohort_id` = new cohort,
  `eval_config` = the active run's first config, or the config matching the
  task's own provider/model if it matches one of the run's configs).
- Inserts N-1 additional sibling tasks with the same logical prompt, same
  `depends_on` / `based_on`, each with a distinct `eval_config`.
- Increments `samples_collected` on the `eval_runs` row.

After the transaction commits, the cohort siblings run through the normal
executor. Nothing special happens at execution time.

### Executor: no changes at execution time

Siblings are ordinary tasks. The runner doesn't know or care about cohorts.
The only execution-time touch is that the provider/model is resolved from the
task's `eval_config` column when present, overriding the project default.

This keeps the existing runner code untouched and means the slug-generation
fix (gza-0000mr), the prune-logic fix (gza-0000mq), the review score
(gza-0000mw), etc. all work uniformly on eval siblings.

### Winner selection

Selection runs when all siblings in a cohort reach terminal state (completed,
failed, dropped). The cohort's selection method determines what happens:

- **`score`**: read the `review_score` of each sibling (gza-0000mw). If no
  score is available for all siblings (e.g. `task_type == "implement"`, where
  there's no score yet), fall back to `human`. Otherwise: pick the highest
  score, break ties by earliest `completed_at`. Write `winner_task_id` and set
  the winning sibling's `eval_winner = 1`.
- **`judge`**: create a `judge` task (new task type, or reuse `internal`)
  that reads all sibling outputs and produces pairwise preferences. Once the
  judge task completes, populate `eval_preferences`, compute a ranking, and
  mark the top-ranked sibling as the winner. The judge task's prompt template
  is a follow-up design item.
- **`human`**: do nothing. The cohort sits in a `pending selection` state
  until `gza eval choose <cohort> <winner-id>` is run manually.

Selection is implemented as a lifecycle hook on the cohort, not as a polling
loop. The cleanest place to put it is in the same path that marks tasks
terminal (`runner.py`'s completion handling): after marking a sibling
terminal, check if all siblings in its cohort are terminal and trigger
selection if so.

### Downstream lineage resolution

When any command resolves a task that might be part of a cohort — the most
important cases being `gza implement <plan-id>` and auto-review creation —
the resolution follows this rule:

- If the referenced task has `eval_cohort_id IS NULL`, resolve directly to it
  (unchanged).
- If it has a cohort ID and the cohort has `winner_task_id IS NOT NULL`,
  resolve to the winner.
- If it has a cohort ID but no winner yet, refuse the operation with an error:
  `Task <id> is part of eval cohort <cohort-id> which has no winner. Run 'gza eval choose' or wait for selection to complete.`

This gives eval mode the "pick one, feed downstream" property without
requiring any manual ID tracking on the user's part.

### Retry and resume

- **Retry** (`gza retry <id>`): if the target is an eval sibling, the new
  retry task inherits `eval_cohort_id` and `eval_config` but NOT `eval_winner`.
  It belongs to the cohort but doesn't auto-win — if you want it to replace
  the current winner, re-run selection manually.
- **Resume** (`gza resume <id>`): reuses the same task ID, so eval fields
  are untouched. The config stays the same.
- **Eval mode never fires on retries or resumes.** Cohort creation only
  triggers on fresh tasks (check #3 in "Cohort creation trigger" above). This
  keeps the experimental dataset clean.

### Visibility filters

Eval siblings are real tasks and will show up in `gza history`, `gza unmerged`,
`gza ps`, etc. by default. Add a `--eval` / `--no-eval` flag family:

- `gza history --no-eval`: hide eval-cohort tasks. Default for daily workflow.
- `gza history --eval-only`: show only eval tasks. Useful for cohort review.
- `gza unmerged --no-eval`: ditto.

The default behavior should probably be `--no-eval` when eval mode is off, and
plain (no filter) when eval mode is on, so that an active eval run is visible
in the operator's normal views without cluttering them when eval isn't in use.

### Results and analytics

```
gza eval status                  # current run, if any
gza eval list                    # past runs
gza eval results <run-id>        # per-cohort breakdown
gza eval ranking <run-id>        # Bradley-Terry / Elo on judge preferences
```

`gza eval results` should show, per cohort: the logical prompt, the sibling
IDs + configs + scores, the winner, and the selection method. JSON output via
`--json` for downstream tooling.

`gza eval ranking` only makes sense for judge-based runs. It aggregates
`eval_preferences` rows and prints a ranking:

```
Rank  Config                       Wins  Losses  Rating
1     codex:gpt-5.4                 12    3       1654
2     claude:claude-opus-4-6        9     6       1573
...
```

## Open questions

These need to be resolved during implementation but don't need to be decided
before the spec is approved.

1. **Sample counting semantics.** Does `--samples 10` mean "10 cohorts" or
   "10 sibling tasks" or "10 tasks worth of cost budget"? The spec currently
   says 10 cohorts. Discussion welcome.
2. **Cohort concurrency.** Do siblings in a cohort run in parallel or serially?
   Parallel cuts wall-clock time but multiplies peak cost and concurrent
   worker load. The existing worker pool behavior governs this; the spec
   doesn't change it. If the current pool runs N tasks in parallel by
   default, eval cohorts will naturally use that capacity.
3. **Failure semantics.** If 2 of 3 siblings succeed and 1 fails, does the
   cohort get a winner from the 2 survivors, or is it considered failed
   until the 3rd is retried? The spec currently implies "pick from survivors"
   by saying "all siblings in terminal state." A failed sibling is terminal.
   Documented here so reviewers can push back.
4. **Eval mode and `gza advance`.** `gza advance` can create review tasks
   automatically as part of its lifecycle walk. Eval cohort creation should
   fire on those same as any manual creation. Verify that the interception
   point in `SqliteTaskStore.add()` covers this path.
5. **Config matching.** If the operator's default provider already matches
   one of the configs in the active run, does sibling #1 reuse the existing
   task (the one the user `gza add`ed) or is it a fresh sibling with the
   user's original task discarded? The spec currently says the just-inserted
   task becomes sibling #1 if its provider/model matches a run config; if
   not, the run creates N siblings and the original task becomes one of them
   with its `eval_config` set. Worth confirming this doesn't produce weird
   lineage.
6. **Per-task-type score comparability.** Score-based selection only works
   today for reviews. For plan and implement cohorts, the spec defaults to
   human selection. A follow-up spec should explore whether a "plan quality
   score" or "implementation quality score" is feasible without also becoming
   a brittle LLM-scored metric.
7. **Cohort ID format.** `cohort-YYYYMMDD-aa` or something shorter tied to
   the run ID? Low stakes; pick whatever matches existing conventions in
   `src/gza/db.py`.

## Interactions with other specs and in-flight work

- **gza-0000mr (slug collision fix).** Eval siblings must have distinct
  slugs. The `<date>-<task-id-suffix>-<type>-<target-slug>` format from
  gza-0000mr guarantees this because each sibling has its own task ID.
- **gza-0000mw (derived review score).** Required for score-based selection
  on review cohorts. Eval mode should check for its presence and degrade to
  human selection when `review_score` is NULL.
- **gza-0000mx (per-model analytics).** Eval analytics overlap with this
  path — `gza stats reviews` groups by model, `gza eval ranking` groups by
  cohort configuration. Implementations should share the grouping helpers
  instead of duplicating them.
- **gza-0000ma (review/plan contract tightening).** The spec assumes reviews
  include the full ask (plan or prompt). Eval mode's cohort reviews rely on
  this to produce comparable reviews across siblings.
- **gza-0000mq (worker prune fix).** Eval cohorts produce many more worker
  registry entries; the fixed prune logic becomes important to prevent
  registry bloat.

## Implementation sketch (rough)

A rough order for whoever picks this up. Not prescriptive.

1. Schema migration: add the new columns and tables.
2. CLI scaffold for `gza eval start` / `stop` / `status` / `list` (no cohort
   creation yet — just the `eval_runs` row lifecycle).
3. Cohort creation interception in `SqliteTaskStore.add()`.
4. Per-task provider/model override from `eval_config`.
5. Terminal-state lifecycle hook that triggers selection.
6. Score-based selection.
7. Visibility filters (`--no-eval`, `--eval-only`).
8. `gza eval choose` (human selection).
9. `gza eval results` with text and JSON output.
10. Judge-task integration (after the judge prompt template is designed in
    a follow-up).
11. `gza eval ranking` with Bradley-Terry / Elo aggregation.

Each step is independently useful; the feature is functional at step 9 even
without the judge pipeline.
