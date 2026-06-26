# My Workflow

## Ensure latest GZA skills are installed for Claude and Codex in your env

TODO: which commands is this required for?  review, rebase, ?

This is only necessary if you want to use GZA stuff in interactive conversations.

```bash
$ gza skills-install --update
```


## List queue for specific tag (shows first 50 tasks)

```bash
$ gza queue --tag v0.5.0 -n 50
```


## Run 1 task at a time, checking every 30 minutes to see if a new task should be started

```bash
$ gza watch --tag v0.5.0 --batch 1 --poll 1800
```


## Look at pending tasks

```bash
$ gza next
```

## Run next 3 pending tasks in the background

3 could be any number.  `-b` is short for `--background`

```bash
$ gza work -b --count 3
```

## Run a task with 5 implement/review cycles

```bash
$ gza iterate -i 5 -b <task_id>
```

## Track running tasks

Failed tasks rise to the top of the list.

```bash
$ gza ps --poll
```

## Live logs for multiple running tasks

```bash
$ gza tv
```


## Find recently failed tasks

(these should be captured by the `incomplete` command above)

```bash
gza history --status failed
```


## Find plan or exploretasks that were never implemented

```bash
gza advance --unimplemented
```

You can run with `--create` to automatically create implement tasks for the listed tasks.
But chances are you'll want to review them first.

You can manually review plans using the `gza-plan-review` skill to refine before implementing.


## Implement a specific plan

```bash
gza implement <plan_task_id>
(grab new pending implement task i)
gza iterate -i 5 -b <implement_task_id>


## Drop tasks that are "incomplete" but don't need follow-up

If you have a plan/explore task that you will never follow up on, mark it as dropped.

```bash
$ gza set-status <task_id> dropped
```


## Extract changes from a branch into a new task

```bash
$ uv run gza extract --branch $branch --base-branch $base_branch
$ uv run gza iterate <new-implement-task-id>
```

Bare `gza implement <plan_task_id>` and `uv run gza extract ...` queue the new implementation task by default. Add `--run` when you want immediate foreground execution instead of queueing.
