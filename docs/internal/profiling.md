# Profiling with py-spy

[py-spy](https://github.com/benfred/py-spy) is a sampling profiler for Python.
It periodically reads the call stack of a running Python process and aggregates
the samples into a flame graph. It does not instrument the process, so overhead
is low (typically 1–5% at the default sample rate) and it works on long-running
production-style workloads.

This document covers how to use it against the gza app: against a fresh
invocation, by attaching to an already-running process, and what the three knobs
you'll touch most (`--subprocesses`, `--rate`, `--idle`) actually do.

## Install / invocation

py-spy is not a project dependency. Run it via `uvx` so it doesn't pollute the
project:

```
uvx py-spy --help
```

On macOS, sampling another process requires `sudo` because py-spy uses
`task_for_pid` to read the target's memory. On Linux it needs `CAP_SYS_PTRACE`
(typically also via sudo).

After a sudo'd run, the output SVG will be owned by root. Either run
`sudo chown $USER:staff <file>` or pipe through a non-sudo'd shell step to fix
it.

## Profile a fresh invocation

For a command you're about to launch, let py-spy spawn it. This avoids the race
where py-spy tries to attach to a short-lived process and times out:

```
sudo uvx py-spy record \
  -o tmp/flamegraph-$(date +%Y%m%d%H%M%S).svg \
  --rate 250 --subprocesses \
  -- .venv/bin/python -m gza watch
```

Notes:

- Invoke the venv Python directly (`.venv/bin/python -m gza ...`) instead of
  `uv run gza ...`. `uv run` spawns Python as a subprocess, which adds a layer
  between py-spy and the actual workload. With `--subprocesses` py-spy can
  follow it, but skipping the wrapper is simpler.
- Stop sampling with Ctrl-C. py-spy will write the SVG and exit cleanly.

## Attach to a running process

For a `gza watch` that's already been running for a while, find the pid and
attach:

```
pgrep -f "gza watch"
sudo uvx py-spy record \
  -o tmp/flamegraph-$(date +%Y%m%d%H%M%S).svg \
  --rate 250 --subprocesses \
  --pid <pid> --duration 120
```

`--duration <seconds>` is useful here. Without it, py-spy samples until you
Ctrl-C — fine interactively, awkward in a script. Pick a window where you
expect actual work to happen (a task firing, a verify run), not a quiet
idle stretch.

## The flags

### `--rate <Hz>`

How often py-spy reads the stack. Default is 100Hz (every 10ms).

- **Higher rate** catches faster functions. At 100Hz, anything that runs in
  <10ms rarely lands on the stack and effectively disappears from the
  flame graph. Bump to `--rate 250` or `--rate 500` if your hot path is fast
  per-call but runs many times. `--rate 1000` is the practical ceiling on
  most laptops.
- **Higher rate costs more.** Overhead scales with sampling frequency. 100Hz
  is ~1%, 1000Hz is closer to 5–10%. Fine for diagnostic runs, less ideal
  for "always on" sampling of a production-ish workload.
- **Native code is still invisible.** Sample rate doesn't help with C
  extensions (SQLite, JSON parsing, regex). Those samples land on the
  Python frame that called them, so they look like "all time is in
  `sqlite3.execute`" even though the work is happening in C.

For most gza profiling, 250 is a reasonable default. Use 1000 if you suspect
fast-but-frequent hot spots are hiding.

### `--subprocesses`

By default, py-spy samples only the pid it was given. With `--subprocesses`,
it follows `fork()` and `exec()` and samples child Python processes too,
aggregating them into the same flame graph.

When you need it:

- **`uv run` workflows.** `uv run python -m gza ...` runs Python as a child
  of `uv`. Without `--subprocesses`, py-spy samples `uv` itself, which is
  Rust, and you'd get nothing useful.
- **pytest with xdist.** Workers are subprocesses.
- **gza features that shell out.** `gza watch` orchestrates `claude` /
  `gza run-inline` subprocesses. Without `--subprocesses` you only see the
  watch loop itself; with it, you see what the children are doing too.

It's almost always the right default for gza workloads. The cost is small
(extra bookkeeping) and the loss of visibility without it is large.

### `--idle`

By default py-spy includes idle frames — moments where the thread is blocked
on I/O, waiting on a subprocess, sleeping, etc. These show up as wide flat
bars at functions like `select.select`, `subprocess.Popen.wait`, or
`time.sleep`.

`--idle` is a *toggle* in py-spy's CLI: it changes which samples are recorded.
Run `uvx py-spy record --help` to confirm the exact semantics for the version
you have installed (the flag has shifted between releases), but in general:

- **With idle samples included (default behavior in recent versions):** the
  flame graph reflects wall-clock time. Useful for "where is the process
  spending its hour?" — including the time it spent waiting.
- **With idle samples excluded:** the flame graph reflects only on-CPU time.
  Useful for "what is the process actually computing?" — drops out the
  I/O waits and shows the CPU-bound work.

For a `gza watch` profile, the default (wall-clock) view is usually what you
want first — gza spends a lot of time waiting on Claude API calls and child
processes, and seeing those waits in the flame graph is informative. Switch
to on-CPU-only when you've identified a CPU-bound hot spot and want to drill
into it without I/O drowning out the signal.

## Reading the flame graph

Open the SVG in a browser:

```
open tmp/flamegraph-<tstamp>.svg
```

- **Width = cumulative sample time** (self + descendants). Wide blocks are
  where the process spent the most wall-clock time, whether through one slow
  call or many fast ones.
- **Y-axis = stack depth.** The bottom is the entry point; functions higher
  up are deeper in the call stack.
- **X-axis ordering is alphabetical**, not chronological. A function that ran
  twice with different intermediate state appears as two adjacent bars, not
  one merged bar.
- **Click any block to zoom in;** the view rescales so that block is 100%
  width. Click "Reset Zoom" (top left) to return.
- **Ctrl-F searches function names** and highlights every matching frame. Use
  this to answer "how much total time across all callers does function X
  consume?" — the search result shows aggregate percentage.

## When py-spy isn't the right tool

- **Sub-millisecond Python code.** Even at 1000Hz you may not catch it. Use
  `cProfile` or `line_profiler` for deterministic instrumentation if you
  need that resolution.
- **Async code with many short tasks.** py-spy sees the running coroutine at
  each sample, but the visualization can be misleading when many coroutines
  rotate through the same thread. `--threads` shows per-thread breakdowns
  but doesn't fully solve this.
- **Memory profiling.** py-spy is CPU/time only. Use `memray` or
  `tracemalloc` for memory.

## Output convention

By repo convention, drop profile artifacts in `tmp/` with a sortable
timestamp suffix:

```
tmp/flamegraph-20260519113123.svg
```

This matches the pattern used by `src/gza/test_latency.py` and keeps profile
output out of git history.
