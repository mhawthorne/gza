# gza-server

Local HTTP API and web UI for gza task data.

Run the server CLI through the project launcher:

```bash
server/bin/gza-server start
server/bin/gza-server status
server/bin/gza-server open
server/bin/gza-server logs
server/bin/gza-server restart
server/bin/gza-server stop
```

The launcher works from any working directory and runs the server with the
server subproject environment.

`restart` stops a running server and starts it again, taking the same
`--port` and `--no-reload` flags as `start`. A server that is not running is
not an error: restarting is how you ask for the server to be up.

## Logs

The server writes stdout and stderr to `.gza/gza-server.log` in the project,
which outlives both the process and the terminal that started it.
`gza-server logs` prints its tail and `gza-server status` prints its path. One
generation is rotated to `.gza/gza-server.log.1` once the log grows past a few
megabytes.

## Restart guard

`start` runs uvicorn under a guard process that polls `/api/health`. Uvicorn's
reload supervisor owns the listening socket, so a worker that stops serving
leaves the port accepting connections that are never answered: the server looks
alive while every request hangs. Watching the child's PID cannot see that, so
the guard watches health instead, and recycles the server after several
consecutive failed checks. After repeated restarts it gives up, so the port is
released and `gza-server status` reports the failure rather than hiding it
behind a restart loop. An explicit `gza-server stop` is never fought: the guard
shuts its child down and exits.
