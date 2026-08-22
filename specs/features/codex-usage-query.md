# Codex Usage Query

> **Status: Proposed**

Query the current user's ChatGPT-backed Codex usage limits without scraping the
Codex web dashboard or reading Codex credentials directly.

## Supported Interface

OpenAI does not document a public HTTP endpoint for personal Codex quota data.
The supported programmatic interface is the Codex app server's
`account/rateLimits/read` JSON-RPC method.

Despite its name, `codex app-server` does not need to run as a daemon. Start it
as a short-lived child process using its default stdio transport, send one
request, read the response, and terminate it. Codex owns ChatGPT authentication
and token refresh throughout this exchange.

References:

- [Codex App Server: getting started](https://learn.chatgpt.com/docs/app-server#getting-started)
- [Codex App Server: auth and rate-limit endpoints](https://learn.chatgpt.com/docs/app-server#auth-endpoints)
- [Codex usage dashboard and `/status`](https://learn.chatgpt.com/docs/pricing#where-can-i-see-my-current-usage-limits)

## Protocol

Start the process:

```text
codex app-server
```

Write these newline-delimited JSON messages to stdin in order:

```jsonl
{"method":"initialize","id":0,"params":{"clientInfo":{"name":"gza","title":"Gza","version":"<gza-version>"}}}
{"method":"initialized","params":{}}
{"method":"account/rateLimits/read","id":1}
```

Continue reading newline-delimited JSON from stdout until the response with
`id: 1` arrives. Notifications and responses with other IDs must be ignored.
The response has this general shape:

```json
{
  "id": 1,
  "result": {
    "rateLimits": {
      "limitId": "codex",
      "primary": {
        "usedPercent": 25,
        "windowDurationMins": 300,
        "resetsAt": 1730947200
      },
      "secondary": null,
      "rateLimitReachedType": null
    },
    "rateLimitsByLimitId": {
      "codex": {
        "limitId": "codex",
        "limitName": null,
        "primary": {
          "usedPercent": 25,
          "windowDurationMins": 300,
          "resetsAt": 1730947200
        },
        "secondary": null
      }
    }
  }
}
```

`resetsAt` is a Unix timestamp in seconds. `usedPercent` is consumed capacity,
so remaining capacity is `max(0, 100 - usedPercent)`.

## Normalized Result

Preserve every returned limit bucket and window rather than assuming there is
only one five-hour limit:

```python
@dataclass(frozen=True)
class CodexUsageWindow:
    limit_id: str
    limit_name: str | None
    window: Literal["primary", "secondary"]
    used_percent: float
    resets_at: datetime
    window_duration_minutes: int

    @property
    def remaining_percent(self) -> float:
        return max(0.0, 100.0 - self.used_percent)
```

Prefer `result.rateLimitsByLimitId` when present. Fall back to the legacy
single-bucket `result.rateLimits` field when it is absent. For each bucket,
emit a normalized entry for every non-null `primary` or `secondary` window.

Do not label a window "five-hour" or "weekly" based on its position. Use the
server-provided `windowDurationMins`, because available buckets and durations
can vary by account, plan, model, and product policy.

## Python Implementation Shape

Use `subprocess.Popen` rather than starting a persistent service:

```python
import json
import subprocess
from typing import Any


def read_codex_rate_limits(timeout_seconds: float = 10) -> dict[str, Any]:
    process = subprocess.Popen(
        ["codex", "app-server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    messages = [
        {
            "method": "initialize",
            "id": 0,
            "params": {
                "clientInfo": {
                    "name": "gza",
                    "title": "Gza",
                    "version": "<gza-version>",
                }
            },
        },
        {"method": "initialized", "params": {}},
        {"method": "account/rateLimits/read", "id": 1},
    ]

    try:
        assert process.stdin is not None
        assert process.stdout is not None
        for message in messages:
            process.stdin.write(json.dumps(message) + "\n")
        process.stdin.flush()

        # Production code must enforce timeout_seconds while reading rather
        # than using this unbounded illustrative loop.
        for line in process.stdout:
            message = json.loads(line)
            if message.get("id") != 1:
                continue
            if "error" in message:
                raise RuntimeError(message["error"])
            return message["result"]
        raise RuntimeError("Codex app server exited without a usage response")
    finally:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
```

The final implementation must enforce a wall-clock timeout across process
startup and response reading. The loop above shows the protocol only; it must
not be copied with an unbounded blocking read.

## Process and Error Handling

- Fail clearly when `codex` is not installed or `app-server` is unsupported.
- Surface an unauthenticated response as an instruction to run `codex login`.
- Treat malformed JSON, JSON-RPC errors, early EOF, and timeout as query
  failures, not as zero usage.
- Capture a bounded stderr tail for diagnostics. Never include authentication
  tokens or the contents of Codex credential files in logs.
- Always terminate and reap the child process, including on cancellation.
- Do not retry authentication or rate-limit failures automatically. A caller
  may retry transient process failures once, but the query must remain cheap
  and bounded.

## Security Boundary

Do not read, copy, log, or parse `~/.codex/auth.json`, browser cookies, access
tokens, or private dashboard requests. Do not call undocumented ChatGPT HTTP
endpoints. The child process inherits the same Codex configuration and login as
an interactive `codex` invocation and keeps credential handling inside Codex.

## Scope

This query reports ChatGPT-backed Codex quota windows. It does not report
OpenAI API-key billing, organization API spend, or API platform rate limits.
Those are separate accounting systems and require separate OpenAI Platform
usage APIs.

## Acceptance Criteria

- A single query starts and stops one local `codex app-server` child process.
- The query performs the required initialize handshake before requesting usage.
- All returned limit IDs and non-null windows are preserved.
- Each result exposes used percentage, remaining percentage, duration, and an
  absolute reset time.
- Missing authentication, unavailable Codex, timeout, malformed output, and
  JSON-RPC errors produce distinct actionable failures.
- No credentials or undocumented HTTP endpoints are accessed directly.
