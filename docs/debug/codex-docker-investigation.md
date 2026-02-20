# Codex Docker Investigation

Date: 2026-02-19
Status: **RESOLVED**

## Summary

OpenAI Codex CLI now works in Docker with both authentication methods.

## Root Causes Found

### 1. Wrong Environment Variable Name

**Issue**: We were using `OPENAI_API_KEY`, but Codex `exec` requires **`CODEX_API_KEY`**.

From the [Codex docs](https://developers.openai.com/codex/noninteractive/):
> "To use a different API key for a single run, set `CODEX_API_KEY` inline"

**Fix**: Changed env var from `OPENAI_API_KEY` to `CODEX_API_KEY`.

### 2. SSL Certificates Missing

**Issue**: `node:20-slim` lacks SSL certificates, causing HTTPS failures.

**Fix**: Added `ca-certificates` to Dockerfile.

### 3. OAuth Works Fine (Earlier Assumption Wrong)

**Original assumption**: OAuth doesn't work in Docker due to websocket/network issues.

**Reality**: OAuth works perfectly when `~/.codex` is mounted writable. The earlier failures were due to read-only mounts causing cache write errors.

## Final Solution

Auth priority in `_get_docker_config()`:
1. If `~/.codex/auth.json` exists → mount `~/.codex` (OAuth, ChatGPT pricing)
2. Otherwise → pass `CODEX_API_KEY` env var (API pricing)

## Test Results (Final)

| Test | Result |
|------|--------|
| `codex --version` in Docker | Pass |
| `codex exec` in Docker with OAuth (~/.codex mounted) | Pass |
| `codex exec` in Docker with CODEX_API_KEY | Pass |
| `codex exec` outside Docker | Pass |

## Files Modified

1. `src/gza/providers/codex.py` - Auth logic: prefer OAuth, fallback to CODEX_API_KEY
2. `etc/Dockerfile.codex` - Add ca-certificates
3. `tests_integration/test_docker.py` - Separate tests for OAuth and API key auth
4. `docs/docker.md` - Updated Codex documentation

## Commands for Debugging

```bash
# Test OAuth auth
docker run --rm -v ~/.codex:/home/gza/.codex -v /tmp/test:/workspace -w /workspace gza-codex codex exec --json --skip-git-repo-check "Say hello"

# Test API key auth
source ~/.gza/.env && docker run --rm -e CODEX_API_KEY="$CODEX_API_KEY" -v /tmp/test:/workspace -w /workspace gza-codex codex exec --json --skip-git-repo-check "Say hello"

# Check API key is passed
docker run --rm -e CODEX_API_KEY="$CODEX_API_KEY" gza-codex sh -c 'echo "CODEX_API_KEY length: ${#CODEX_API_KEY}"'
```
