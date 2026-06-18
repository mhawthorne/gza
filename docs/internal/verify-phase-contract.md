# Verify Phase Contract

Autonomous review verification parses structured phase lines from verify-command
stdout. The contract is line-oriented and append-only: wrappers may print
additional output, but phase records must appear exactly as documented here.

## Start line

Before a phase runs, emit:

```text
gza-verify phase=start name=<name>
```

- `<name>` must match `[A-Za-z0-9_.-]+`.
- This line is informational and marks the phase boundary for operators.

## Result line

After the phase completes, emit exactly one result line:

```text
gza-verify phase=<passed|failed> name=<name> duration_seconds=<float>
```

Optional exact-tree checkpointing appends:

```text
 tree_fingerprint=<64-hex>
```

Full examples:

```text
gza-verify phase=passed name=ruff duration_seconds=0.842317 tree_fingerprint=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
gza-verify phase=failed name=functional duration_seconds=12.500000
```

## Fingerprint rule

- Emit `tree_fingerprint=` only when the harness can compute an exact current tree
  fingerprint.
- If fingerprinting is unavailable, returns `None`, or raises because the gitdir is
  unavailable, omit the entire ` tree_fingerprint=...` suffix.
- If the fingerprint probe returns a non-empty value that does not match
  `[0-9a-f]{64}`, warn on stderr and omit the suffix rather than emitting an
  unparseable result line.
- When the underlying fingerprint probe reports an unavailable gitdir, keep the
  result line parseable and expect the diagnostic warning on stderr rather than in
  the structured stdout record.
- If fingerprinting fails unexpectedly, keep the same result-line format and emit a
  diagnostic warning separately; do not replace the structured phase record with a
  traceback or alternate stdout line.
- Do not emit placeholders such as `None`, `unknown`, or an empty value.

## Consumer behavior

- `src/gza/runner.py` parses only `phase=passed` and `phase=failed` result lines.
- Timeout-resume guidance may reuse recorded successful phases only when the saved
  `tree_fingerprint` matches the exact tree state being resumed.
