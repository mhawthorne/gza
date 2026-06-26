#!/usr/bin/env bash
# revive-stuck.sh — overnight unattended reviver for parked gza tasks.
#
# YOU launch this (it runs `gza iterate`); it is the "automated tool the user runs"
# pattern, like `gza watch -y`. It does NOT guess --resume vs --retry vs neither:
# it dispatches on the `next_action` field that `gza incomplete --json` already
# computes, and SKIPS anything the system has parked for manual intervention
# (next_action = skip / needs_rebase / needs_discussion / awaiting_human).
#
# Important: most "needs attention" rows are next_action=skip — the system already
# decided automatic recovery won't help (3-cycle no-progress backstop, retry-limit
# reached, GIT_ERROR awaiting the worktree-isolation fix). This script will NOT
# force those; reviving them needs a content-changing rebase or the systemic fixes
# to land, not an iterate flag. It only acts on genuinely-actionable rows.
#
#   chmod +x scripts/revive-stuck.sh
#   nohup ./scripts/revive-stuck.sh > /dev/null 2>&1 &   # launch (then `disown` if needed)
#   tail -f ~/revive-stuck.log                            # watch progress
#   pkill -f revive-stuck.sh                              # stop
#
# Optional first arg = seconds between cycles (default 900 = 15m).

set -uo pipefail

PROJECT="${REVIVE_PROJECT:-/Users/m3h/work/supreme/gza}"   # canonical checkout (shared DB)
INTERVAL="${1:-900}"
LOG="${REVIVE_LOG:-$HOME/revive-stuck.log}"
DONE="$(mktemp -t revive-done.XXXXXX)"
trap 'rm -f "$DONE"' EXIT

cd "$PROJECT" || { echo "cannot cd $PROJECT" >&2; exit 1; }
log() { echo "$(date '+%F %T') $*" >> "$LOG"; }
log "── revive-stuck started (interval ${INTERVAL}s, project $PROJECT)"

# next_action values we will act on, and the iterate flag each needs.
while true; do
  # Pull once; keep only actionable rows as "id<TAB>next_action".
  actionable=$(uv run gza incomplete --json --last 0 2>/dev/null \
    | jq -r '.[]
        | select((.status // "") != "dropped")
        | select((.next_action // "") as $a
                 | ["resume","create_review","improve","create_improve"] | index($a))
        | "\(.id)\t\(.next_action)"' 2>/dev/null)

  # Random pick among actionable rows not already attempted this pass.
  pick=$(printf '%s\n' "$actionable" | grep . | grep -vwF -f "$DONE" 2>/dev/null | sort -R | head -1)

  if [ -n "${pick:-}" ]; then
    id=${pick%%$'\t'*}
    action=${pick##*$'\t'}
    if [ "$action" = "resume" ]; then flag=(--resume); else flag=(); fi
    log "iterate -b ${flag[*]} $id (next_action=$action)"
    uv run gza iterate -b "${flag[@]}" "$id" >> "$LOG" 2>&1
    log "  -> exit $?"
    printf '%s\n' "$id" >> "$DONE"
  else
    log "no fresh actionable task (rest are parked: skip/needs_rebase/awaiting_human); resetting pass"
    : > "$DONE"
  fi

  sleep "$INTERVAL"
done
