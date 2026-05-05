#!/usr/bin/env bash
# Rename a project_id across all tables in the shared gza DB.
#
# Usage: bin/rename-project-id.sh [OLD_ID] [NEW_ID] [DB_PATH]
#   OLD_ID   default: gzarepo01
#   NEW_ID   default: gza
#   DB_PATH  default: /Users/m3h/work/supreme/gza/.gza/gza.db

set -euo pipefail

OLD_ID="${1:-gzarepo01}"
NEW_ID="${2:-gza}"
DB="${3:-/Users/m3h/work/supreme/gza/.gza/gza.db}"

if [[ ! -f "$DB" ]]; then
  echo "error: db not found: $DB" >&2
  exit 1
fi

if [[ ! "$NEW_ID" =~ ^[a-z0-9]{1,64}$ ]]; then
  echo "error: NEW_ID '$NEW_ID' must match ^[a-z0-9]{1,64}$ (gza's _PROJECT_ID_RE)" >&2
  exit 1
fi

# Bail if anything has the DB open. WAL means concurrent readers are safe in
# general, but a running TUI/watcher will get confused mid-rename.
if command -v lsof >/dev/null 2>&1; then
  holders=$(lsof -- "$DB" 2>/dev/null | tail -n +2 || true)
  if [[ -n "$holders" ]]; then
    echo "error: db is open by another process. Stop these first:" >&2
    echo "$holders" >&2
    exit 1
  fi
fi

# Confirm OLD_ID actually exists.
count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM projects WHERE id='$OLD_ID';")
if [[ "$count" != "1" ]]; then
  echo "error: projects has $count rows with id='$OLD_ID' (expected 1)" >&2
  exit 1
fi
# And that NEW_ID doesn't.
collision=$(sqlite3 "$DB" "SELECT COUNT(*) FROM projects WHERE id='$NEW_ID';")
if [[ "$collision" != "0" ]]; then
  echo "error: projects already has a row with id='$NEW_ID'" >&2
  exit 1
fi

backup="${DB%.db}.backup.pre-rename-$(date +%Y%m%d-%H%M%S).db"
echo "backing up: $DB -> $backup"
cp -- "$DB" "$backup"

echo "renaming '$OLD_ID' -> '$NEW_ID' in $DB"
sqlite3 "$DB" <<SQL
BEGIN;
UPDATE projects          SET id='$NEW_ID'         WHERE id='$OLD_ID';
UPDATE project_sequences SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
UPDATE tasks             SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
UPDATE task_tags         SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
UPDATE run_steps         SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
UPDATE run_substeps      SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
UPDATE task_comments     SET project_id='$NEW_ID' WHERE project_id='$OLD_ID';
DELETE FROM projects     WHERE id='default';
COMMIT;
SQL

echo "done. verify:"
sqlite3 -header -column "$DB" "SELECT id, project_name, project_prefix FROM projects;"

cat <<EOF

Next: update gza.yaml in every working tree that points at this DB:
  /Users/m3h/work/supreme/gza/gza.yaml
  /Users/m3h/work/supreme/worktrees/gza-agent-sessions/gza.yaml
  (and any other worktrees with .gza -> /Users/m3h/work/supreme/gza/.gza)
Change 'project_id: $OLD_ID' to 'project_id: $NEW_ID'.
EOF
