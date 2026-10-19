#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${GZA_BACKUP_CONFIG:-$HOME/.gza/backup.conf}"
DB_PATH="$HOME/.gza/gza.db"
SNAPSHOT_DATE_COMPACT="$(date +%Y%m%d)"
SNAPSHOT_DATE="$(date +%Y-%m-%d)"
SNAPSHOT_PATH="/tmp/gza-snapshot-${SNAPSHOT_DATE_COMPACT}.db"
PROJECT_DIR="$HOME/work/supreme/gza"
LOGS_DIR="${PROJECT_DIR}/.gza/logs"
LOG_INSIGHTS_DIR="${PROJECT_DIR}/.gza/log-insights"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

read_bucket() {
  [[ -f "$CONFIG_FILE" ]] || die "missing backup config: $CONFIG_FILE
Create it with a bucket key, for example:
  bucket=my-gza-backup-bucket"

  awk -F= '
    /^[[:space:]]*(#|$)/ { next }
    {
      key=$1
      value=substr($0, index($0, "=") + 1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", key)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      if (key == "bucket" || key == "s3_bucket") {
        gsub(/^["'\''"]|["'\''"]$/, "", value)
        print value
        exit
      }
    }
  ' "$CONFIG_FILE"
}

format_size() {
  local path="$1"

  if [[ -f "$path" ]]; then
    wc -c < "$path" | tr -d ' '
  elif [[ -d "$path" ]]; then
    du -sh "$path" | awk '{print $1}'
  else
    printf 'missing'
  fi
}

bucket="$(read_bucket)"
[[ -n "$bucket" ]] || die "missing bucket key in $CONFIG_FILE
Add a line like:
  bucket=my-gza-backup-bucket"

require_command aws
require_command sqlite3

[[ -f "$DB_PATH" ]] || die "missing SQLite database: $DB_PATH"
[[ -d "$LOGS_DIR" ]] || die "missing logs directory: $LOGS_DIR"
[[ -d "$LOG_INSIGHTS_DIR" ]] || die "missing log insights directory: $LOG_INSIGHTS_DIR"

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  die "AWS auth is not active. Log in first with aws sso login or aws configure, then rerun this script."
fi

rm -f "$SNAPSHOT_PATH"
sqlite3 "$DB_PATH" ".backup $SNAPSHOT_PATH"

s3_base="s3://${bucket}"
db_key="${s3_base}/db/gza-${SNAPSHOT_DATE}.db"
logs_key="${s3_base}/logs/"
log_insights_key="${s3_base}/logs/log-insights/"

aws s3 cp "$SNAPSHOT_PATH" "$db_key"
aws s3 sync "$LOGS_DIR/" "$logs_key"
aws s3 sync "$LOG_INSIGHTS_DIR/" "$log_insights_key"

printf '\nBackup complete\n'
printf '  DB snapshot: %s (%s bytes)\n' "$db_key" "$(format_size "$SNAPSHOT_PATH")"
printf '  Logs synced: %s (%s local)\n' "$logs_key" "$(format_size "$LOGS_DIR")"
printf '  Log insights synced: %s (%s local)\n' "$log_insights_key" "$(format_size "$LOG_INSIGHTS_DIR")"
printf '  Local snapshot: %s\n' "$SNAPSHOT_PATH"
