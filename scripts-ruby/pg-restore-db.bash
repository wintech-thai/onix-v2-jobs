#!/bin/bash
# pg-restore-db.bash <PG_USER> <BACKUP_FILE_GZ> <TMP_DIR> <PG_PASSWORD> <PG_DB>

PG_USER="$1"
BACKUP_FILE="$2"
TMP_DIR="$3"
PG_PASSWORD="$4"
PG_DB="${5:-onix}"

export PGPASSWORD="$PG_PASSWORD"

BACKUP_PATH="$TMP_DIR/$BACKUP_FILE"

if [ ! -f "$BACKUP_PATH" ]; then
  echo "ERROR: Backup file not found: $BACKUP_PATH"
  exit 1
fi

echo "=== Restoring $BACKUP_PATH to database '$PG_DB' ==="

# Terminate existing connections to the target database
psql -U "$PG_USER" -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$PG_DB' AND pid <> pg_backend_pid();" \
  2>/dev/null || true

# Drop and recreate target database
echo "=== Recreating database '$PG_DB' ==="
psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$PG_DB\";"
psql -U "$PG_USER" -d postgres -c "CREATE DATABASE \"$PG_DB\";"

# Restore from gzipped SQL dump
echo "=== Restoring SQL dump ==="
gunzip -c "$BACKUP_PATH" | psql -U "$PG_USER" -d "$PG_DB"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "=== Restore completed successfully ==="
else
  echo "=== Restore failed with exit code $EXIT_CODE ==="
  exit $EXIT_CODE
fi
