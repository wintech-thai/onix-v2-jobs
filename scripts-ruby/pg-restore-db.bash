#!/bin/bash
# pg-restore-db.bash — restore a plain-SQL pg_dump .gz file into a PostgreSQL database
# Usage: pg-restore-db.bash <PG_USER> <BACKUP_FILE_GZ> <TMP_DIR> <PG_PASSWORD> [PG_DB]
set -euo pipefail

PG_USER="$1"
BACKUP_FILE_GZ="$2"
TMP_DIR="$3"
PG_PASSWORD="$4"
PG_DB="${5:-onix}"

export PGPASSWORD="$PG_PASSWORD"

BACKUP_PATH="${TMP_DIR}/${BACKUP_FILE_GZ}"
SQL_PATH="${TMP_DIR}/${BACKUP_FILE_GZ%.gz}"

echo "[1/4] Decompressing ${BACKUP_FILE_GZ}..."
gunzip -f "$BACKUP_PATH"

echo "[2/4] Terminating active connections to ${PG_DB}..."
psql -U "$PG_USER" -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${PG_DB}' AND pid <> pg_backend_pid();" || true

echo "[3/4] Dropping and recreating database ${PG_DB}..."
psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"${PG_DB}\";"
psql -U "$PG_USER" -d postgres -c "CREATE DATABASE \"${PG_DB}\" OWNER \"${PG_USER}\";"

echo "[4/4] Restoring SQL dump..."
psql -U "$PG_USER" -d "$PG_DB" -f "$SQL_PATH"

echo "Restore complete!"
