#!/bin/bash
# db-restore-bitnami.bash <DB_TYPE> <BACKUP_FILE_GZ> <TARGET_DIR>
#
# Runs inside a Bitnami PostgreSQL/MySQL pod — counterpart to
# db-dump-bitnami.bash. User/database/password are read from the pod's own
# env vars, same as the dump script.

DB_TYPE="$1"
BACKUP_FILE="$2"
TARGET_DIR="$3"
BACKUP_PATH="$TARGET_DIR/$BACKUP_FILE"

if [ ! -f "$BACKUP_PATH" ]; then
  echo "ERROR: Backup file not found: $BACKUP_PATH"
  exit 1
fi

case "$DB_TYPE" in
  postgresql)
    DB_USER="${POSTGRES_USER}"
    DB_NAME="${POSTGRES_DB:-$POSTGRES_DATABASE}"
    export PGPASSWORD=$(cat "${POSTGRES_PASSWORD_FILE}")
    echo "=== Restoring $BACKUP_PATH into PostgreSQL db=[$DB_NAME] user=[$DB_USER] ==="

    # Terminate existing connections, then drop and recreate the database
    psql -U "$DB_USER" -d postgres -c \
      "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();" \
      2>/dev/null || true
    psql -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";"
    psql -U "$DB_USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";"

    gunzip -c "$BACKUP_PATH" | psql -U "$DB_USER" -d "$DB_NAME"
    EXIT_CODE=$?
    ;;
  mysql)
    DB_USER="${MYSQL_USER:-root}"
    DB_NAME="${MYSQL_DATABASE}"
    if [ "$DB_USER" = "root" ]; then
      DB_PASSWORD=$(cat "${MYSQL_ROOT_PASSWORD_FILE}")
    else
      DB_PASSWORD=$(cat "${MYSQL_PASSWORD_FILE}")
    fi
    echo "=== Restoring $BACKUP_PATH into MySQL db=[$DB_NAME] user=[$DB_USER] ==="

    mysql -u"$DB_USER" -p"$DB_PASSWORD" -e "DROP DATABASE IF EXISTS \`$DB_NAME\`; CREATE DATABASE \`$DB_NAME\`;"
    gunzip -c "$BACKUP_PATH" | mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME"
    EXIT_CODE=$?
    ;;
  *)
    echo "ERROR: unsupported DB_TYPE [$DB_TYPE] (expected postgresql or mysql)"
    exit 1
    ;;
esac

if [ $EXIT_CODE -eq 0 ]; then
  echo "=== Restore completed successfully ==="
else
  echo "=== Restore failed with exit code $EXIT_CODE ==="
  exit $EXIT_CODE
fi
