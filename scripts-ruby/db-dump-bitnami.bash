#!/bin/bash
# db-dump-bitnami.bash <DB_TYPE> <OUT_FILE> <TARGET_DIR>
#
# Runs inside a Bitnami PostgreSQL/MySQL pod. User/database/password are read
# from the pod's own env vars (set by the Bitnami chart itself) so nothing
# needs to be passed in or hardcoded here — this stays generic across apps.

DB_TYPE="$1"
OUT_FILE="$2"
TARGET_DIR="$3"

cd "$TARGET_DIR" || exit 1
rm -f "$OUT_FILE" "$OUT_FILE.gz"

case "$DB_TYPE" in
  postgresql)
    DB_USER="${POSTGRES_USER}"
    DB_NAME="${POSTGRES_DB:-$POSTGRES_DATABASE}"
    export PGPASSWORD=$(cat "${POSTGRES_PASSWORD_FILE}")
    echo "Dumping PostgreSQL db=[$DB_NAME] user=[$DB_USER]"
    pg_dump -U "$DB_USER" -d "$DB_NAME" -F p -f "$TARGET_DIR/$OUT_FILE"
    ;;
  mysql)
    DB_USER="${MYSQL_USER:-root}"
    DB_NAME="${MYSQL_DATABASE}"
    if [ "$DB_USER" = "root" ]; then
      DB_PASSWORD=$(cat "${MYSQL_ROOT_PASSWORD_FILE}")
    else
      DB_PASSWORD=$(cat "${MYSQL_PASSWORD_FILE}")
    fi
    echo "Dumping MySQL db=[$DB_NAME] user=[$DB_USER]"
    mysqldump -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" > "$TARGET_DIR/$OUT_FILE"
    ;;
  *)
    echo "ERROR: unsupported DB_TYPE [$DB_TYPE] (expected postgresql or mysql)"
    exit 1
    ;;
esac

if [ $? -ne 0 ]; then
  echo "DB dump failed"
  exit 1
fi

gzip "$TARGET_DIR/$OUT_FILE"
if [ $? -ne 0 ]; then
  echo "gzip failed"
  exit 1
fi

ls -lh "$TARGET_DIR/$OUT_FILE.gz"
echo "Dump complete: $TARGET_DIR/$OUT_FILE.gz"
