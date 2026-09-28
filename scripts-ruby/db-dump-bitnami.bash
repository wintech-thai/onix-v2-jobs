#!/bin/bash
# db-dump-bitnami.bash <DB_TYPE> <OUT_FILE> <TARGET_DIR>
#
# Runs inside a Bitnami PostgreSQL/MySQL pod. User/database/password are read
# from the pod's own env vars (set by the Bitnami chart itself) so nothing
# needs to be passed in or hardcoded here — this stays generic across apps.
#
# Bitnami sets the password either as a plain env var (e.g. MYSQL_ROOT_PASSWORD)
# or, when a mounted secret is used, as a "_FILE" pointer (e.g.
# MYSQL_ROOT_PASSWORD_FILE) — this script checks the _FILE variant first and
# falls back to the plain value.

DB_TYPE="$1"
OUT_FILE="$2"
TARGET_DIR="$3"

resolve_secret() {
  # resolve_secret <FILE_VAR_NAME> <PLAIN_VAR_NAME>
  local file_var="$1" plain_var="$2" file_path
  file_path="${!file_var}"
  if [ -n "$file_path" ] && [ -f "$file_path" ]; then
    cat "$file_path"
  else
    echo "${!plain_var}"
  fi
}

cd "$TARGET_DIR" || exit 1
rm -f "$OUT_FILE" "$OUT_FILE.gz"

case "$DB_TYPE" in
  postgresql)
    DB_USER="${POSTGRES_USER}"
    DB_NAME="${POSTGRES_DB:-$POSTGRES_DATABASE}"
    export PGPASSWORD=$(resolve_secret POSTGRES_PASSWORD_FILE POSTGRES_PASSWORD)
    echo "Dumping PostgreSQL db=[$DB_NAME] user=[$DB_USER]"
    pg_dump -U "$DB_USER" -d "$DB_NAME" -F p -f "$TARGET_DIR/$OUT_FILE"
    ;;
  mysql)
    DB_USER="${MYSQL_USER:-root}"
    DB_NAME="${MYSQL_DATABASE}"
    if [ "$DB_USER" = "root" ]; then
      DB_PASSWORD=$(resolve_secret MYSQL_ROOT_PASSWORD_FILE MYSQL_ROOT_PASSWORD)
    else
      DB_PASSWORD=$(resolve_secret MYSQL_PASSWORD_FILE MYSQL_PASSWORD)
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
