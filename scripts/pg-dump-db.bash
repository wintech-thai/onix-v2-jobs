#!/bin/bash
# pg-dump-payment.bash
# Runs inside the postgresql pod.
# Args: $1=PG_USER  $2=DMP_FILE  $3=TARGET_DIR  $4=PGPASSWORD

PG_USER=${1}
DMP_FILE=${2}
TARGET_DIR=${3}
export PGPASSWORD=${4}

echo "Starting pg_dump: user=[${PG_USER}] file=[${DMP_FILE}] dir=[${TARGET_DIR}]"

pg_dump -U "${PG_USER}" -d onix -F p -f "${TARGET_DIR}/${DMP_FILE}"
if [ $? -ne 0 ]; then
    echo "pg_dump failed"
    exit 1
fi

echo "Compressing dump file..."
gzip "${TARGET_DIR}/${DMP_FILE}"
if [ $? -ne 0 ]; then
    echo "gzip failed"
    exit 1
fi

ls -lh "${TARGET_DIR}/${DMP_FILE}.gz"
echo "Dump complete: ${TARGET_DIR}/${DMP_FILE}.gz"
