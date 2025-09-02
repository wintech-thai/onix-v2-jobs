#!/bin/bash

echo "BACKUP_NAME_PREFIX = [${BACKUP_NAME_PREFIX}]"
echo "EXTENSION = [${EXTENSION}]"
echo "EXCLUDE_FLAG = [${EXCLUDE_FLAG}]"
echo "GCS_FOLDER = [${GCS_FOLDER}]"

NAME_PREFIX=${BACKUP_NAME_PREFIX}
EXT=${EXTENSION}

FOLDER=${GCS_FOLDER}
if [ -z "${FOLDER}" ]; then
    FOLDER="db-backup-v2"
fi

#TARGET_NS=ads-prod
#TARGET_POD=postgresql-ads-prod

DST_DIR=/tmp
TARGET_DIR=/tmp
TS=$(date +%Y%m%d_%H%M%S)
DMP_FILE=${NAME_PREFIX}-${EXT}-backup-${TS}.sql
DMP_FILE_GZ=${DMP_FILE}.gz
BUCKET_NAME=onix-v2-backup
SCRIPT_FILE=pg-dump-bitnami.bash

gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}

echo "Copying [${SCRIPT_FILE}] into pod=[${TARGET_POD}], namespace=[${TARGET_NS}]"
kubectl cp ${SCRIPT_FILE} -n ${TARGET_NS} ${TARGET_POD}:/${TARGET_DIR}/
if [ $? -ne 0 ]; then
    exit 1
fi

echo "Running [${SCRIPT_FILE}] in pod=[${TARGET_POD}], namespace=[${TARGET_NS}]"
kubectl exec -i -n ${TARGET_NS} ${TARGET_POD} -- bash ${TARGET_DIR}/${SCRIPT_FILE} "${PG_USER}" "${DMP_FILE}" "${TARGET_DIR}" "${EXCLUDE_FLAG}"
if [ $? -ne 0 ]; then
    exit 1
fi

echo "Copying [${DMP_FILE_GZ}] in pod=[${TARGET_POD}], namespace=[${TARGET_NS}] to [${DST_DIR}]"
kubectl cp -n ${TARGET_NS} ${TARGET_POD}:/${TARGET_DIR}/${DMP_FILE_GZ} ${DST_DIR}/${DMP_FILE_GZ}
if [ $? -ne 0 ]; then
    exit 1
fi

ls -al ${TARGET_DIR}/${DMP_FILE_GZ}

EXPORTED_FILE=${DST_DIR}/${DMP_FILE_GZ}
GCS_PATH_DB=gs://${BUCKET_NAME}/${FOLDER}/${DMP_FILE_GZ}

gsutil cp ${EXPORTED_FILE} ${GCS_PATH_DB}
if [ $? -ne 0 ]; then
    exit 1
fi
