#!/bin/bash

CRONJOB_NAME="scan-item-generator"

TIMESTAMP=$(date +%H%M%S)
JOB_NAME="scan-item-generator-${TIMESTAMP}"

NAMESPACE="onix-v2-development"

# สร้าง Job จาก CronJob เดิม
# แก้ไขชื่อ container "my-container" ให้ถูกต้อง
kubectl create job --from=cronjob/${CRONJOB_NAME} ${JOB_NAME} -n ${NAMESPACE} --dry-run=client -o yaml | \
kubectl patch --local -p '{"spec":{"template":{"spec":{"containers":[{"name":"cron-script","env":[{"name":"SCAN_ITEM_COUNT","value":"10000"}]}]}}}}' --type=strategic -f - -o yaml | \
kubectl apply -f -
