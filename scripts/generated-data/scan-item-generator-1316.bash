  #!/bin/bash

  kubectl create job --from=cronjob/scan-item-generator scan-item-generator-1316 -n onix-v2-development --dry-run=client -o yaml |   kubectl patch --local -p '{"spec":{"template":{"spec":{"containers":[{"name":"cron-script","env":[
    {"name":"JOB_ID","value":"2304b55b-73d6-4951-9d30-3ef003ed6db4"},
    {"name":"SCAN_ITEM_ORG", "value":"default"},
    {"name":"SCAN_ITEM_COUNT", "value":"50"},
    {"name":"SCAN_ITEM_URL", "value":"https://scan-dev.please-scan.com/org/{VAR_ORG}/Verify/{VAR_SERIAL}/{VAR_PIN}"},
    {"name":"EMAIL_NOTI_ADDRESS", "value":"support@please-scan.com"},
    {"name":"SERIAL_NUMBER_DIGIT", "value":"7"}
  ]}]}}}}' --type=strategic -f - -o yaml |   kubectl apply -f -
