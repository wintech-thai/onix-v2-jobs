  #!/bin/bash

  kubectl create job --from=cronjob/simple-email-send simple-email-send-211321-nw6bq -n onix-v2-development --dry-run=client -o yaml |   kubectl patch --local -p '{"spec":{"template":{"spec":{"containers":[{"name":"cron-script","env":[
    {"name":"JOB_ID","value":"4f56f892-ae3f-4338-8881-9b1be813862a"},
    {"name":"EMAIL_OTP_ADDRESS", "value":"pjame.fb@gmail.com"},
    {"name":"TEMPLATE_TYPE", "value":"org-registration-otp"},
    {"name":"OTP", "value":"696509"}
  ]}]}}}}' --type=strategic -f - -o yaml |   kubectl apply -f -
