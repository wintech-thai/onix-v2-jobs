#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'csv'
require 'erb'
require './utils'
require "google/cloud/storage"

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

$stdout.sync = true

templateType = fallback(ENV['TEMPLATE_TYPE'], 'customer-registration-otp')
serial = ENV['SERIAL']
pin = ENV['PIN']
otp = ENV['OTP']
mailGunApiKey = ENV['MAILGUN_API_KEY']

jobId = fallback(ENV['JOB_ID'], '')
emailNotiAddress = fallback(ENV['EMAIL_NOTI_ADDRESS'], 'support@please-scan.com')
emailOtpAddress = fallback(ENV['EMAIL_OTP_ADDRESS'], 'error@please-scan.com')

puts("INFO : ### Start sending OTP [#{otp}] to [#{emailOtpAddress}], TEMPLATE_TYPE=[#{templateType}]")
puts("INFO : ### JOB_ID=[#{jobId}]")

pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]
conn = connect_db(pgHost, pgDb, ENV["PG_USER"], ENV["PG_PASSWORD"])
if (conn.nil?)
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end
puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")

update_job_status(conn, jobId, 'Running') unless jobId == ""

if (templateType == 'customer-registration-otp')
  subject = "Your product registration OTP [#{otp}]"
  emailText = <<~TEXT
Hello,
Your One-Time Password (OTP) is: #{otp}

Please enter this code within 15 minutes to verify your identity.
If you did not request this code, you can safely ignore this email.

Below are the information of your serial & PIN.
Serial : #{serial}
Pin : #{pin}

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'customer-registration-welcome')
  subject = "Congratulation!!! - Your product registration is complete."
  emailText = <<~TEXT
Hello,
Congratulations!!! Your product has been successfully registered.

Below are the information of your serial & PIN.
Serial : #{serial}
Pin : #{pin}

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'customer-email-verification')
  subject = "Verify your email address to complete your registration."
  emailText = <<~TEXT
Hi #{ENV['ENTITY_NAME']},

Thank you for registering with Please Scan (on behalf of #{ENV['USER_ORG_ID']})

To verify your email address and complete your registration, please click the link below:

#{ENV['REGISTRATION_URL']}

If you didn't request this verification, you can safely ignore this email.

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'org-registration-otp')
  subject = "Your organization registration OTP [#{otp}]"
  emailText = <<~TEXT
Hello,
Your organization registration One-Time Password (OTP) is: #{otp}

Please enter this code within 15 minutes to verify your identity.
If you did not request this code, you can safely ignore this email.

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'org-registration-welcome')
  subject = "Congratulation!!! - Your organization [#{ENV['USER_ORG_ID']}] is ready to use"
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

🎉 Congratulations! Your organization [#{ENV['USER_ORG_ID']}] has been successfully created and is now ready to use.

If you need any help, feel free to check our Help Center or contact our support team.
Welcome aboard, and we're excited to see what you'll build with us!

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'user-password-change')
  subject = "Your password for user [#{ENV['ORG_USER_NAMME']}] has been updated."
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

We wanted to let you know that the password for your account (#{ENV['ORG_USER_NAMME']}) was successfully changed.

If you made this change, no further action is required.

If you did not change your password, please reset your password immediately
or contact our support team.

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'user-forgot-password')
  subject = "Your password reset link for user [#{ENV['USER_NAME']}]"
  emailText = <<~TEXT
Hi #{ENV['USER_NAME']},

We received a request to reset your password for your [#{ENV['USER_NAME']}] account.
If you made this request, please click the link below to reset your password:

#{ENV['RESET_PASSWORD_URL']}

If you did not change your password, please reset your password immediately
or contact our support team.

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'admin-invitation')
  subject = "Your're invited to join the admins team."
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

You've been invited by [#{ENV['INVITED_BY']}] to join [#{ENV['USER_ORG_ID']}] the admins team of Please Scan product. To get started, simply click the link below (expire within 12 hours):
#{ENV['REGISTRATION_URL']}


Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'admin-invitation-welcome')
  subject = "Congratulation!!!, your're successfully joined admins team."
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

Welcome aboard! 🎉

We're thrilled to have you as part of the admins team of Please Scan.
Your account has been successfully linked, and you can now start exploring and collaborating with your team.

If you have any questions or need help getting started, feel free to reach out to us anytime.

Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'user-invitation-to-org')
  subject = "Your're invited to join organization [#{ENV['USER_ORG_ID']}]."
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

You've been invited by [#{ENV['INVITED_BY']}] to join [#{ENV['USER_ORG_ID']}] organization of Please Scan product. To get started, simply click the link below (expire within 24 hours):
#{ENV['REGISTRATION_URL']}


Best regards,  
The Please Scan Team
TEXT

elsif (templateType == 'user-invitation-to-org-welcome')
  subject = "Congratulation!!!, your're successfully joined organization [#{ENV['USER_ORG_ID']}]."
  emailText = <<~TEXT
Hi #{ENV['ORG_USER_NAMME']},

Welcome aboard! 🎉

We're thrilled to have you as part of the [#{ENV['USER_ORG_ID']}] organization in Please Scan.
Your account has been successfully linked, and you can now start exploring and collaborating with your team.

If you have any questions or need help getting started, feel free to reach out to us anytime.

Best regards,  
The Please Scan Team
TEXT

else
  subject = "Unidentified email template type [#{templateType}]"
  emailText = subject
end

### Start email ####
emailObj = {
  'from' => 'no-reply@please-scan.com',
  'to' => emailOtpAddress,
  'bcc' => emailNotiAddress,
  'subject' => subject,
  'text' => emailText,
}

send_email(emailObj, mailGunApiKey, nil)

message = "Done sending email to [#{emailOtpAddress}]" 
update_job_done(conn, jobId, 1, 0, message) unless jobId == ""

puts("INFO : ### #{message}")
