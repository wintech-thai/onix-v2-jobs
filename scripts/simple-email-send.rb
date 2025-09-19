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

texplateType = fallback(ENV['TEMPLATE_TYPE'], 'customer-registration-otp')
serial = ENV['SERIAL']
pin = ENV['PIN']
otp = ENV['OTP']
mailGunApiKey = ENV['MAILGUN_API_KEY']

jobId = fallback(ENV['JOB_ID'], '')
emailNotiAddress = fallback(ENV['EMAIL_NOTI_ADDRESS'], 'support@please-scan.com')
emailOtpAddress = fallback(ENV['EMAIL_OTP_ADDRESS'], 'error@please-scan.com')

puts("INFO : ### Start sending OTP [#{otp}] to [#{emailOtpAddress}], SERIAL=[#{serial}], PIN=[#{pin}]")
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

if (texplateType == 'customer-registration-otp')
  subject = "Your product registration OTP [#{otp}]"
  emailText = <<~TEXT
Hello,
Your One-Time Password (OTP) is: #{otp}

Please enter this code within 15 minutes to verify your identity.
If you did not request this code, you can safely ignore this email.

Below are the information of your serial & PIN.
Serial : #{serial}
Pin : #{pin}

Thank you.
TEXT
end

### Start email ####
emailObj = {
  'from' => 'otp@please-scan.com',
  'to' => emailOtpAddress,
  'bcc' => emailNotiAddress,
  'subject' => subject,
  'text' => emailText,
}

send_email(emailObj, mailGunApiKey, nil)

message = "Done sending OTP [#{otp}] to email [#{emailOtpAddress}]" 
update_job_done(conn, jobId, 1, 0, message) unless jobId == ""

puts("INFO : ### #{message}")
