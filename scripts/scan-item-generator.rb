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

def insert_data(conn, item)
  seqNo = item[:seqNo]
  serial = item[:serial]
  pin = item[:pin]
  url = item[:url]
  runId = item[:runId]
  uploadPath = item[:uploadPath]
  scanItemOrg = item[:orgId]
  itemGroup = item[:itemGroup]

  begin
    conn.transaction do |con|
        con.exec "INSERT INTO \"ScanItems\" 
        (
            scan_item_id,
            org_id,
            serial,
            pin,
            registered_flag,
            sequence_no,
            url,
            run_id,
            item_group,
            uploaded_path,
            created_date
        )
        VALUES
        (
            gen_random_uuid(),
            '#{escape_char(scanItemOrg)}',
            '#{escape_char(serial)}',
            '#{escape_char(pin)}',
            'FALSE',
            '#{escape_char(seqNo)}',
            '#{escape_char(url)}',
            '#{escape_char(runId)}',
            '#{escape_char(itemGroup)}',
            '#{escape_char(uploadPath)}',
            CURRENT_TIMESTAMP
        )
        "
    end
  rescue PG::Error => e
    return false, e.message
  end

  return true, ""
end

totalItem = ENV['SCAN_ITEM_COUNT']
scanItemUrl = ENV['SCAN_ITEM_URL']
scanItemBucket = ENV['SCAN_ITEM_BUCKET']
scanItemOrg = ENV['SCAN_ITEM_ORG'] 
tempDir = ENV['TEMP_DIR'] 
itemGroup = ENV['SCAN_ITEM_GROUP']
mailGunApiKey = ENV['MAILGUN_API_KEY']

serialStart = fallback(ENV['SERIAL_NUMBER_START'], '1').to_i
serialPrefix = fallback(ENV['SERIAL_NUMBER_PREFIX'], "#{random_char()}#{random_char()}")
serialDigit = fallback(ENV['SERIAL_NUMBER_DIGIT'], '6').to_i
jobId = fallback(ENV['JOB_ID'], '')
emailNotiAddress = fallback(ENV['EMAIL_NOTI_ADDRESS'], 'support@please-scan.com')

puts("INFO : ### Start generating scan-items.")

puts("INFO : ### SCAN_ITEM_COUNT=[#{totalItem}]")
puts("INFO : ### SCAN_ITEM_URL=[#{scanItemUrl}]")
puts("INFO : ### SCAN_ITEM_BUCKET=[#{scanItemBucket}]")
puts("INFO : ### SCAN_ITEM_ORG=[#{scanItemOrg}]")
puts("INFO : ### SCAN_ITEM_GROUP=[#{itemGroup}]")

puts("INFO : ### SERIAL_NUMBER_START=[#{serialStart}]")
puts("INFO : ### SERIAL_NUMBER_PREFIX=[#{serialPrefix}]")
puts("INFO : ### SERIAL_NUMBER_DIGIT=[#{serialDigit}]")
puts("INFO : ### JOB_ID=[#{jobId}]")

runDate = Time.now.strftime("%Y%m%d")
runDateTime = Time.now.strftime("%Y%m%d%H%M")

dummyText = random_string(3)
fileName = "#{runDateTime}-#{dummyText}.csv"
filePath = "#{tempDir}/#{fileName}"
remotePath = "#{scanItemOrg}/#{itemGroup}/#{runDate}/#{fileName}"
gcsPath = "gs://#{scanItemBucket}/#{remotePath}"

puts("INFO : ### Saving file to [#{filePath}]")

itemCnt = totalItem.to_i
i = 1

pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]
conn = connect_db(pgHost, pgDb, ENV["PG_USER"], ENV["PG_PASSWORD"])
if (conn.nil?)
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end
puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")

update_job_status(conn, jobId, 'Running') unless jobId == ""

line = "SERIAL,PIN,QR (URL)\n"
File.write(filePath, line)

successCnt = 0
failedCnt = 0

while i <= itemCnt do
  serial = generate_serial(prefix: serialPrefix, digits: serialDigit, start: serialStart + (i-1))
  pin = random_string()

  url = scanItemUrl.sub('{VAR_ORG}', scanItemOrg)
  url = url.sub('{VAR_SERIAL}', serial)
  url = url.sub('{VAR_PIN}', pin)

  puts("INFO : ### [#{dummyText}] [#{i}/#{itemCnt}] Generated serial=[#{serial}], pin=[#{pin}], url=[#{url}]")

  item = { 
    seqNo: i,
    serial: serial, 
    pin: pin,
    url: url,
    runId: dummyText,
    uploadPath: gcsPath,
    orgId: scanItemOrg,
    itemGroup: itemGroup
  }

  isSuccess, err = insert_data(conn, item)
  if (isSuccess)
    line = "#{serial},#{pin},#{url}\n"
    File.write(filePath, line, mode: "a")

    successCnt = successCnt + 1
  else
    puts("ERROR : ### Unable to insert data --> serial=[#{serial}], pin=[#{pin}], [#{err}]")
    failedCnt = failedCnt + 1
  end

  i = i + 1
end

puts("INFO : ### Uploading file [#{filePath}] to [#{gcsPath}]...")

storage = Google::Cloud::Storage.new(project_id: ENV['SCAN_ITEM_GCS_PROJECT'])
bucket = storage.bucket(scanItemBucket)
uploaded = bucket.create_file(filePath, remotePath)
preSignedUrl = uploaded.signed_url(method: "GET", expires: 3600) # 1 hr expire

jobStatus = 'Succeed'
if (failedCnt.to_i > 0)
  jobStatus = 'Failed'
end

update_job_status(conn, jobId, jobStatus) unless jobId == ""

message = "Done generating [#{itemCnt}], succeed=[#{successCnt}], failed=[#{failedCnt}]" 
update_job_done(conn, jobId, successCnt, failedCnt, message) unless jobId == ""

### Start email ####
class Report
    def initialize(statusArr)
      @statusItems = statusArr
    end

    # Support templating of member data.
    def get_binding
      binding
    end
end

linkUrl = "<a href='#{preSignedUrl}'>#{gcsPath}</a>"
operationStatus = [
  { 'name' => 'Job ID', 'description' => "#{jobId}" },
  { 'name' => 'File (click to download)', 'description' => "#{linkUrl}" },
  { 'name' => 'Total', 'description' => "#{itemCnt}" },
  { 'name' => 'Job Status', 'description' => "#{jobStatus}" },
  { 'name' => 'Succeed', 'description' => "#{successCnt}" },
  { 'name' => 'Failed', 'description' => "#{failedCnt}" },
]

emailObj = {
  'from' => 'no-reply@please-scan.com',
  'to' => emailNotiAddress,
  'subject' => "[#{jobStatus}] - Scan items job [#{jobId}] for [#{scanItemOrg}] is done.",
  #'text' => 'Please see detail below.',
}

rpt = Report.new(operationStatus)
reportFile = "#{tempDir}/report.html"
render_report_file(rpt, "templates/scan-item-notify.erb", reportFile)

content = File.read(reportFile)
send_email(emailObj, mailGunApiKey, content)
### End email ####

puts("INFO : ### Done uploading file [#{gcsPath}]")
puts("INFO : ### Done generating [#{itemCnt}] items.")
