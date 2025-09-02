#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'csv'
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

serialStart = fallback(ENV['SERIAL_NUMBER_START'], '1').to_i
serialPrefix = fallback(ENV['SERIAL_NUMBER_PREFIX'], random_char())
serialDigit = fallback(ENV['SERIAL_NUMBER_DIGIT'], '7').to_i

puts("INFO : ### Start generating scan-items.")

puts("INFO : ### SCAN_ITEM_COUNT=[#{totalItem}]")
puts("INFO : ### SCAN_ITEM_URL=[#{scanItemUrl}]")
puts("INFO : ### SCAN_ITEM_BUCKET=[#{scanItemBucket}]")
puts("INFO : ### SCAN_ITEM_ORG=[#{scanItemOrg}]")
puts("INFO : ### SCAN_ITEM_GROUP=[#{itemGroup}]")

puts("INFO : ### SERIAL_NUMBER_START=[#{serialStart}]")
puts("INFO : ### SERIAL_NUMBER_PREFIX=[#{serialPrefix}]")
puts("INFO : ### SERIAL_NUMBER_DIGIT=[#{serialDigit}]")

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


line = "SERIAL,PIN,QR (URL)\n"
File.write(filePath, line)

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
  else
    puts("ERROR : ### Unable to insert data --> serial=[#{serial}], pin=[#{pin}], [#{err}]")
  end

  i = i + 1
end

puts("INFO : ### Uploading file [#{filePath}] to [#{gcsPath}]...")

storage = Google::Cloud::Storage.new(project_id: ENV['SCAN_ITEM_GCS_PROJECT'])
bucket = storage.bucket(scanItemBucket)
uploaded = bucket.create_file(filePath, remotePath)

puts("INFO : ### Done uploading file [#{gcsPath}]")
puts("INFO : ### Done generating [#{itemCnt}] items.")
