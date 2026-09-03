#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'net/http'
require 'json'
require 'securerandom'

require './utils'

if File.exist?('env.rb')
  require './env'
end

def insert_audit_notice(conn, orgId, rowId, trackModel, message)
  return unless conn && rowId && !rowId.to_s.strip.empty?
  conn.exec_params(
    "INSERT INTO \"AuditNotices\" (notice_id, org_id, track_model, row_id, message, created_date) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)",
    [SecureRandom.uuid, orgId, trackModel, rowId, message]
  )
  update_notice_count(conn, rowId)
rescue => e
  puts "WARN : insert_audit_notice failed: #{e.message}"
end

# บันทึก AuditNotice ให้ทั้ง PaymentRequest และ PaymentTransaction (ถ้ามี pmtId มาด้วย)
# เพราะ job Payment.Success / PaymentOut.Success ส่งมาทั้งสอง ID ในขณะที่ job ประเภท Rejected จะมีแต่ pmrId
def insert_audit_notices(conn, orgId, pmrId, pmtId, message)
  insert_audit_notice(conn, orgId, pmrId, 'PaymentRequest', message)
  insert_audit_notice(conn, orgId, pmtId, 'PaymentTransaction', message)
end

def update_notice_count(conn, row_id)
  return unless conn && row_id && !row_id.to_s.strip.empty?
  res = conn.exec_params(
    'SELECT COUNT(*) AS cnt FROM "AuditNotices" WHERE row_id = $1', [row_id]
  )
  cnt = res.first['cnt'].to_i
  begin
    conn.exec_params(
      'UPDATE "PaymentRequests" SET notice_count = $1 WHERE request_id::text = $2',
      [cnt, row_id]
    )
  rescue => e
    puts "WARN : update_notice_count (PaymentRequests) failed: #{e.message}"
  end
  begin
    conn.exec_params(
      'UPDATE "PaymentTransactions" SET notice_count = $1 WHERE transaction_id::text = $2',
      [cnt, row_id]
    )
  rescue => e
    puts "WARN : update_notice_count (PaymentTransactions) failed: #{e.message}"
  end
rescue => e
  puts "WARN : update_notice_count failed: #{e.message}"
end

def get_webhook_config(conn, merchantId, eventName)
  sql = "SELECT * FROM \"WebhookConfigs\" WHERE (merchant_id = $1) AND (event_name = $2)"
  res = conn.exec_params(sql, [merchantId, eventName])
  return nil if res.ntuples <= 0
  res.first
end

def call_webhook(webhookConfig, data, lines, jobId)
  begin
    endpoint_url = webhookConfig['endpoint_url']
    http_method = (webhookConfig['http_method'] || 'POST').upcase

    timeout_sec = webhookConfig['timeout_sec'].to_i
    timeout_sec = 5 if timeout_sec <= 0 || timeout_sec > 5

    headers = {}
    if webhookConfig['headers_definition'] && !webhookConfig['headers_definition'].empty?
      headers = JSON.parse(webhookConfig['headers_definition'])
    end

    uri = URI.parse(endpoint_url)

    unless ['http', 'https'].include?(uri.scheme)
      str = "INFO : [#{jobId}] : Webhook failed: unsupported URL scheme '#{uri.scheme}' in endpoint URL '#{endpoint_url}'"
      lines << str
      puts(str)
      return
    end

    str = "INFO : [#{jobId}] : Calling webhook #{http_method} #{endpoint_url}"
    lines << str
    puts(str)

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https')
    http.open_timeout = timeout_sec
    http.read_timeout = timeout_sec

    request =
      case http_method
      when 'GET'    then Net::HTTP::Get.new(uri)
      when 'POST'   then Net::HTTP::Post.new(uri)
      when 'PUT'    then Net::HTTP::Put.new(uri)
      when 'PATCH'  then Net::HTTP::Patch.new(uri)
      when 'DELETE' then Net::HTTP::Delete.new(uri)
      else
        str = "INFO : [#{jobId}] : Webhook failed: unsupported HTTP method '#{http_method}'"
        lines << str
        puts(str)
        return
      end

    headers.each { |key, value| request[key] = value.to_s }
    request['Content-Type'] ||= 'application/json'
    request.body = data.to_json unless http_method == 'GET'

    response = http.request(request)
    body_preview = (response.body || '')[0, 200]

    str = "INFO : [#{jobId}] : Webhook response: status=#{response.code} body='#{body_preview}'"
    lines << str
    puts(str)

    response
  rescue JSON::ParserError => ex
    str = "INFO : [#{jobId}] : Webhook failed: invalid headers_definition (#{ex.message})"
    lines << str; puts(str); nil
  rescue Net::OpenTimeout, Net::ReadTimeout
    str = "INFO : [#{jobId}] : Webhook failed: timeout after #{timeout_sec}s"
    lines << str; puts(str); nil
  rescue StandardError => ex
    str = "INFO : [#{jobId}] : Webhook failed: #{ex.class} #{ex.message}"
    lines << str; puts(str); nil
  end
end

def process_payment_success_job(stream, data, conn, eventName = 'Payment.Success')
  lines  = []
  jobId  = data['Id']
  eventType = data['Type']
  params = data['Parameters']
  hash   = params.map { |p| [p['Name'], p['Value']] }.to_h
  merchantId   = hash['MERCHANT_ID']
  merchantCode = hash['MERCHANT_CODE']
  orgId        = hash['ORG_ID'] || merchantId
  pmrId        = hash['PMR_ID']
  pmtId        = hash['PMT_ID']

  str = "INFO : [#{jobId}] : Processing job from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str); lines << str

  update_job_status(conn, jobId, 'Submitted')
  update_job_status(conn, jobId, 'Running')

  whc = get_webhook_config(conn, merchantId, eventType)
  if whc.nil?
    str = "ERROR : [#{jobId}] : No webhook configuration found for merchant [#{merchantId}] [#{merchantCode}]"
    puts(str); lines << str
    insert_audit_notices(conn, orgId, pmrId, pmtId, "Webhook config not found for event [#{eventType}] merchant [#{merchantCode}]")
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    return
  end

  unless whc['is_active']
    str = "ERROR : [#{jobId}] : Webhook is not active for merchant [#{merchantId}] [#{merchantCode}]"
    puts(str); lines << str
    insert_audit_notices(conn, orgId, pmrId, pmtId, "Webhook is not active for event [#{eventType}] merchant [#{merchantCode}]")
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    return
  end

  webhookUrl = whc['endpoint_url']
  str = "INFO : [#{jobId}] : Calling webhook [#{webhookUrl}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str); lines << str

  response = call_webhook(whc, data, lines, jobId)

  if response.nil?
    insert_audit_notices(conn, orgId, pmrId, pmtId, "Webhook call failed (timeout or error) for event [#{eventType}] url [#{webhookUrl}]")
  elsif response.code.to_i < 200 || response.code.to_i >= 300
    body_preview = (response.body || '')[0, 200]
    insert_audit_notices(conn, orgId, pmrId, pmtId, "Webhook invalid response HTTP #{response.code} for event [#{eventType}] url [#{webhookUrl}] body=[#{body_preview}]")
  else
    # 2xx — validate response body must be JSON with status == "ok" or "success"
    begin
      body = JSON.parse(response.body || '{}')
      status_val = body['status'].to_s.downcase
      unless ['ok', 'success'].include?(status_val)
        insert_audit_notices(conn, orgId, pmrId, pmtId,
          "Webhook response status not ok (got '#{body['status']}') for event [#{eventType}] url [#{webhookUrl}]")
      end
    rescue JSON::ParserError
      insert_audit_notices(conn, orgId, pmrId, pmtId,
        "Webhook response is not valid JSON for event [#{eventType}] url [#{webhookUrl}]")
    end
  end

  str = "INFO : [#{jobId}] : Done processing job from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str); lines << str

  update_job_done(conn, jobId, 1, 0, lines.join("\n"))
end

# PayIn.Requested เอาไว้อัพเดต IoC table ด้วย PayerName ที่ดึงมาจาก pay-in request (ทั้ง P2P และ non-P2P)
# ไม่ได้ยิง webhook เหมือน job ประเภทอื่น ๆ เลยแยก process function ต่างหาก
def process_payin_requested_job(stream, data, conn)
  lines = []
  jobId = data['Id']
  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  orgId = hash['ORG_ID']
  pmrId = hash['PMR_ID']
  payerName = hash['PAYER_NAME']

  str = "INFO : [#{jobId}] : Processing PayIn.Requested job for PMR [#{pmrId}] org [#{orgId}]"
  puts(str); lines << str

  update_job_status(conn, jobId, 'Submitted')
  update_job_status(conn, jobId, 'Running')

  if payerName.nil? || payerName.strip.empty?
    str = "INFO : [#{jobId}] : PayerName is blank, skip IoC upsert"
    puts(str); lines << str
    update_job_done(conn, jobId, 1, 0, lines.join("\n"))
    return
  end

  payerName = payerName.strip

  # IoC เป็น feature ระดับ admin (จัดการผ่าน AdminIocController ซึ่ง hardcode org_id="global" เสมอ
  # ไม่ผูกกับ orgId ของ merchant แต่ละราย) ต้อง upsert ด้วย org_id="global" ให้ตรงกัน ไม่งั้นจะไม่มีวัน match
  # กับที่ policy check (PaymentRequestService.cs) หรือหน้า admin IoC list มองเห็น
  iocOrgId = 'global'

  begin
    existing = conn.exec_params(
      'SELECT oic_id, seen_count FROM "Iocs" WHERE org_id = $1 AND ioc_type = $2 AND ioc_value = $3',
      [iocOrgId, 'PayerName', payerName]
    )

    if existing.ntuples > 0
      row = existing.first
      newSeenCount = row['seen_count'].to_i + 1
      conn.exec_params(
        'UPDATE "Iocs" SET seen_count = $1, last_seen_date = CURRENT_TIMESTAMP WHERE oic_id = $2',
        [newSeenCount, row['oic_id']]
      )
      str = "INFO : [#{jobId}] : Updated existing IoC [#{row['oic_id']}] for PayerName [#{payerName}], SeenCount=[#{newSeenCount}]"
      puts(str); lines << str
    else
      iocId = SecureRandom.uuid
      conn.exec_params(
        "INSERT INTO \"Iocs\" (oic_id, org_id, ioc_type, ioc_value, status, reputation, risk_score, confidence_score, seen_count, created_date, first_seen_date, last_seen_date)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        [iocId, iocOrgId, 'PayerName', payerName, 'Active', 'Unknown', 0, 0, 1]
      )
      str = "INFO : [#{jobId}] : Inserted new IoC [#{iocId}] for PayerName [#{payerName}]"
      puts(str); lines << str
    end

    str = "INFO : [#{jobId}] : Done processing PayIn.Requested job"
    puts(str); lines << str

    update_job_done(conn, jobId, 1, 0, lines.join("\n"))
  rescue PG::Error => e
    str = "ERROR : [#{jobId}] : IoC upsert failed: #{e.message}"
    puts(str); lines << str
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    raise
  end
end

$stdout.sync = true

environment   = ENV['ENVIRONMENT']
redisHost     = ENV['REDIS_HOST']
redisPort     = ENV['REDIS_PORT']
pgHost        = ENV['PG_HOST']
pgDb          = ENV['PG_DB']

group_name    = 'k8s-job'
consumer_name = 'k8s-job-dispatcher'
streams = [
  "JobSubmitted:#{environment}:Payment.Success",
  "JobSubmitted:#{environment}:PaymentOut.Success",
  "JobSubmitted:#{environment}:PaymentIn.Rejected",
  "JobSubmitted:#{environment}:PaymentOut.Rejected",
  "JobSubmitted:#{environment}:PayIn.Requested",
]

WEBHOOK_JOB_TYPES = %w[Payment.Success PaymentOut.Success PaymentIn.Rejected PaymentOut.Rejected].freeze
KNOWN_JOB_TYPES = (WEBHOOK_JOB_TYPES + %w[PayIn.Requested]).freeze

def dispatch_job(stream, data, conn)
  if data['Type'] == 'PayIn.Requested'
    process_payin_requested_job(stream, data, conn)
  else
    process_payment_success_job(stream, data, conn)
  end
end
MAX_PG_RECONNECT = 3

puts "INFO : ### Start dispatching jobs."
puts "INFO : ### ENVIRONMENT=[#{environment}]"
puts "INFO : ### REDIS_HOST=[#{redisHost}]"
puts "INFO : ### REDIS_PORT=[#{redisPort}]"

redis = Redis.new(host: redisHost, port: redisPort, read_timeout: 10, reconnect_attempts: 2)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, '$', mkstream: true)
    puts "INFO : ### Created group [#{group_name}] for stream [#{stream_key}]"
  rescue Redis::CommandError => e
    puts "INFO : ### Group already created for stream [#{stream_key}]" if e.message.include?('BUSYGROUP')
  end
end

conn = nil
pg_reconnect_count = 0

loop do
  begin
    if conn.nil?
      puts "INFO : ### Connecting to PostgreSQL [#{pgHost}] [#{pgDb}]"
      conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
      if conn.nil?
        pg_reconnect_count += 1
        if pg_reconnect_count >= MAX_PG_RECONNECT
          puts "ERROR : ### Failed to reconnect #{MAX_PG_RECONNECT} times — exiting"
          exit 1
        end
        puts "ERROR : ### Unable to connect (attempt #{pg_reconnect_count}/#{MAX_PG_RECONNECT}) — retrying in 10s"
        sleep 10
        next
      end
      puts "INFO : ### Connected to PostgreSQL"
      pg_reconnect_count = 0

      # Drain PEL on reconnect
      ids     = Array.new(streams.size, '0')
      pending = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
      if pending
        pending.each do |stream, messages|
          messages.each do |id, fields|
            begin
              data = JSON.parse(fields['message']) rescue nil
              if data && KNOWN_JOB_TYPES.include?(data['Type'])
                dispatch_job(stream, data, conn)
              end
            rescue => e
              puts "WARN : PEL drain error [#{id}]: #{e.message}"
            ensure
              redis.xack(stream, group_name, id) rescue nil
            end
          end
        end
      end
    end

    File.write('/tmp/dispatcher-heartbeat', Time.now.to_i.to_s)

    entries = redis.xreadgroup(
      group_name,
      consumer_name,
      streams,
      Array.new(streams.size, '>'),
      count: 10,
      block: 5000
    )

    next unless entries

    pg_failed = false
    entries.each do |stream, messages|
      break if pg_failed
      messages.each do |id, fields|
        break if pg_failed
        begin
          data = JSON.parse(fields['message']) rescue nil
          if data && KNOWN_JOB_TYPES.include?(data['Type'])
            dispatch_job(stream, data, conn)
          end
          redis.xack(stream, group_name, id) rescue nil
        rescue PG::Error => e
          puts "ERROR : ### PG connection error [#{id}]: #{e.message} — leaving in PEL for retry"
          begin; conn&.close; rescue; end
          conn = nil
          pg_failed = true
        rescue => e
          puts "ERROR : ### process error [#{id}]: #{e.message}"
          redis.xack(stream, group_name, id) rescue nil
        end
      end
    end

  rescue PG::Error => e
    puts "ERROR : ### PostgreSQL error: #{e.message} — reconnecting in 5s"
    begin; conn&.close; rescue; end
    conn = nil
    sleep 5

  rescue Redis::BaseError => e
    puts "ERROR : ### Redis error: #{e.message} — retrying in 5s"
    sleep 5

  rescue => e
    puts "ERROR : ### Unexpected error: #{e.message} — retrying in 5s"
    sleep 5
  end
end
