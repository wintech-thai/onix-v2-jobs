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

def get_noti_channels(conn, orgId)
  sql = "SELECT * FROM \"NotiChannels\" WHERE (org_id = $1) AND (status = $2)"
  res = conn.exec_params(sql, [orgId, 'Enabled'])
  res.to_a
end

def channel_matches_event?(channel, eventType)
  matched = channel['events_matched']
  return false if matched.nil? || matched.empty?
  matched.split('|').include?(eventType)
end

def build_discord_embed(eventType, hash)
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')
  filename   = hash['FILENAME']   || '-'
  bucket     = hash['BUCKET']     || '-'
  path       = hash['PATH']       || '-'
  schedule   = hash['SCHEDULE']   || '-'
  error      = hash['ERROR']      || '-'
  file_size  = hash['FILE_SIZE']  || nil
  duration   = hash['DURATION']   || nil
  start_time = hash['START_TIME'] || nil
  end_time   = hash['END_TIME']   || nil

  case eventType
  when 'Backup.Done'
    success_flag = hash['SUCCESS']
    is_success = success_flag.nil? || success_flag == 'true'
    if is_success
      lines = [
        "**สถานะ**: ✅ Success",
        "**ไฟล์**: #{filename}",
        "**Bucket**: #{bucket}",
        "**Path**: #{path}",
        "**Schedule**: #{schedule}",
      ]
      lines << "**File Size**: #{file_size}" if file_size
      lines << "**Duration**: #{duration}"   if duration
      lines << "**Start**: #{start_time}"    if start_time
      lines << "**End**: #{end_time}"        if end_time
      lines << "**เวลา**: #{now}"
      { title: 'Backup Done', color: 0x57F287, description: lines.join("\n") }
    else
      lines = [
        "**สถานะ**: ❌ Failed",
        "**Error**: #{error}",
        "**Schedule**: #{schedule}",
      ]
      lines << "**Duration**: #{duration}" if duration
      lines << "**เวลา**: #{now}"
      { title: 'Backup Done', color: 0xED4245, description: lines.join("\n") }
    end
  when 'Restore.Success'
    lines = [
      "**สถานะ**: ✅ Success",
      "**ไฟล์**: #{filename}",
      "**เวลา**: #{now}",
    ]
    { title: 'Restore Done', color: 0x57F287, description: lines.join("\n") }
  when 'Restore.Failed'
    lines = [
      "**สถานะ**: ❌ Failed",
      "**ไฟล์**: #{filename}",
      "**Error**: #{error}",
      "**เวลา**: #{now}",
    ]
    { title: 'Restore Done', color: 0xED4245, description: lines.join("\n") }
  else
    { title: eventType.to_s, color: 0x99AAB5, description: '' }
  end
end

def build_message(eventType, hash, bold)
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')
  filename   = hash['FILENAME']   || '-'
  bucket     = hash['BUCKET']     || '-'
  path       = hash['PATH']       || '-'
  schedule   = hash['SCHEDULE']   || '-'
  error      = hash['ERROR']      || '-'
  file_size  = hash['FILE_SIZE']  || nil
  duration   = hash['DURATION']   || nil
  start_time = hash['START_TIME'] || nil
  end_time   = hash['END_TIME']   || nil

  case eventType
  when 'Backup.Done'
    success_flag = hash['SUCCESS']
    is_success = success_flag.nil? || success_flag == 'true'
    if is_success
      lines = [
        bold.call('Backup Done'),
        "#{bold.call('สถานะ')}: ✅ Success",
        "#{bold.call('ไฟล์')}: #{filename}",
        "#{bold.call('Bucket')}: #{bucket}",
        "#{bold.call('Path')}: #{path}",
        "#{bold.call('Schedule')}: #{schedule}",
      ]
      lines << "#{bold.call('File Size')}: #{file_size}" if file_size
      lines << "#{bold.call('Duration')}: #{duration}"   if duration
      lines << "#{bold.call('Start')}: #{start_time}"    if start_time
      lines << "#{bold.call('End')}: #{end_time}"        if end_time
      lines << "#{bold.call('เวลา')}: #{now}"
      lines.join("\n")
    else
      lines = [
        bold.call('Backup Done'),
        "#{bold.call('สถานะ')}: ❌ Failed",
        "#{bold.call('Error')}: #{error}",
        "#{bold.call('Schedule')}: #{schedule}",
      ]
      lines << "#{bold.call('Duration')}: #{duration}" if duration
      lines << "#{bold.call('เวลา')}: #{now}"
      lines.join("\n")
    end
  when 'Restore.Success'
    [
      bold.call('Restore Done'),
      "#{bold.call('สถานะ')}: ✅ Success",
      "#{bold.call('ไฟล์')}: #{filename}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")
  when 'Restore.Failed'
    [
      bold.call('Restore Done'),
      "#{bold.call('สถานะ')}: ❌ Failed",
      "#{bold.call('ไฟล์')}: #{filename}",
      "#{bold.call('Error')}: #{error}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")
  else
    bold.call(eventType.to_s)
  end
end

def send_discord(webhookUrl, body, jobId)
  begin
    uri = URI.parse(webhookUrl)
    unless ['http', 'https'].include?(uri.scheme)
      puts "INFO : [#{jobId}] : Discord notify failed: unsupported scheme '#{uri.scheme}'"
      return
    end
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https')
    http.open_timeout = 5
    http.read_timeout = 5
    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request.body = body.to_json
    response = http.request(request)
    return response.code
  rescue => e
    puts "INFO : [#{jobId}] : Discord notify error: #{e.message}"
    return 'error'
  end
end

def send_telegram(botToken, chatId, message, jobId)
  begin
    return if botToken.nil? || botToken.empty? || chatId.nil? || chatId.empty?
    uri = URI.parse("https://api.telegram.org/bot#{botToken}/sendMessage")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.open_timeout = 5
    http.read_timeout = 5
    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request.body = { chat_id: chatId, text: message, parse_mode: 'Markdown' }.to_json
    response = http.request(request)
    return response.code
  rescue => e
    puts "INFO : [#{jobId}] : Telegram notify error: #{e.message}"
    return 'error'
  end
end

def create_noti_event_job(conn, eventType, hash)
  noti_job_id = SecureRandom.uuid
  now = Time.now.utc
  filename = hash['FILENAME'] || '-'
  bucket   = hash['BUCKET']   || '-'
  path     = hash['PATH']     || '-'
  schedule = hash['SCHEDULE'] || '-'
  error    = hash['ERROR']    || '-'

  description = case eventType
    when 'Backup.Done'
      "Backup completed: #{bucket}/#{path}/#{filename} (#{schedule})"
    when 'Backup.Failed'
      "Backup failed: #{error} (#{schedule})"
    else
      eventType
    end

  params_json = hash.map { |k, v| { 'Name' => k, 'Value' => v.to_s } }.to_json

  status = eventType.end_with?('.Done') || eventType.end_with?('.Success') ? 'Done' : 'Failed'
  sql = <<~SQL
    INSERT INTO "Jobs"
      (job_id, org_id, status, name, description, type, tags, configuration, progress_pct, succeed_cnt, failed_cnt, created_date, updated_date, end_date)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 100, $9, $10, $11, $11, $11)
  SQL
  succeed_cnt = status == 'Done' ? 1 : 0
  failed_cnt  = status == 'Done' ? 0 : 1
  conn.exec_params(sql, [noti_job_id, 'global', status, eventType, description, eventType, 'notify', params_json, succeed_cnt, failed_cnt, now])
  noti_job_id
rescue => e
  puts "WARN : create_noti_event_job error: #{e.message}"
  nil
end

def update_job_message2(conn, notiJobId, lines)
  return if notiJobId.nil?
  msg = lines.join("\n")
  conn.exec_params("UPDATE \"Jobs\" SET job_message2 = $1 WHERE job_id = $2", [msg, notiJobId])
rescue => e
  puts "WARN : update_job_message2 error: #{e.message}"
end

def process_event(stream, data, conn)
  eventType = data['Type']
  params    = data['Parameters'] || []
  hash      = params.map { |p| [p['Name'], p['Value']] }.to_h
  jobId     = hash['JOB_ID'] || 'unknown'
  logs      = []

  log = ->(msg) {
    puts "INFO : [#{jobId}] : #{msg}"
    logs << "INFO : [#{jobId}] : #{msg}"
  }

  log.call("Processing #{eventType} from stream [#{stream}]")

  notiJobId = create_noti_event_job(conn, eventType, hash)

  channels = get_noti_channels(conn, 'global')
  if channels.empty?
    log.call("No enabled notification channels for global")
    update_job_message2(conn, notiJobId, logs)
    return
  end

  matchedCount = 0
  channels.each do |channel|
    next unless channel_matches_event?(channel, eventType)
    matchedCount += 1
    channelName = channel['channel_name']
    type = channel['type']

    case type
    when 'Discord'
      webhookUrl = channel['discord_webhook_url']
      if webhookUrl.nil? || webhookUrl.empty?
        log.call("Skip Discord channel [#{channelName}] — no webhook URL")
        next
      end
      embed = build_discord_embed(eventType, hash)
      log.call("Notifying Discord [#{channelName}]")
      resp_code = send_discord(webhookUrl, { embeds: [embed] }, jobId)
      log.call("Discord response: #{resp_code}")

    when 'Telegram'
      botToken = channel['telegram_webhook_url']
      chatId   = channel['telegram_chat_id']
      message  = build_message(eventType, hash, ->(s) { "*#{s}*" })
      log.call("Notifying Telegram [#{channelName}]")
      resp_code = send_telegram(botToken, chatId, message, jobId)
      log.call("Telegram response: #{resp_code}")

    else
      log.call("Skip channel [#{channelName}] — unsupported type [#{type}]")
    end
  end

  log.call("Done — matched #{matchedCount} channel(s) for event [#{eventType}]")
  update_job_message2(conn, notiJobId, logs)
end

def drain_stream(redis, group_name, consumer_name, streams, conn)
  ids     = Array.new(streams.size, '0')
  entries = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
  return 0 unless entries
  count = 0
  entries.each do |stream, messages|
    messages.each do |id, fields|
      begin
        data = JSON.parse(fields['message']) rescue nil
        process_event(stream, data, conn) if data
      rescue => e
        puts "WARN : drain error on #{id}: #{e.message}"
      ensure
        redis.xack(stream, group_name, id) rescue nil
        count += 1
      end
    end
  end
  count
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost   = ENV['REDIS_HOST']
redisPort   = ENV['REDIS_PORT']
pgHost      = ENV['PG_HOST']
pgDb        = ENV['PG_DB']

group_name    = 'k8s-job-generic-notify'
consumer_name = 'job-dispatcher-generic-notify'
streams = [
  "JobSubmitted:#{environment}:Backup.Done",
  "JobSubmitted:#{environment}:Restore.Success",
  "JobSubmitted:#{environment}:Restore.Failed",
]

puts "INFO : ### job-dispatcher-generic-notify starting"
puts "INFO : ### ENVIRONMENT=[#{environment}]"
puts "INFO : ### REDIS_HOST=[#{redisHost}]"

redis = Redis.new(host: redisHost, port: redisPort, read_timeout: 10, reconnect_attempts: 2)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, '$', mkstream: true)
    puts "INFO : ### Created group [#{group_name}] for stream [#{stream_key}]"
  rescue Redis::CommandError => e
    puts "INFO : ### Group already exists for [#{stream_key}]" if e.message.include?('BUSYGROUP')
  end
end

conn = nil

loop do
  begin
    # Reconnect to PostgreSQL if connection is lost
    if conn.nil? || conn.finished?
      puts "INFO : ### Connecting to PostgreSQL [#{pgHost}] [#{pgDb}]"
      conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
      if conn.nil?
        puts "ERROR : ### Unable to connect to PostgreSQL — retrying in 10s"
        sleep 10
        next
      end
      puts "INFO : ### Connected to PostgreSQL"
      n = drain_stream(redis, group_name, consumer_name, streams, conn)
      puts "INFO : ### Drained #{n} pending PEL message(s)" if n > 0
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

    entries.each do |stream, messages|
      messages.each do |id, fields|
        begin
          data = JSON.parse(fields['message']) rescue nil
          process_event(stream, data, conn) if data
        rescue => e
          puts "ERROR : ### process_event error [#{id}]: #{e.message}"
        ensure
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
