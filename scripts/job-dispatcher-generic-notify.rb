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
  filename = hash['FILENAME'] || '-'
  bucket   = hash['BUCKET']   || '-'
  path     = hash['PATH']     || '-'
  schedule = hash['SCHEDULE'] || '-'
  error    = hash['ERROR']    || '-'

  case eventType
  when 'Backup.Done'
    {
      title: 'Backup Done',
      color: 0x57F287,
      description: [
        "**ไฟล์**: #{filename}",
        "**Bucket**: #{bucket}",
        "**Path**: #{path}",
        "**Schedule**: #{schedule}",
        "**เวลา**: #{now}",
      ].join("\n")
    }
  when 'Backup.Failed'
    {
      title: 'Backup Failed',
      color: 0xED4245,
      description: [
        "**Error**: #{error}",
        "**Schedule**: #{schedule}",
        "**เวลา**: #{now}",
      ].join("\n")
    }
  else
    { title: eventType.to_s, color: 0x99AAB5, description: '' }
  end
end

def build_message(eventType, hash, bold)
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')
  filename = hash['FILENAME'] || '-'
  bucket   = hash['BUCKET']   || '-'
  path     = hash['PATH']     || '-'
  schedule = hash['SCHEDULE'] || '-'
  error    = hash['ERROR']    || '-'

  case eventType
  when 'Backup.Done'
    [
      bold.call('Backup Done'),
      "#{bold.call('ไฟล์')}: #{filename}",
      "#{bold.call('Bucket')}: #{bucket}",
      "#{bold.call('Path')}: #{path}",
      "#{bold.call('Schedule')}: #{schedule}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")
  when 'Backup.Failed'
    [
      bold.call('Backup Failed'),
      "#{bold.call('Error')}: #{error}",
      "#{bold.call('Schedule')}: #{schedule}",
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
    puts "INFO : [#{jobId}] : Discord response: #{response.code}"
  rescue => e
    puts "INFO : [#{jobId}] : Discord notify error: #{e.message}"
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
    puts "INFO : [#{jobId}] : Telegram response: #{response.code}"
  rescue => e
    puts "INFO : [#{jobId}] : Telegram notify error: #{e.message}"
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

def process_event(stream, data, conn)
  eventType = data['Type']
  params    = data['Parameters'] || []
  hash      = params.map { |p| [p['Name'], p['Value']] }.to_h
  jobId     = hash['JOB_ID'] || 'unknown'

  puts "INFO : [#{jobId}] : Processing #{eventType} from stream [#{stream}]"

  create_noti_event_job(conn, eventType, hash)

  channels = get_noti_channels(conn, 'global')
  if channels.empty?
    puts "INFO : [#{jobId}] : No enabled notification channels for global"
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
        puts "INFO : [#{jobId}] : Skip Discord channel [#{channelName}] — no webhook URL"
        next
      end
      embed = build_discord_embed(eventType, hash)
      puts "INFO : [#{jobId}] : Notifying Discord [#{channelName}]"
      send_discord(webhookUrl, { embeds: [embed] }, jobId)

    when 'Telegram'
      botToken = channel['telegram_webhook_url']
      chatId   = channel['telegram_chat_id']
      message  = build_message(eventType, hash, ->(s) { "*#{s}*" })
      puts "INFO : [#{jobId}] : Notifying Telegram [#{channelName}]"
      send_telegram(botToken, chatId, message, jobId)

    else
      puts "INFO : [#{jobId}] : Skip channel [#{channelName}] — unsupported type [#{type}]"
    end
  end

  puts "INFO : [#{jobId}] : Done — matched #{matchedCount} channel(s)"
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
  "JobSubmitted:#{environment}:Backup.Failed",
]

puts "INFO : ### job-dispatcher-generic-notify starting"
puts "INFO : ### ENVIRONMENT=[#{environment}]"
puts "INFO : ### REDIS_HOST=[#{redisHost}]"

conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
if conn.nil?
  puts "ERROR : ### Unable to connect to PostgreSQL [#{pgHost}] [#{pgDb}]"
  exit 101
end
puts "INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]"

redis = Redis.new(host: redisHost, port: redisPort)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, '$', mkstream: true)
    puts "INFO : ### Created group [#{group_name}] for stream [#{stream_key}]"
  rescue Redis::CommandError => e
    puts "INFO : ### Group already exists for [#{stream_key}]" if e.message.include?('BUSYGROUP')
  end
end

loop do
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
      redis.xack(stream, group_name, id)
      data = JSON.parse(fields['message']) rescue nil
      next if data.nil?

      process_event(stream, data, conn)
    end
  end
end
