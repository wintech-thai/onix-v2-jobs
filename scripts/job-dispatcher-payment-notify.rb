#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'net/http'
require 'json'

require './utils'

if File.exist?('env.rb')
  #Default environment variables
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

def build_message(eventType, hash, bold)
  merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
  merchantCode = hash['MERCHANT_CODE'] || '-'
  amount = hash['PAYIN_GENERATED_AMOUNT'] || hash['PAYIN_REQUEST_AMOUNT'] || '-'
  bankCode = hash['PAYIN_BANK_CODE'] || '-'
  bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || '-'
  refId = hash['PMR_REF_ID'] || '-'
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')

  title =
    case eventType
    when 'Payment.Success'
      "#{bold.call('Payment Success')}"
    else
      "#{bold.call("#{eventType}")}"
    end

  [
    title,
    "#{bold.call('ร้านค้า')}: #{merchantName} (#{merchantCode})",
    "#{bold.call('ยอดเงิน')}: #{amount} THB",
    "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo}",
    "#{bold.call('Ref')}: #{refId}",
    "#{bold.call('เวลา')}: #{now}",
  ].join("\n")
end

def send_discord(webhookUrl, message, lines, jobId)
  begin
    uri = URI.parse(webhookUrl)

    unless ['http', 'https'].include?(uri.scheme)
      str = "INFO : [#{jobId}] : Discord notify failed: unsupported URL scheme '#{uri.scheme}'"
      lines << str
      puts(str)
      return nil
    end

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = (uri.scheme == 'https')
    http.open_timeout = 5
    http.read_timeout = 5

    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request.body = { content: message }.to_json

    response = http.request(request)

    str = "INFO : [#{jobId}] : Discord notify response: status=#{response.code}"
    lines << str
    puts(str)

    response
  rescue StandardError => ex
    str = "INFO : [#{jobId}] : Discord notify failed: #{ex.class} #{ex.message}"
    lines << str
    puts(str)
    nil
  end
end

def send_telegram(botToken, chatId, message, lines, jobId)
  begin
    if botToken.nil? || botToken.empty? || chatId.nil? || chatId.empty?
      str = "INFO : [#{jobId}] : Telegram notify failed: missing bot token or chat id"
      lines << str
      puts(str)
      return nil
    end

    uri = URI.parse("https://api.telegram.org/bot#{botToken}/sendMessage")

    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.open_timeout = 5
    http.read_timeout = 5

    request = Net::HTTP::Post.new(uri)
    request['Content-Type'] = 'application/json'
    request.body = { chat_id: chatId, text: message, parse_mode: 'Markdown' }.to_json

    response = http.request(request)

    str = "INFO : [#{jobId}] : Telegram notify response: status=#{response.code}"
    lines << str
    puts(str)

    response
  rescue StandardError => ex
    str = "INFO : [#{jobId}] : Telegram notify failed: #{ex.class} #{ex.message}"
    lines << str
    puts(str)
    nil
  end
end

def notify_channel(channel, eventType, hash, lines, jobId)
  channelName = channel['channel_name']
  type = channel['type']

  case type
  when 'Discord'
    webhookUrl = channel['discord_webhook_url']
    if webhookUrl.nil? || webhookUrl.empty?
      str = "INFO : [#{jobId}] : Skip channel [#{channelName}] (Discord) : discord_webhook_url not configured"
      lines << str
      puts(str)
      return
    end

    message = build_message(eventType, hash, ->(s) { "**#{s}**" })
    str = "INFO : [#{jobId}] : Notifying Discord channel [#{channelName}]"
    lines << str
    puts(str)
    send_discord(webhookUrl, message, lines, jobId)

  when 'Telegram'
    botToken = channel['telegram_webhook_url']
    chatId = channel['telegram_chat_id']

    message = build_message(eventType, hash, ->(s) { "*#{s}*" })
    str = "INFO : [#{jobId}] : Notifying Telegram channel [#{channelName}]"
    lines << str
    puts(str)
    send_telegram(botToken, chatId, message, lines, jobId)

  else
    str = "INFO : [#{jobId}] : Skip channel [#{channelName}] : unsupported type [#{type}]"
    lines << str
    puts(str)
  end
end

def process_payment_success_job(stream, data, conn)
  lines = []
  jobId = data['Id']
  eventType = data['Type']

  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  merchantId = hash['MERCHANT_ID']
  merchantCode = hash['MERCHANT_CODE']
  orgId = fallback(hash['ORG_ID'], 'global')

  str = "INFO : [#{jobId}] : Processing job from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str)
  lines.push(str)

  jobStatus = 'Submitted'
  update_job_status(conn, jobId, jobStatus)

  jobStatus = 'Running'
  update_job_status(conn, jobId, jobStatus)

  channels = get_noti_channels(conn, orgId)
  if channels.empty?
    str = "ERROR : [#{jobId}] : No enabled notification channel found for org [#{orgId}]"
    puts(str)
    lines.push(str)

    message = lines.join("\n")
    update_job_done(conn, jobId, 0, 1, message)
    return
  end

  matchedCount = 0
  channels.each do |channel|
    next unless channel_matches_event?(channel, eventType)

    matchedCount += 1
    notify_channel(channel, eventType, hash, lines, jobId)
  end

  str = "INFO : [#{jobId}] : Done processing job from stream [#{stream}], matched [#{matchedCount}] channel(s) for event [#{eventType}]"
  puts(str)
  lines.push(str)

  message = lines.join("\n")
  update_job_done(conn, jobId, 1, 0, message)
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']

# กลุ่ม consumer แยกออกจาก job-dispatcher-payment.rb (group "k8s-job")
# เพื่อให้ Redis Stream ส่ง message เดียวกันไปทั้งสองฝั่ง (webhook + notify) แบบ fan-out
# ถ้าใช้ group เดียวกัน message จะถูกแบ่งกันประมวลผล ไม่ใช่ทั้งสองฝั่งได้รับ event เดียวกัน
group_name   = "k8s-job-notify"
consumer_name = "k8s-job-dispatcher-notify"
streams = [
  "JobSubmitted:#{environment}:Payment.Success",
]

puts("INFO : ### Start dispatching notify jobs.")
puts("INFO : ### ENVIRONMENT=[#{environment}]")
puts("INFO : ### REDIS_HOST=[#{redisHost}]")
puts("INFO : ### REDIS_PORT=[#{redisPort}]")


pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]
conn = connect_db(pgHost, pgDb, ENV["PG_USER"], ENV["PG_PASSWORD"])
if (conn.nil?)
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end
puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")


redis = Redis.new(host: redisHost, port: redisPort)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, "$", mkstream: true)
    puts("INFO : ### Created group [#{group_name}] for stream [#{stream_key}]")
  rescue Redis::CommandError => e
    puts("INFO : ### Group already created for stream [#{stream_key}]") if e.message.include?("BUSYGROUP")
  end
end

# Loop อ่าน message จากทุก stream
stream_offsets = streams.map { |s| [s, ">"] }.to_h
loop do
  # ใช้ Hash => { stream_key => ">" }
  entries = redis.xreadgroup(
    group_name,
    consumer_name,
    streams,                        # stream keys
    Array.new(streams.size, ">"),   # ตำแหน่งเริ่ม (ทุก stream ใช้ ">")
    count: 10,
    block: 5000
  )

  if entries
    entries.each do |stream, messages|
      messages.each do |id, fields|
        #puts("INFO : ### Got [#{id}] from stream [#{stream}], group [#{group_name}]")
        redis.xack(stream, group_name, id)

        rawJson = fields["message"]
        data = JSON.parse(rawJson) rescue nil

        jobType = data['Type']
        if jobType == 'Payment.Success'
          process_payment_success_job(stream, data, conn)
        end

      end
    end
  end
end
