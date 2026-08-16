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

def format_amount(value)
  return '-' if value.nil? || value.to_s.strip.empty?
  begin
    num = Float(value.to_s.gsub(',', ''))
    integer_part, decimal_part = ('%.2f' % num).split('.')
    integer_with_commas = integer_part.reverse.scan(/.{1,3}/).join(',').reverse
    "#{integer_with_commas}.#{decimal_part}"
  rescue
    value.to_s
  end
end

def build_discord_embed(eventType, hash)
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')

  case eventType
  when 'Payment.Success'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    amount = format_amount(hash['PAYIN_GENERATED_AMOUNT'] || hash['PAYIN_REQUEST_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    {
      title: 'Payment In Success',
      color: 0x57F287,
      description: [
        "**ร้านค้า**: #{merchantName} (#{merchantCode})",
        "**ยอดเงิน**: #{amount} THB",
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Ref1**: #{ref1}",
        "**Ref2**: #{ref2}",
        "**Ref3**: #{ref3}",
        "**เวลา**: #{now}",
      ].join("\n")
    }

  when 'PaymentOut.Success'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    txAmount = format_amount(hash['TX_AMOUNT'])
    requestAmount = format_amount(hash['PAYOUT_REQUEST_AMOUNT'])
    bankCode = hash['PAYOUT_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYOUT_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYOUT_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']
    isPartial = hash['PAYOUT_IS_PARTIAL'].to_s.downcase == 'true'

    {
      title: 'Payment Out Success',
      color: 0x57F287,
      description: [
        "**ร้านค้า**: #{merchantName} (#{merchantCode})",
        "**ยอดโอนจริง**: #{txAmount} THB",
        "**ยอดที่ขอ**: #{requestAmount} THB",
        isPartial ? "**P2P Partial**: True" : nil,
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Ref1**: #{ref1}",
        "**Ref2**: #{ref2}",
        "**Ref3**: #{ref3}",
        "**เวลา**: #{now}",
      ].compact.join("\n")
    }

  when 'PaymentIn.Rejected'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    statusCode = hash['STATUS_CODE'] || '-'
    statusReason = hash['STATUS_REASON'].to_s.empty? ? '-' : hash['STATUS_REASON']
    amount = format_amount(hash['PAYIN_REQUEST_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    {
      title: 'Payment In Rejected',
      color: 0xED4245,
      description: [
        "**ร้านค้า**: #{merchantName} (#{merchantCode})",
        "**ยอดเงิน**: #{amount} THB",
        "**Status Code**: #{statusCode}",
        "**เหตุผล**: #{statusReason}",
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Ref1**: #{ref1}",
        "**Ref2**: #{ref2}",
        "**Ref3**: #{ref3}",
        "**เวลา**: #{now}",
      ].join("\n")
    }

  when 'PaymentOut.Rejected'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    statusCode = hash['STATUS_CODE'] || '-'
    statusReason = hash['STATUS_REASON'].to_s.empty? ? '-' : hash['STATUS_REASON']
    amount = format_amount(hash['PAYOUT_REQUEST_AMOUNT'])
    bankCode = hash['PAYOUT_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYOUT_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYOUT_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    {
      title: 'Payment Out Rejected',
      color: 0xED4245,
      description: [
        "**ร้านค้า**: #{merchantName} (#{merchantCode})",
        "**ยอดเงิน**: #{amount} THB",
        "**Status Code**: #{statusCode}",
        "**เหตุผล**: #{statusReason}",
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Ref1**: #{ref1}",
        "**Ref2**: #{ref2}",
        "**Ref3**: #{ref3}",
        "**เวลา**: #{now}",
      ].join("\n")
    }

  when 'Payment.DailyTxAmountLimitExceeded'
    bankCode = hash['BANK_CODE'] || '-'
    bankAccountNo = hash['BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['BANK_ACCOUNT_NAME'] || '-'
    dailyQuota = format_amount(hash['BANK_ACCOUNT_DAILY_QUOTA'])
    currentAmount = format_amount(hash['CURRENT_DAILY_TX_AMOUNT'])

    {
      title: 'Daily Tx Amount Limit Exceeded',
      color: 0xED4245,
      description: [
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Daily Limit**: #{dailyQuota} THB",
        "**ยอดปัจจุบัน**: #{currentAmount} THB",
        "**เวลา**: #{now}",
      ].join("\n")
    }

  when 'Payment.Unidentified'
    amount = format_amount(hash['TX_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || hash['BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || hash['BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || hash['BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    {
      title: 'Payment Unidentified',
      color: 0xFFA500,
      description: [
        "**ยอดเงิน**: #{amount} THB",
        "**ธนาคาร**: #{bankCode} #{bankAccountNo} #{bankAccountName}",
        "**Ref1**: #{ref1}",
        "**Ref2**: #{ref2}",
        "**Ref3**: #{ref3}",
        "**เวลา**: #{now}",
      ].join("\n")
    }

  else
    { title: eventType.to_s, color: 0x99AAB5, description: '' }
  end
end

def build_message(eventType, hash, bold)
  now = Time.now.strftime('%Y-%m-%d %H:%M:%S')

  case eventType
  when 'Payment.Success'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    amount = format_amount(hash['PAYIN_GENERATED_AMOUNT'] || hash['PAYIN_REQUEST_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    [
      bold.call('Payment In Success'),
      "#{bold.call('ร้านค้า')}: #{merchantName} (#{merchantCode})",
      "#{bold.call('ยอดเงิน')}: #{amount} THB",
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Ref1')}: #{ref1}",
      "#{bold.call('Ref2')}: #{ref2}",
      "#{bold.call('Ref3')}: #{ref3}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")

  when 'PaymentOut.Success'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    txAmount = format_amount(hash['TX_AMOUNT'])
    requestAmount = format_amount(hash['PAYOUT_REQUEST_AMOUNT'])
    bankCode = hash['PAYOUT_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYOUT_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYOUT_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    isPartial = hash['PAYOUT_IS_PARTIAL'].to_s.downcase == 'true'
    [
      bold.call('Payment Out Success'),
      "#{bold.call('ร้านค้า')}: #{merchantName} (#{merchantCode})",
      "#{bold.call('ยอดโอนจริง')}: #{txAmount} THB",
      "#{bold.call('ยอดที่ขอ')}: #{requestAmount} THB",
      isPartial ? "#{bold.call('P2P Partial')}: True" : nil,
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Ref1')}: #{ref1}",
      "#{bold.call('Ref2')}: #{ref2}",
      "#{bold.call('Ref3')}: #{ref3}",
      "#{bold.call('เวลา')}: #{now}",
    ].compact.join("\n")

  when 'PaymentIn.Rejected'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    statusCode = hash['STATUS_CODE'] || '-'
    statusReason = hash['STATUS_REASON'].to_s.empty? ? '-' : hash['STATUS_REASON']
    amount = format_amount(hash['PAYIN_REQUEST_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    [
      bold.call('Payment In Rejected'),
      "#{bold.call('ร้านค้า')}: #{merchantName} (#{merchantCode})",
      "#{bold.call('ยอดเงิน')}: #{amount} THB",
      "#{bold.call('Status Code')}: #{statusCode}",
      "#{bold.call('เหตุผล')}: #{statusReason}",
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Ref1')}: #{ref1}",
      "#{bold.call('Ref2')}: #{ref2}",
      "#{bold.call('Ref3')}: #{ref3}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")

  when 'PaymentOut.Rejected'
    merchantName = hash['MERCHANT_NAME'] || hash['MERCHANT_CODE'] || '-'
    merchantCode = hash['MERCHANT_CODE'] || '-'
    statusCode = hash['STATUS_CODE'] || '-'
    statusReason = hash['STATUS_REASON'].to_s.empty? ? '-' : hash['STATUS_REASON']
    amount = format_amount(hash['PAYOUT_REQUEST_AMOUNT'])
    bankCode = hash['PAYOUT_BANK_CODE'] || '-'
    bankAccountNo = hash['PAYOUT_BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYOUT_BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    [
      bold.call('Payment Out Rejected'),
      "#{bold.call('ร้านค้า')}: #{merchantName} (#{merchantCode})",
      "#{bold.call('ยอดเงิน')}: #{amount} THB",
      "#{bold.call('Status Code')}: #{statusCode}",
      "#{bold.call('เหตุผล')}: #{statusReason}",
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Ref1')}: #{ref1}",
      "#{bold.call('Ref2')}: #{ref2}",
      "#{bold.call('Ref3')}: #{ref3}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")

  when 'Payment.DailyTxAmountLimitExceeded'
    bankCode = hash['BANK_CODE'] || '-'
    bankAccountNo = hash['BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['BANK_ACCOUNT_NAME'] || '-'
    dailyQuota = format_amount(hash['BANK_ACCOUNT_DAILY_QUOTA'])
    currentAmount = format_amount(hash['CURRENT_DAILY_TX_AMOUNT'])

    [
      bold.call('Daily Tx Amount Limit Exceeded'),
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Daily Limit')}: #{dailyQuota} THB",
      "#{bold.call('ยอดปัจจุบัน')}: #{currentAmount} THB",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")

  when 'Payment.Unidentified'
    amount = format_amount(hash['TX_AMOUNT'])
    bankCode = hash['PAYIN_BANK_CODE'] || hash['BANK_CODE'] || '-'
    bankAccountNo = hash['PAYIN_BANK_ACCOUNT_NO'] || hash['BANK_ACCOUNT_NO'] || '-'
    bankAccountName = hash['PAYIN_BANK_ACCOUNT_NAME'] || hash['BANK_ACCOUNT_NAME'] || '-'
    ref1 = hash['PMR_REF_ID1'].to_s.empty? ? '-' : hash['PMR_REF_ID1']
    ref2 = hash['PMR_REF_ID2'].to_s.empty? ? '-' : hash['PMR_REF_ID2']
    ref3 = hash['PMR_REF_ID3'].to_s.empty? ? '-' : hash['PMR_REF_ID3']

    [
      bold.call('Payment Unidentified'),
      "#{bold.call('ยอดเงิน')}: #{amount} THB",
      "#{bold.call('ธนาคาร')}: #{bankCode} #{bankAccountNo} #{bankAccountName}",
      "#{bold.call('Ref1')}: #{ref1}",
      "#{bold.call('Ref2')}: #{ref2}",
      "#{bold.call('Ref3')}: #{ref3}",
      "#{bold.call('เวลา')}: #{now}",
    ].join("\n")

  else
    bold.call(eventType.to_s)
  end
end

def send_discord(webhookUrl, body, lines, jobId)
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
    request.body = body.to_json

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

    embed = build_discord_embed(eventType, hash)
    str = "INFO : [#{jobId}] : Notifying Discord channel [#{channelName}]"
    lines << str
    puts(str)
    send_discord(webhookUrl, { embeds: [embed] }, lines, jobId)

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
  orgId = 'global'

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
    update_job_done2(conn, jobId, 0, 1, message)
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
  update_job_done2(conn, jobId, 1, 0, message)
end

KNOWN_JOB_TYPES = %w[
  Payment.Success PaymentOut.Success PaymentIn.Rejected
  PaymentOut.Rejected Payment.DailyTxAmountLimitExceeded Payment.Unidentified
].freeze

def drain_pending(redis, group_name, consumer_name, streams, conn)
  ids     = Array.new(streams.size, '0')
  entries = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
  return 0 unless entries
  count = 0
  entries.each do |stream, messages|
    messages.each do |id, fields|
      begin
        data = JSON.parse(fields['message']) rescue nil
        if data && KNOWN_JOB_TYPES.include?(data['Type'])
          process_payment_success_job(stream, data, conn)
        end
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

environment   = ENV['ENVIRONMENT']
redisHost     = ENV['REDIS_HOST']
redisPort     = ENV['REDIS_PORT']
pgHost        = ENV['PG_HOST']
pgDb          = ENV['PG_DB']

group_name    = 'k8s-job-notify'
consumer_name = 'k8s-job-dispatcher-notify'
streams = [
  "JobSubmitted:#{environment}:Payment.Success",
  "JobSubmitted:#{environment}:PaymentOut.Success",
  "JobSubmitted:#{environment}:PaymentIn.Rejected",
  "JobSubmitted:#{environment}:PaymentOut.Rejected",
  "JobSubmitted:#{environment}:Payment.DailyTxAmountLimitExceeded",
  "JobSubmitted:#{environment}:Payment.Unidentified",
]

puts "INFO : ### job-dispatcher-payment-notify starting"
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

MAX_PG_RECONNECT = 3

conn = nil
pg_reconnect_count = 0

loop do
  begin
    if conn.nil? || conn.finished?
      puts "INFO : ### Connecting to PostgreSQL [#{pgHost}] [#{pgDb}]"
      conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
      if conn.nil?
        pg_reconnect_count += 1
        puts "ERROR : ### Unable to connect to PostgreSQL (#{pg_reconnect_count}/#{MAX_PG_RECONNECT})"
        if pg_reconnect_count >= MAX_PG_RECONNECT
          puts "ERROR : ### PG reconnect failed #{MAX_PG_RECONNECT} times — exiting"
          exit 1
        end
        sleep 10
        next
      end
      pg_reconnect_count = 0
      puts "INFO : ### Connected to PostgreSQL"
      n = drain_pending(redis, group_name, consumer_name, streams, conn)
      puts "INFO : ### Drained #{n} pending PEL message(s)" if n > 0
    end

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
            process_payment_success_job(stream, data, conn)
          end
          redis.xack(stream, group_name, id) rescue nil
        rescue PG::Error => e
          puts "ERROR : ### PG error [#{id}]: #{e.message} — leaving in PEL for retry"
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
    pg_reconnect_count += 1
    puts "ERROR : ### PostgreSQL error: #{e.message} (#{pg_reconnect_count}/#{MAX_PG_RECONNECT})"
    if pg_reconnect_count >= MAX_PG_RECONNECT
      puts "ERROR : ### PG failed #{MAX_PG_RECONNECT} times — exiting"
      exit 1
    end
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
