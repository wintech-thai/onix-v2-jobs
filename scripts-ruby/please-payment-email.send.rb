#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'json'
require 'redis'

require './utils'

if File.exist?('env.rb')
  require './env'
end

def build_registration_email(hash)
  firstName = hash['FIRST_NAME'].to_s.strip
  link = hash['REGISTRATION_LINK']
  lang = (hash['LANG'] || 'en').to_s.downcase
  lang = 'en' unless lang == 'th'

  expireDate = begin
    Time.parse(hash['EXPIRE_DATE'].to_s).localtime.strftime('%Y-%m-%d %H:%M')
  rescue
    hash['EXPIRE_DATE'].to_s
  end

  greeting = firstName.empty? ? '' : " #{firstName}"

  if lang == 'th'
    subject = 'ลงทะเบียนร้านค้าของคุณให้เสร็จสมบูรณ์'
    text = <<~TEXT
    สวัสดีคุณ#{greeting},

    กรุณาคลิกลิงก์ด้านล่างเพื่อกรอกข้อมูลลงทะเบียนร้านค้าของคุณให้เสร็จสมบูรณ์:
    #{link}

    ลิงก์นี้จะหมดอายุวันที่ #{expireDate}

    หากคุณไม่ได้ทำการขอลงทะเบียนนี้ กรุณาเพิกเฉยต่ออีเมลนี้

    ขอแสดงความนับถือ
    ทีมสนับสนุน
    TEXT
  else
    subject = 'Complete your merchant registration'
    text = <<~TEXT
    Hello#{greeting},

    Please click the link below to complete your merchant registration:
    #{link}

    This link will expire on #{expireDate}.

    If you did not request this, you can safely ignore this email.

    Best regards,
    Support Team
    TEXT
  end

  [subject, text]
end

def process_email_job(stream, data, conn, smtpUser, smtpPassword, fromDomain, notifyAddress)
  lines = []
  jobId = data['Id']
  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  emailAddr = hash['EMAIL_OTP_ADDRESS']
  link = hash['REGISTRATION_LINK']

  str = "INFO : [#{jobId}] : Processing [#{data['Tags']}] job for [#{emailAddr}]"
  puts(str); lines << str

  update_job_status(conn, jobId, 'Submitted')
  update_job_status(conn, jobId, 'Running')

  if emailAddr.to_s.strip.empty? || link.to_s.strip.empty?
    str = "ERROR : [#{jobId}] : Missing EMAIL_OTP_ADDRESS or REGISTRATION_LINK — skip"
    puts(str); lines << str
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    return
  end

  subject, text = build_registration_email(hash)

  emailObj = {
    'from' => "no-reply@#{fromDomain}",
    'to' => emailAddr,
    'bcc' => notifyAddress,
    'subject' => subject,
    'text' => text,
  }

  send_email_smtp(emailObj, smtpUser, smtpPassword)

  str = "INFO : [#{jobId}] : Done sending email to [#{emailAddr}]"
  puts(str); lines << str

  update_job_done(conn, jobId, 1, 0, lines.join("\n"))
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
pgHost = ENV['PG_HOST']
pgDb = ENV['PG_DB']
smtpUser = ENV['SMTP_USER']
smtpPassword = ENV['SMTP_PASSWORD']
fromDomain = fallback(ENV['SMTP_FROM_DOMAIN'], 'please-payment.com')
notifyAddress = fallback(ENV['EMAIL_NOTI_ADDRESS'], 'support@please-payment.com')

group_name = 'k8s-job'
consumer_name = 'k8s-job-dispatcher'
streams = ["JobSubmitted:#{environment}:PleasePaymentEmailSend"]

puts "INFO : ### please-payment-email.send starting"
puts "INFO : ### ENVIRONMENT=[#{environment}]"
puts "INFO : ### REDIS_HOST=[#{redisHost}]"

redis = Redis.new(host: redisHost, port: redisPort, read_timeout: 10, reconnect_attempts: 2)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, '$', mkstream: true)
    puts "INFO : ### Created group [#{group_name}] for stream [#{stream_key}]"
  rescue Redis::CommandError => e
    puts "INFO : ### Group already created for stream [#{stream_key}]" if e.message.include?('BUSYGROUP')
  end
end

MAX_PG_RECONNECT = 3
conn = nil
pg_reconnect_count = 0

def drain_pending(redis, group_name, consumer_name, streams, conn, smtpUser, smtpPassword, fromDomain, notifyAddress)
  ids = Array.new(streams.size, '0')
  entries = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
  return unless entries
  entries.each do |stream, messages|
    messages.each do |id, fields|
      begin
        data = JSON.parse(fields['message']) rescue nil
        process_email_job(stream, data, conn, smtpUser, smtpPassword, fromDomain, notifyAddress) if data
      rescue => e
        puts "WARN : PEL drain error [#{id}]: #{e.message}"
      ensure
        redis.xack(stream, group_name, id) rescue nil
      end
    end
  end
end

loop do
  begin
    if conn.nil? || conn.finished?
      puts "INFO : ### Connecting to PostgreSQL [#{pgHost}] [#{pgDb}]"
      conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
      if conn.nil?
        pg_reconnect_count += 1
        if pg_reconnect_count >= MAX_PG_RECONNECT
          puts "ERROR : ### PG reconnect failed #{MAX_PG_RECONNECT} times — exiting"
          exit 1
        end
        puts "ERROR : ### Unable to connect (attempt #{pg_reconnect_count}/#{MAX_PG_RECONNECT}) — retrying in 10s"
        sleep 10
        next
      end
      pg_reconnect_count = 0
      puts "INFO : ### Connected to PostgreSQL"
      drain_pending(redis, group_name, consumer_name, streams, conn, smtpUser, smtpPassword, fromDomain, notifyAddress)
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
          process_email_job(stream, data, conn, smtpUser, smtpPassword, fromDomain, notifyAddress) if data
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
