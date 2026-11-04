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

def build_approval_email(hash)
  firstName = hash['FIRST_NAME'].to_s.strip
  link = hash['SIGNUP_LINK']
  lang = (hash['LANG'] || 'en').to_s.downcase
  lang = 'en' unless lang == 'th'

  greeting = firstName.empty? ? '' : " #{firstName}"

  if lang == 'th'
    subject = 'คำขอลงทะเบียนร้านค้าของคุณได้รับการอนุมัติแล้ว'
    text = <<~TEXT
    สวัสดีคุณ#{greeting},

    ยินดีด้วย! คำขอลงทะเบียนร้านค้าของคุณได้รับการอนุมัติแล้ว

    กรุณาคลิกลิงก์ด้านล่างเพื่อตั้งค่ารหัสผ่านและเข้าสู่ระบบ:
    #{link}

    ขอแสดงความนับถือ
    ทีมสนับสนุน
    TEXT
  else
    subject = 'Your merchant registration has been approved'
    text = <<~TEXT
    Hello#{greeting},

    Congratulations! Your merchant registration request has been approved.

    Please click the link below to set your password and sign in:
    #{link}

    Best regards,
    Support Team
    TEXT
  end

  [subject, text]
end

def build_rejection_email(hash)
  firstName = hash['FIRST_NAME'].to_s.strip
  reason = hash['REASON'].to_s.strip
  reason = '-' if reason.empty?
  lang = (hash['LANG'] || 'en').to_s.downcase
  lang = 'en' unless lang == 'th'

  greeting = firstName.empty? ? '' : " #{firstName}"

  if lang == 'th'
    subject = 'คำขอลงทะเบียนร้านค้าของคุณไม่ได้รับการอนุมัติ'
    text = <<~TEXT
    สวัสดีคุณ#{greeting},

    ขออภัย คำขอลงทะเบียนร้านค้าของคุณไม่ได้รับการอนุมัติ ด้วยเหตุผลดังนี้:
    #{reason}

    หากมีข้อสงสัย กรุณาติดต่อทีมสนับสนุน

    ขอแสดงความนับถือ
    ทีมสนับสนุน
    TEXT
  else
    subject = 'Your merchant registration request was not approved'
    text = <<~TEXT
    Hello#{greeting},

    We're sorry, but your merchant registration request was not approved for the following reason:
    #{reason}

    If you have any questions, please contact our support team.

    Best regards,
    Support Team
    TEXT
  end

  [subject, text]
end

def build_test_email(hash)
  subject = 'SMTP Configuration Test — please-payment'
  text = <<~TEXT
    This is a test email sent at #{Time.now}.

    If you received this, your SMTP configuration (SMTP_USER / SMTP_PASSWORD / SMTP_FROM_DOMAIN) is working correctly.
  TEXT

  [subject, text]
end

# ถ้า ENV['SMTP_USER'] ไม่ได้ตั้งไว้ (หรือตั้งแต่เป็นค่าว่าง) ให้ fallback ไปอ่านจากตาราง Configurations
# (org_id='global', config_type='Smtp') แทน — อ่านใหม่ทุกครั้งที่ประมวลผล job (ไม่ cache ไว้ที่ process start)
# เพื่อให้ Admin เปลี่ยน config ผ่านหน้าจอแล้วมีผลทันทีโดยไม่ต้อง restart worker pod
#
# force_db: true สำหรับปุ่ม "Send Test Email" จากหน้าจอ — ต้องอ่านจาก DB เท่านั้นเสมอ
# (ข้าม ENV var ไปเลย) เพราะจุดประสงค์คือทดสอบว่า config ที่บันทึกไว้ใน DB ใช้งานได้จริงหรือไม่
def resolve_smtp_config(conn, lines, force_db: false)
  unless force_db
    envUser = ENV['SMTP_USER']
    envPassword = ENV['SMTP_PASSWORD']
    envFromDomain = ENV['SMTP_FROM_DOMAIN']

    unless envUser.to_s.strip.empty?
      lines << "INFO : Using SMTP config from ENV var (SMTP_USER=[#{envUser}])"
      return { user: envUser, password: envPassword, from_domain: fallback(envFromDomain, 'please-payment.com') }
    end

    lines << "INFO : SMTP_USER env var not set (or empty) — reading SMTP config from Configurations table (org_id=global)"
  else
    lines << "INFO : Test email — reading SMTP config from Configurations table (org_id=global) only, ENV var is ignored"
  end

  result = begin
    conn.exec("SELECT config_value FROM \"Configurations\" WHERE org_id = 'global' AND config_type = 'Smtp' AND status = 'Active' LIMIT 1")
  rescue PG::Error => e
    lines << "ERROR : Failed to query SMTP configuration from Configurations table: [#{e.message}]"
    return nil
  end

  if result.ntuples == 0
    lines << "ERROR : No active SMTP configuration found in Configurations table (org_id=global, config_type=Smtp)#{force_db ? '' : ', and SMTP_USER env var is not set'}"
    return nil
  end

  cfg = JSON.parse(result[0]['config_value']) rescue nil
  if cfg.nil? || cfg['SmtpUser'].to_s.strip.empty?
    lines << "ERROR : SMTP configuration stored in Configurations table is invalid or missing SmtpUser"
    return nil
  end

  lines << "INFO : Using SMTP config from DB (Configurations table, SmtpUser=[#{cfg['SmtpUser']}])"
  { user: cfg['SmtpUser'], password: cfg['SmtpPassword'], from_domain: fallback(cfg['SmtpFromDomain'], 'please-payment.com') }
end

def process_email_job(stream, data, conn, notifyAddress)
  lines = []
  jobId = data['Id']
  tags = data['Tags'].to_s
  isTestEmail = (tags == 'smtp-test-email')
  isRejectionEmail = (tags == 'service-request-rejected')
  isApprovalEmail = (tags == 'service-request-approved')
  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  emailAddr = isTestEmail ? hash['TEST_EMAIL_ADDRESS'] : hash['EMAIL_OTP_ADDRESS']
  link = isApprovalEmail ? hash['SIGNUP_LINK'] : hash['REGISTRATION_LINK']

  str = "INFO : [#{jobId}] : Processing [#{tags}] job for [#{emailAddr}]"
  puts(str); lines << str

  update_job_status(conn, jobId, 'Submitted')
  update_job_status(conn, jobId, 'Running')

  requiresLink = !isTestEmail && !isRejectionEmail
  if emailAddr.to_s.strip.empty? || (requiresLink && link.to_s.strip.empty?)
    missing = isTestEmail ? 'TEST_EMAIL_ADDRESS' : (requiresLink ? "EMAIL_OTP_ADDRESS or #{isApprovalEmail ? 'SIGNUP_LINK' : 'REGISTRATION_LINK'}" : 'EMAIL_OTP_ADDRESS')
    str = "ERROR : [#{jobId}] : Missing #{missing} — skip"
    puts(str); lines << str
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    return
  end

  smtpCfg = resolve_smtp_config(conn, lines, force_db: isTestEmail)
  if smtpCfg.nil?
    str = "ERROR : [#{jobId}] : SMTP configuration is not available — cannot send email"
    puts(str); lines << str
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
    return
  end

  subject, text = if isTestEmail
    build_test_email(hash)
  elsif isRejectionEmail
    build_rejection_email(hash)
  elsif isApprovalEmail
    build_approval_email(hash)
  else
    build_registration_email(hash)
  end

  emailObj = {
    'from' => "\"#{smtpCfg[:from_domain]}\" <#{smtpCfg[:user]}>",
    'to' => emailAddr,
    'bcc' => isTestEmail ? nil : notifyAddress,
    'subject' => subject,
    'text' => text,
  }

  sent, errMsg = send_email_smtp(emailObj, smtpCfg[:user], smtpCfg[:password])

  if sent
    str = "INFO : [#{jobId}] : Done sending email to [#{emailAddr}]"
    puts(str); lines << str
    update_job_done(conn, jobId, 1, 0, lines.join("\n"))
  else
    str = "ERROR : [#{jobId}] : Failed to send email to [#{emailAddr}] — #{errMsg}"
    puts(str); lines << str
    update_job_done(conn, jobId, 0, 1, lines.join("\n"))
  end
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
pgHost = ENV['PG_HOST']
pgDb = ENV['PG_DB']
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

def drain_pending(redis, group_name, consumer_name, streams, conn, notifyAddress)
  ids = Array.new(streams.size, '0')
  entries = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
  return unless entries
  entries.each do |stream, messages|
    messages.each do |id, fields|
      begin
        data = JSON.parse(fields['message']) rescue nil
        process_email_job(stream, data, conn, notifyAddress) if data
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
      drain_pending(redis, group_name, consumer_name, streams, conn, notifyAddress)
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
          process_email_job(stream, data, conn, notifyAddress) if data
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
