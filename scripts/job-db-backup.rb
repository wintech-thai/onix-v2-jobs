#!/usr/bin/env ruby

require 'pg'
require 'redis'
require 'json'
require 'time'
require 'securerandom'
require 'aws-sdk-s3'

require './utils'

if File.exist?('env.rb')
  require './env'
end

ENVIRONMENT   = ENV['ENVIRONMENT'] || 'Development'
PG_HOST       = ENV['PG_HOST']     || 'postgresql-onix'
PG_PORT       = (ENV['PG_PORT']    || '5432').to_i
PG_USER       = ENV['PG_USER']     || 'postgres'
PG_PASSWORD   = ENV['PG_PASSWORD'] || ''
PG_DB         = ENV['PG_DB']       || 'onix'
PG_NAMESPACE  = ENV['PG_NAMESPACE']  || 'default'
PG_POD_NAME   = ENV['PG_POD_NAME']   || 'postgresql-onix-0'
REDIS_HOST    = ENV['REDIS_HOST']  || 'redis-master'
REDIS_PORT    = (ENV['REDIS_PORT'] || '6379').to_i
SCRIPT_FILE   = 'pg-dump-db.bash'
TMP_DIR       = '/tmp'

$last_backup_slot = nil

def connect_db_local
  PG.connect(host: PG_HOST, port: PG_PORT, dbname: PG_DB, user: PG_USER, password: PG_PASSWORD)
end

def get_backup_policy(conn)
  sql = "SELECT config_value FROM \"Configurations\" WHERE org_id = 'global' AND config_type = 'BackupPolicy' AND status = 'Active' LIMIT 1"
  res = conn.exec(sql)
  return nil if res.ntuples == 0

  raw = res[0]['config_value']
  return nil if raw.nil? || raw.empty?

  JSON.parse(raw)
rescue => e
  puts "get_backup_policy error: #{e.message}"
  nil
end

def should_backup?(policy)
  return false if policy.nil? || policy['IsEnabled'] != true

  now = Time.now
  interval_hours = case (policy['ScheduleInterval'] || 'daily')
    when '4h'  then 4
    when '8h'  then 8
    when '12h' then 12
    else 24
  end

  start_hour = (policy['ScheduleStartHour'] || 0).to_i
  current_hour = now.hour
  current_min  = now.min

  hour_diff = (current_hour - start_hour) % 24
  return false unless (hour_diff % interval_hours) == 0 && current_min < 10

  # deduplicate: only trigger once per scheduled slot
  slot = Time.new(now.year, now.month, now.day, current_hour, 0, 0)
  return false if $last_backup_slot == slot

  true
end

def current_slot
  now = Time.now
  Time.new(now.year, now.month, now.day, now.hour, 0, 0)
end

def create_job(conn, name, description)
  job_id = SecureRandom.uuid
  now = Time.now.utc
  sql = <<~SQL
    INSERT INTO "Jobs"
      (job_id, org_id, status, name, description, type, tags, progress_pct, succeed_cnt, failed_cnt, created_date, updated_date)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 0, 0, 0, $8, $8)
  SQL
  conn.exec_params(sql, [job_id, 'global', 'Running', name, description, 'Backup.Schedule', 'backup', now])
  job_id
end

def append_log(conn, job_id, line)
  sql = <<~SQL
    UPDATE "Jobs"
    SET job_message2 = COALESCE(job_message2, '') || $2 || E'\n', updated_date = $3
    WHERE job_id = $1
  SQL
  conn.exec_params(sql, [job_id, line, Time.now.utc])
end

def finish_job(conn, job_id, success, message)
  status = success ? 'Done' : 'Failed'
  sql = <<~SQL
    UPDATE "Jobs"
    SET status = $2, job_message = $3, end_date = $4, updated_date = $4,
        succeed_cnt = $5, failed_cnt = $6, progress_pct = 100
    WHERE job_id = $1
  SQL
  conn.exec_params(sql, [job_id, status, message, Time.now.utc, (success ? 1 : 0), (success ? 0 : 1)])
end

def publish_backup_done(redis, job_id, policy, success, filename, error_msg)
  event_type = success ? 'Backup.Done' : 'Backup.Failed'
  stream = "JobSubmitted:#{ENVIRONMENT}:#{event_type}"
  params = [
    { 'Name' => 'JOB_ID',      'Value' => job_id },
    { 'Name' => 'SUCCESS',     'Value' => success.to_s },
    { 'Name' => 'BUCKET',      'Value' => policy['Bucket'] || '' },
    { 'Name' => 'PATH',        'Value' => policy['Path'] || '' },
    { 'Name' => 'FILENAME',    'Value' => filename },
    { 'Name' => 'ERROR',       'Value' => error_msg || '' },
    { 'Name' => 'SCHEDULE',    'Value' => "#{policy['ScheduleInterval']}@#{policy['ScheduleStartHour']}h" },
  ]
  payload = { 'Type' => event_type, 'Parameters' => params }.to_json
  redis.xadd(stream, { message: payload })
  puts "Published #{event_type} to #{stream}"
rescue => e
  puts "publish_backup_done error: #{e.message}"
end

def run_backup(policy, conn, redis)
  ts = Time.now.strftime('%Y%m%d%H%M%S')
  prefix = policy['FilePrefix']&.strip&.empty? ? 'please-payment' : policy['FilePrefix'].strip
  dmp_file    = "#{prefix}-backup-#{ts}.sql"
  dmp_file_gz = "#{dmp_file}.gz"
  local_gz    = "#{TMP_DIR}/#{dmp_file_gz}"
  remote_key  = [policy['Path']&.strip, dmp_file_gz].reject { |s| s.nil? || s.empty? }.join('/')

  job_id = create_job(conn, "Backup #{ts}", "PostgreSQL backup via policy: #{policy['ScheduleInterval']}@#{policy['ScheduleStartHour']}h")
  puts "Created job #{job_id}"

  begin
    # Step 1: copy script into postgresql pod
    append_log(conn, job_id, "[1/4] Copying #{SCRIPT_FILE} into pod #{PG_POD_NAME}...")
    rc = system("kubectl cp #{SCRIPT_FILE} -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/")
    raise "kubectl cp script failed (exit #{$?.exitstatus})" unless rc

    # Step 2: run pg_dump inside pod
    append_log(conn, job_id, "[2/4] Running pg_dump inside pod...")
    rc = system("kubectl exec -i -n #{PG_NAMESPACE} #{PG_POD_NAME} -- bash #{TMP_DIR}/#{SCRIPT_FILE} #{PG_USER} #{dmp_file} #{TMP_DIR} #{PG_PASSWORD}")
    raise "pg_dump exec failed (exit #{$?.exitstatus})" unless rc

    # Step 3: copy backup file out of pod
    append_log(conn, job_id, "[3/4] Copying #{dmp_file_gz} out of pod to #{local_gz}...")
    rc = system("kubectl cp -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/#{dmp_file_gz} #{local_gz}")
    raise "kubectl cp backup file failed (exit #{$?.exitstatus})" unless rc

    # Step 4: upload to S3-compatible cloud storage
    append_log(conn, job_id, "[4/4] Uploading #{dmp_file_gz} to #{policy['Bucket']}/#{remote_key}...")
    s3 = Aws::S3::Client.new(
      endpoint:          policy['StorageUrl'],
      access_key_id:     policy['StorageKey'],
      secret_access_key: policy['StorageSecret'],
      region:            'auto',
      force_path_style:  false
    )
    File.open(local_gz, 'rb') do |f|
      s3.put_object(bucket: policy['Bucket'], key: remote_key, body: f)
    end

    append_log(conn, job_id, "Backup complete: #{remote_key}")
    finish_job(conn, job_id, true, "Backup complete: #{policy['Bucket']}/#{remote_key}")
    publish_backup_done(redis, job_id, policy, true, dmp_file_gz, nil)
    puts "Backup succeeded: #{remote_key}"

  rescue => e
    puts "Backup error: #{e.message}"
    append_log(conn, job_id, "ERROR: #{e.message}")
    finish_job(conn, job_id, false, "Backup failed: #{e.message}")
    publish_backup_done(redis, job_id, policy, false, dmp_file_gz, e.message)
  ensure
    File.delete(local_gz) if File.exist?(local_gz)
  end
end

# ---- Main loop ----
$stdout.sync = true
puts "job-db-backup starting (env=#{ENVIRONMENT}, pod=#{PG_POD_NAME})"

redis = Redis.new(host: REDIS_HOST, port: REDIS_PORT)

loop do
  begin
    conn = connect_db_local
    policy = get_backup_policy(conn)

    if should_backup?(policy)
      $last_backup_slot = current_slot
      puts "Backup triggered at #{Time.now}"
      run_backup(policy, conn, redis)
    else
      puts "#{Time.now.strftime('%H:%M')} — no backup needed"
    end

    conn.close
  rescue => e
    puts "Loop error: #{e.message}"
  end

  sleep 600 # 10 minutes
end
