#!/usr/bin/env ruby

require 'pg'
require 'redis'
require 'json'
require 'time'
require 'securerandom'
require 'aws-sdk-s3'
require 'timeout'

require './utils'

if File.exist?('env.rb')
  require './env'
end

ENVIRONMENT         = ENV['ENVIRONMENT'] || 'Development'
PG_HOST             = ENV['PG_HOST']     || 'postgresql-onix'
PG_PORT             = (ENV['PG_PORT']    || '5432').to_i
PG_USER             = ENV['PG_USER']     || 'postgres'
PG_PASSWORD         = ENV['PG_PASSWORD'] || ''
PG_DB               = ENV['PG_DB']       || 'onix'
PG_NAMESPACE        = ENV['PG_NAMESPACE']  || 'default'
PG_POD_NAME         = ENV['PG_POD_NAME']   || 'postgresql-onix-0'
REDIS_HOST          = ENV['REDIS_HOST']  || 'redis-master'
REDIS_PORT          = (ENV['REDIS_PORT'] || '6379').to_i
SCRIPT_FILE         = 'pg-dump-db.bash'
RESTORE_SCRIPT_FILE = 'pg-restore-db.bash'
TMP_DIR             = '/tmp'
NOTIFIED_SET_KEY    = "BackupNotifiedJobs:#{ENVIRONMENT}"
NOTIFIED_SET_TTL    = 7 * 24 * 3600

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
  start_hour   = (policy['ScheduleStartHour'] || 0).to_i
  current_hour = now.hour
  hour_diff    = (current_hour - start_hour) % 24
  return false unless (hour_diff % interval_hours) == 0
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
  now    = Time.now.utc
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

# ── Notification tracking ──────────────────────────────────────────────────────
def mark_notified(redis, job_id)
  redis.sadd(NOTIFIED_SET_KEY, job_id)
  redis.expire(NOTIFIED_SET_KEY, NOTIFIED_SET_TTL)
rescue => e
  puts "mark_notified error: #{e.message}"
end

def notified?(redis, job_id)
  redis.sismember(NOTIFIED_SET_KEY, job_id)
rescue
  false
end

def publish_backup_done(redis, job_id, policy, success, filename, error_msg, extra = {})
  return if notified?(redis, job_id)
  event_type = 'Backup.Done'
  stream     = "JobSubmitted:#{ENVIRONMENT}:#{event_type}"
  params = [
    { 'Name' => 'JOB_ID',   'Value' => job_id },
    { 'Name' => 'SUCCESS',  'Value' => success.to_s },
    { 'Name' => 'BUCKET',   'Value' => policy['Bucket'] || '' },
    { 'Name' => 'PATH',     'Value' => policy['Path'] || '' },
    { 'Name' => 'FILENAME', 'Value' => filename },
    { 'Name' => 'ERROR',    'Value' => error_msg || '' },
    { 'Name' => 'SCHEDULE', 'Value' => "#{policy['ScheduleInterval']}@#{policy['ScheduleStartHour']}h" },
  ]
  extra.each { |k, v| params << { 'Name' => k.to_s, 'Value' => v.to_s } }
  payload = { 'Type' => event_type, 'Parameters' => params }.to_json
  3.times do |attempt|
    begin
      redis.xadd(stream, { message: payload })
      mark_notified(redis, job_id)
      puts "Published #{event_type} to #{stream}"
      return
    rescue => e
      puts "publish_backup_done error (attempt #{attempt + 1}/3): #{e.message}"
      sleep 5 if attempt < 2
    end
  end
end

def publish_restore_event(redis, event_type, job_id, filename, error_msg = nil)
  stream = "JobSubmitted:#{ENVIRONMENT}:#{event_type}"
  params = [
    { 'Name' => 'JOB_ID',   'Value' => job_id },
    { 'Name' => 'FILENAME', 'Value' => filename || '' },
    { 'Name' => 'ERROR',    'Value' => error_msg || '' },
  ]
  payload = { 'Type' => event_type, 'Parameters' => params }.to_json
  redis.xadd(stream, { message: payload })
  puts "Published #{event_type} to #{stream}"
rescue => e
  puts "publish_restore_event error: #{e.message}"
end

# ── Startup helpers ────────────────────────────────────────────────────────────
def cleanup_stuck_jobs(conn)
  res = conn.exec_params(
    "UPDATE \"Jobs\" SET status = 'Failed', end_date = NOW(), updated_date = NOW(), job_message = 'Job timed out — marked failed on restart'
     WHERE status = 'Running' AND type IN ('Backup.Schedule', 'Backup.Adhoc', 'Backup.Restore')
       AND start_date < NOW() - INTERVAL '30 minutes'
     RETURNING job_id",
    []
  )
  puts "cleanup_stuck_jobs: marked #{res.ntuples} stuck job(s) as Failed" if res.ntuples > 0
rescue => e
  puts "cleanup_stuck_jobs error: #{e.message}"
end

def init_last_backup_slot(conn)
  now  = Time.now
  slot = Time.new(now.year, now.month, now.day, now.hour, 0, 0)
  res  = conn.exec_params(
    "SELECT job_id FROM \"Jobs\" WHERE type IN ('Backup.Schedule', 'Backup.Adhoc') AND status IN ('Done', 'Running') AND created_date >= $1 LIMIT 1",
    [slot.utc]
  )
  if res.ntuples > 0
    $last_backup_slot = slot
    puts "init_last_backup_slot: backup already done this slot — skipping"
  end
rescue => e
  puts "init_last_backup_slot error: #{e.message}"
end

def republish_pending_notifications(conn, redis)
  since = (Time.now - 86_400).utc
  res   = conn.exec_params(
    "SELECT job_id, job_message, metadata FROM \"Jobs\" WHERE type IN ('Backup.Schedule', 'Backup.Adhoc') AND status IN ('Done', 'Failed') AND created_date >= $1",
    [since]
  )
  count = 0
  res.each do |row|
    job_id = row['job_id']
    next if notified?(redis, job_id)
    meta = JSON.parse(row['metadata'] || 'null') rescue nil
    next unless meta
    policy_stub = { 'Bucket' => meta['bucket'] || '', 'Path' => meta['path'] || '', 'ScheduleInterval' => '', 'ScheduleStartHour' => '' }
    success = row['job_message'].to_s.include?('complete')
    publish_backup_done(redis, job_id, policy_stub, success, meta['filename'] || '', success ? nil : row['job_message'])
    count += 1
  end
  puts "republish_pending_notifications: republished #{count} missed notification(s)" if count > 0
rescue => e
  puts "republish_pending_notifications error: #{e.message}"
end

# ── Backup ─────────────────────────────────────────────────────────────────────
def run_backup(policy, conn, redis, adhoc: false, job_id: nil)
  ts          = Time.now.strftime('%Y%m%d%H%M%S')
  prefix      = policy['FilePrefix']&.strip&.empty? ? 'please-payment' : policy['FilePrefix'].strip
  tag         = adhoc ? 'adhoc' : 'backup'
  dmp_file    = "#{prefix}-#{tag}-#{ts}.sql"
  dmp_file_gz = "#{dmp_file}.gz"
  local_gz    = "#{TMP_DIR}/#{dmp_file_gz}"
  remote_key  = [policy['Path']&.strip, dmp_file_gz].reject { |s| s.nil? || s.empty? }.join('/')
  label       = adhoc ? 'Adhoc Backup' : 'Backup'

  if job_id
    update_job_status(conn, job_id, 'Running')
  else
    job_id = create_job(conn, "#{label} #{ts}", "PostgreSQL backup via policy: #{policy['ScheduleInterval']}@#{policy['ScheduleStartHour']}h")
  end
  puts "Running backup job #{job_id}"

  start_time = Time.now
  append_log(conn, job_id, "Start time: #{start_time.strftime('%Y-%m-%d %H:%M:%S')}")

  begin
    append_log(conn, job_id, "[1/4] Copying #{SCRIPT_FILE} into pod #{PG_POD_NAME}...")
    rc = system("kubectl cp #{SCRIPT_FILE} -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/")
    raise "kubectl cp script failed (exit #{$?.exitstatus})" unless rc

    append_log(conn, job_id, "[2/4] Running pg_dump inside pod...")
    rc = system("kubectl exec -i -n #{PG_NAMESPACE} #{PG_POD_NAME} -- bash #{TMP_DIR}/#{SCRIPT_FILE} #{PG_USER} #{dmp_file} #{TMP_DIR} #{PG_PASSWORD}")
    raise "pg_dump exec failed (exit #{$?.exitstatus})" unless rc

    append_log(conn, job_id, "[3/4] Copying #{dmp_file_gz} out of pod to #{local_gz}...")
    rc = system("kubectl cp -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/#{dmp_file_gz} #{local_gz}")
    raise "kubectl cp backup file failed (exit #{$?.exitstatus})" unless rc

    file_size_bytes = File.size(local_gz) rescue 0
    file_size_mb    = (file_size_bytes / 1024.0 / 1024.0).round(2)

    append_log(conn, job_id, "[4/4] Uploading #{dmp_file_gz} (#{file_size_mb} MB) to #{policy['Bucket']}/#{remote_key}...")
    s3 = Aws::S3::Client.new(
      endpoint:          policy['StorageUrl'],
      access_key_id:     policy['StorageKey'],
      secret_access_key: policy['StorageSecret'],
      region:            'auto',
      force_path_style:  false,
      http_open_timeout: 10,
      http_read_timeout: 60,
    )
    Timeout.timeout(180) do
      File.open(local_gz, 'rb') { |f| s3.put_object(bucket: policy['Bucket'], key: remote_key, body: f) }
    end

    begin
      head = s3.head_object(bucket: policy['Bucket'], key: remote_key)
      remote_size_mb  = (head.content_length / 1024.0 / 1024.0).round(2)
      remote_modified = head.last_modified.strftime('%Y-%m-%d %H:%M:%S UTC') rescue head.last_modified.to_s
      append_log(conn, job_id, "[verify] s3://#{policy['Bucket']}/#{remote_key} — #{remote_size_mb} MB, last_modified: #{remote_modified}")
    rescue => ve
      append_log(conn, job_id, "[verify] WARNING: could not confirm file at destination — #{ve.message}")
    end

    end_time     = Time.now
    duration     = (end_time - start_time).round(1)
    mins         = (duration / 60).to_i
    secs         = (duration % 60).round(1)
    duration_str = mins > 0 ? "#{mins}m #{secs}s" : "#{secs}s"

    meta = { bucket: policy['Bucket'], path: policy['Path'] || '', filename: dmp_file_gz }.to_json
    conn.exec_params("UPDATE \"Jobs\" SET metadata = $1 WHERE job_id = $2", [meta, job_id]) rescue nil

    finish_job(conn, job_id, true, "Backup complete: #{policy['Bucket']}/#{remote_key}")
    publish_backup_done(redis, job_id, policy, true, dmp_file_gz, nil, {
      'FILE_SIZE'  => "#{file_size_mb} MB",
      'DURATION'   => duration_str,
      'START_TIME' => start_time.strftime('%Y-%m-%d %H:%M:%S'),
      'END_TIME'   => end_time.strftime('%Y-%m-%d %H:%M:%S'),
    })
    puts "Backup succeeded: #{remote_key} (#{file_size_mb} MB, #{duration_str})"
    append_log(conn, job_id, "End time: #{end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    append_log(conn, job_id, "Duration: #{duration_str}")
    append_log(conn, job_id, "File size: #{file_size_mb} MB (#{file_size_bytes} bytes)")
    append_log(conn, job_id, "Backup complete: #{remote_key}")

  rescue => e
    end_time = Time.now
    duration = (end_time - start_time).round(1)
    puts "Backup error: #{e.message}"
    finish_job(conn, job_id, false, "Backup failed: #{e.message}")
    publish_backup_done(redis, job_id, policy, false, dmp_file_gz, e.message)
    append_log(conn, job_id, "End time: #{end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    append_log(conn, job_id, "Duration: #{duration}s")
    append_log(conn, job_id, "ERROR: #{e.message}")
  ensure
    File.delete(local_gz) if File.exist?(local_gz) rescue nil
  end
end

# ── Restore ────────────────────────────────────────────────────────────────────
def run_restore(policy, conn, redis, job_id, filename, bucket, folder)
  local_gz   = "#{TMP_DIR}/#{filename}"
  remote_key = [folder&.strip, filename].reject { |s| s.nil? || s.empty? }.join('/')

  update_job_status(conn, job_id, 'Running')
  puts "Running restore job #{job_id}: #{filename}"

  start_time = Time.now
  append_log(conn, job_id, "Start time: #{start_time.strftime('%Y-%m-%d %H:%M:%S')}")

  begin
    s3 = Aws::S3::Client.new(
      endpoint:          policy['StorageUrl'],
      access_key_id:     policy['StorageKey'],
      secret_access_key: policy['StorageSecret'],
      region:            'auto',
      force_path_style:  false,
      http_open_timeout: 10,
      http_read_timeout: 60,
    )

    append_log(conn, job_id, "[1/4] Downloading s3://#{bucket}/#{remote_key} to #{local_gz}...")
    Timeout.timeout(300) do
      s3.get_object(bucket: bucket, key: remote_key, response_target: local_gz)
    end
    file_size_mb = (File.size(local_gz) / 1024.0 / 1024.0).round(2)
    append_log(conn, job_id, "[1/4] Downloaded #{file_size_mb} MB")

    append_log(conn, job_id, "[2/4] Copying restore script into pod #{PG_POD_NAME}...")
    rc = system("kubectl cp #{RESTORE_SCRIPT_FILE} -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/")
    raise "kubectl cp restore script failed (exit #{$?.exitstatus})" unless rc

    append_log(conn, job_id, "[3/4] Copying #{filename} into pod...")
    rc = system("kubectl cp #{local_gz} -n #{PG_NAMESPACE} #{PG_POD_NAME}:#{TMP_DIR}/#{filename}")
    raise "kubectl cp backup file failed (exit #{$?.exitstatus})" unless rc

    append_log(conn, job_id, "[4/4] Running pg_restore inside pod...")
    rc = system("kubectl exec -i -n #{PG_NAMESPACE} #{PG_POD_NAME} -- bash #{TMP_DIR}/#{RESTORE_SCRIPT_FILE} #{PG_USER} #{filename} #{TMP_DIR} #{PG_PASSWORD} #{PG_DB}")
    raise "pg_restore exec failed (exit #{$?.exitstatus})" unless rc

    # DB is now freshly restored — old conn is stale, original job records are gone.
    # Publish success via Redis immediately (does not use conn).
    end_time     = Time.now
    duration     = (end_time - start_time).round(1)
    mins         = (duration / 60).to_i
    secs         = (duration % 60).round(1)
    duration_str = mins > 0 ? "#{mins}m #{secs}s" : "#{secs}s"

    publish_restore_event(redis, 'Restore.Success', job_id, filename)
    puts "Restore succeeded: #{filename} (#{duration_str})"

    # Insert a restore record into the freshly restored DB so the UI can show it.
    begin
      fresh_conn     = connect_db_local
      restore_job_id = SecureRandom.uuid
      now = Time.now.utc
      fresh_conn.exec_params(
        "INSERT INTO \"Jobs\" (job_id, org_id, status, name, description, type, tags, progress_pct, succeed_cnt, failed_cnt, created_date, updated_date, end_date, job_message)
         VALUES ($1, 'global', 'Done', $2, $3, 'Backup.Restore', 'backup,restore', 100, 1, 0, $4, $4, $4, $5)",
        [restore_job_id, "Restore #{filename}", "Restored from #{bucket}/#{remote_key}", now, "Restore complete in #{duration_str}"]
      )
      fresh_conn.close rescue nil
      puts "Inserted restore success record #{restore_job_id} into restored DB"
    rescue => ie
      puts "Could not insert restore success record: #{ie.message}"
    end

  rescue => e
    puts "Restore error: #{e.message}"
    begin
      finish_job(conn, job_id, false, "Restore failed: #{e.message}")
      append_log(conn, job_id, "ERROR: #{e.message}")
      publish_restore_event(redis, 'Restore.Failed', job_id, filename, e.message)
    rescue => inner
      puts "Could not update job after restore failure: #{inner.message}"
    end
  ensure
    File.delete(local_gz) if File.exist?(local_gz) rescue nil
  end
end

# ── Threads ────────────────────────────────────────────────────────────────────
def start_adhoc_thread(redis)
  group_name    = 'k8s-job-db-backup'
  consumer_name = 'job-db-backup-adhoc'
  stream        = "JobSubmitted:#{ENVIRONMENT}:Backup.Adhoc"

  begin
    redis.xgroup(:create, stream, group_name, '$', mkstream: true)
  rescue Redis::CommandError => e
    raise e unless e.message.include?('BUSYGROUP')
  end

  Thread.new do
    # Each thread needs its own Redis connection — blocking xreadgroup is not thread-safe on a shared connection
    redis = Redis.new(host: REDIS_HOST, port: REDIS_PORT, read_timeout: 10, reconnect_attempts: 2)
    puts "Adhoc backup listener started on #{stream}"

    begin
      pending = redis.xreadgroup(group_name, consumer_name, stream, '0', count: 10) rescue nil
      if pending
        pending.each do |_s, msgs|
          msgs.each do |msg_id, fields|
            conn = nil; adhoc_job_id = nil
            begin
              data = JSON.parse(fields['message']) rescue {}
              adhoc_job_id = data['Id'] || data['id']
              conn   = connect_db_local
              policy = get_backup_policy(conn)
              if policy && policy['IsEnabled']
                run_backup(policy, conn, redis, adhoc: true, job_id: adhoc_job_id)
              else
                update_job_status(conn, adhoc_job_id, 'Failed') if adhoc_job_id
              end
            rescue => e
              puts "Adhoc PEL drain error [#{msg_id}]: #{e.message}"
              begin; update_job_status(conn, adhoc_job_id, 'Failed') if conn && adhoc_job_id; rescue nil; end
            ensure
              conn&.close rescue nil
              redis.xack(stream, group_name, msg_id) rescue nil
            end
          end
        end
      end
    rescue => e
      puts "Adhoc PEL startup drain failed: #{e.message}"
    end

    loop do
      begin
        entries = redis.xreadgroup(group_name, consumer_name, stream, '>', count: 1, block: 5000)
        next unless entries

        entries.each do |_stream, messages|
          messages.each do |msg_id, fields|
            puts "Adhoc backup triggered from Redis at #{Time.now}"
            conn = nil; adhoc_job_id = nil
            begin
              data = JSON.parse(fields['message']) rescue {}
              adhoc_job_id = data['Id'] || data['id']
              conn   = connect_db_local
              policy = get_backup_policy(conn)
              if policy && policy['IsEnabled']
                run_backup(policy, conn, redis, adhoc: true, job_id: adhoc_job_id)
              else
                puts 'Backup policy not enabled — skipping adhoc'
                update_job_status(conn, adhoc_job_id, 'Failed') if adhoc_job_id
              end
            rescue => e
              puts "Adhoc backup message error: #{e.message}"
              begin; update_job_status(conn, adhoc_job_id, 'Failed') if conn && adhoc_job_id; rescue nil; end
            ensure
              conn&.close rescue nil
              redis.xack(stream, group_name, msg_id)
            end
          end
        end
      rescue => e
        puts "Adhoc thread error: #{e.message}"
        sleep 5
      end
    end
  end
end

def start_restore_thread(redis)
  group_name    = 'k8s-job-db-backup'
  consumer_name = 'job-db-backup-restore'
  stream        = "JobSubmitted:#{ENVIRONMENT}:Backup.Restore"

  begin
    redis.xgroup(:create, stream, group_name, '$', mkstream: true)
  rescue Redis::CommandError => e
    raise e unless e.message.include?('BUSYGROUP')
  end

  Thread.new do
    # Each thread needs its own Redis connection — blocking xreadgroup is not thread-safe on a shared connection
    redis = Redis.new(host: REDIS_HOST, port: REDIS_PORT, read_timeout: 10, reconnect_attempts: 2)
    puts "Restore listener started on #{stream}"

    begin
      pending = redis.xreadgroup(group_name, consumer_name, stream, '0', count: 10) rescue nil
      if pending
        pending.each do |_s, msgs|
          msgs.each do |msg_id, fields|
            conn = nil; job_id = nil
            begin
              data     = JSON.parse(fields['message']) rescue {}
              job_id   = data['Id'] || data['id']
              config   = JSON.parse(data['Configuration'] || data['configuration'] || '[]') rescue []
              get      = ->(name) { p = config.find { |p| p['Name'] == name || p['name'] == name }; p ? (p['Value'] || p['value']) : nil }
              filename = get.call('FILENAME')
              bucket   = get.call('BUCKET')
              folder   = get.call('FOLDER')
              conn   = connect_db_local
              policy = get_backup_policy(conn)
              if policy
                run_restore(policy, conn, redis, job_id, filename, bucket, folder)
              else
                update_job_status(conn, job_id, 'Failed') if job_id
              end
            rescue => e
              puts "Restore PEL drain error [#{msg_id}]: #{e.message}"
              begin; update_job_status(conn, job_id, 'Failed') if conn && job_id; rescue nil; end
            ensure
              conn&.close rescue nil
              redis.xack(stream, group_name, msg_id) rescue nil
            end
          end
        end
      end
    rescue => e
      puts "Restore PEL startup drain failed: #{e.message}"
    end

    loop do
      begin
        entries = redis.xreadgroup(group_name, consumer_name, stream, '>', count: 1, block: 5000)
        next unless entries

        entries.each do |_stream, messages|
          messages.each do |msg_id, fields|
            puts "Restore triggered at #{Time.now}"
            conn = nil; job_id = nil
            begin
              data     = JSON.parse(fields['message']) rescue {}
              job_id   = data['Id'] || data['id']
              config   = JSON.parse(data['Configuration'] || data['configuration'] || '[]') rescue []
              get      = ->(name) { p = config.find { |p| p['Name'] == name || p['name'] == name }; p ? (p['Value'] || p['value']) : nil }
              filename = get.call('FILENAME')
              bucket   = get.call('BUCKET')
              folder   = get.call('FOLDER')
              conn   = connect_db_local
              policy = get_backup_policy(conn)
              if policy
                run_restore(policy, conn, redis, job_id, filename, bucket, folder)
              else
                update_job_status(conn, job_id, 'Failed') if job_id
                puts 'Backup policy not found — cannot restore'
              end
            rescue => e
              puts "Restore message error: #{e.message}"
              begin; update_job_status(conn, job_id, 'Failed') if conn && job_id; rescue nil; end
            ensure
              conn&.close rescue nil
              redis.xack(stream, group_name, msg_id)
            end
          end
        end
      rescue => e
        puts "Restore thread error: #{e.message}"
        sleep 5
      end
    end
  end
end

# ── Main loop ──────────────────────────────────────────────────────────────────
$stdout.sync = true
puts "job-db-backup starting (env=#{ENVIRONMENT}, pod=#{PG_POD_NAME})"

redis = Redis.new(host: REDIS_HOST, port: REDIS_PORT, read_timeout: 10, reconnect_attempts: 2)

begin
  _init_conn = connect_db_local
  cleanup_stuck_jobs(_init_conn)
  init_last_backup_slot(_init_conn)
  republish_pending_notifications(_init_conn, redis)
  _init_conn.close
rescue => e
  puts "Startup init failed: #{e.message}"
end

start_adhoc_thread(redis)
start_restore_thread(redis)

loop_count = 0
loop do
  begin
    conn   = connect_db_local
    policy = get_backup_policy(conn)

    if should_backup?(policy)
      $last_backup_slot = current_slot
      puts "Backup triggered at #{Time.now}"
      run_backup(policy, conn, redis)
    else
      puts "#{Time.now.strftime('%H:%M')} — no backup needed"
    end

    loop_count += 1
    republish_pending_notifications(conn, redis) if (loop_count % 6).zero?

    conn.close
  rescue => e
    puts "Loop error: #{e.message}"
  end

  sleep 600
end
