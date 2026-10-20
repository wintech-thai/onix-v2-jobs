#!/usr/bin/env ruby
# x079: WordPress backup/restore
# Generic backup script for "a database pod + (optionally) an app pod's data directory".
# Built for WordPress first, but kept generic (see env vars below) so it can
# be reused for other apps later (e.g. SnipeIt) just by changing config.
#
# x088: if APP_DATA_PATH is left blank, the app-pod archive step is skipped
# entirely and only the DB dump gets zipped/uploaded — added for Please Scan,
# which has no app pod to archive, only a PostgreSQL pod to dump.
#
# Meant to run as a one-shot Kubernetes CronJob — not a long-running service.
#
# What it does:
#   1. kubectl exec into the DB pod and dump the database (PostgreSQL or MySQL)
#   2. (skipped when APP_DATA_PATH is blank) kubectl exec into the app pod and tar up its data directory
#   3. Pack the dump (+ app files, if any) into one timestamped zip
#   4. Upload the zip to S3-compatible storage
#   5. Notify a Discord webhook

require 'aws-sdk-s3'
require 'time'
require 'timeout'
require 'open3'

require './utils'

# ── Config (env vars) ────────────────────────────────────────────────────────
DB_TYPE      = ENV['DB_TYPE']      || 'postgresql' # postgresql | mysql
DB_NAMESPACE = ENV['DB_NAMESPACE'] || 'default'
DB_POD_NAME    = ENV['DB_POD_NAME']    || ''       # set this directly if stable (e.g. a StatefulSet pod), OR...
DB_POD_KEYWORD = ENV['DB_POD_KEYWORD'] || ''       # ...set this to find the pod by name substring, OR...
DB_POD_LABEL   = ENV['DB_POD_LABEL']   || ''       # ...set this to a label selector (e.g. "app.kubernetes.io/name=mysql") when a keyword would be ambiguous

APP_NAMESPACE   = ENV['APP_NAMESPACE']   || DB_NAMESPACE
APP_POD_NAME    = ENV['APP_POD_NAME']    || ''     # set this directly, OR...
APP_POD_KEYWORD = ENV['APP_POD_KEYWORD'] || ''     # ...set this to find the pod by name substring (it changes on restart), OR...
APP_POD_LABEL   = ENV['APP_POD_LABEL']   || ''     # ...set this to a label selector when a keyword would be ambiguous
APP_DATA_PATH   = ENV['APP_DATA_PATH']   || ''

S3_STORAGE_URL = ENV['S3_STORAGE_URL']
S3_KEY         = ENV['S3_KEY']
S3_SECRET      = ENV['S3_SECRET']
S3_BUCKET      = ENV['S3_BUCKET']
S3_BUCKET_PATH = ENV['S3_BUCKET_PATH'] || ''
FILE_PREFIX    = ENV['FILE_PREFIX']    || 'app'
S3_TRANSFER_TIMEOUT_SEC = (ENV['S3_TRANSFER_TIMEOUT_SEC'] || '1800').to_i

DISCORD_WEBHOOK = ENV['DISCORD_WEBHOOK']

TMP_DIR        = '/tmp'
DB_DUMP_SCRIPT = 'db-dump-bitnami.bash'

# Human-readable "which app/site is this" label for Discord — S3_BUCKET_PATH
# is the per-site bucket folder (e.g. "nap", "snipeit"), the clearest single
# identifier we have.
SITE_LABEL = S3_BUCKET_PATH.strip.empty? ? FILE_PREFIX : S3_BUCKET_PATH.strip

def fail!(message)
  puts "ERROR: #{message}"
  send_discord_notify(
    DISCORD_WEBHOOK,
    'Backup Done',
    {
      'สถานะ'    => '❌ Failed',
      'App'       => SITE_LABEL,
      'Namespace' => APP_NAMESPACE,
      'Error'     => message,
      'Prefix'    => FILE_PREFIX,
    },
    15158332,
    footer: "onix-v2-jobs · app-backup"
  )
  exit 1
end

# Runs a shell command, returning [success, combined stdout+stderr] — unlike
# plain system(), this lets fail! include the actual underlying error text
# (e.g. the real kubectl/tar error) instead of just an exit code.
def run(cmd)
  output, status = Open3.capture2e(cmd)
  [status.success?, output]
end

def tail(output, limit = 600)
  output = output.to_s.strip
  output.length > limit ? "...#{output[-limit..]}" : output
end

# Best-effort remote cleanup — never fails the job, only logs a warning.
# Scoped to exactly the filenames this run created (passed in by the caller),
# so it never touches another run's or another process's files sitting in
# the same pod's /tmp.
def cleanup_remote_tmp(namespace, pod_name, files)
  return if files.empty?
  paths = files.map { |f| "#{TMP_DIR}/#{f}" }.join(' ')
  ok, out = run("kubectl exec -i -n #{namespace} #{pod_name} -- rm -f #{paths}")
  puts "WARNING: failed to clean up #{paths} on pod #{pod_name}: #{tail(out)}" unless ok
end

def resolve_app_pod_name
  return APP_POD_NAME unless APP_POD_NAME.strip.empty?
  unless APP_POD_LABEL.strip.empty?
    pod = find_pod_by_label(APP_NAMESPACE, APP_POD_LABEL)
    fail! "Could not find a pod matching label [#{APP_POD_LABEL}] in namespace [#{APP_NAMESPACE}]" if pod.nil?
    return pod
  end
  fail! 'Either APP_POD_NAME, APP_POD_LABEL, or APP_POD_KEYWORD must be set' if APP_POD_KEYWORD.strip.empty?
  pod = find_pod_by_keyword(APP_NAMESPACE, APP_POD_KEYWORD)
  fail! "Could not find a pod matching keyword [#{APP_POD_KEYWORD}] in namespace [#{APP_NAMESPACE}]" if pod.nil?
  pod
end

def resolve_db_pod_name
  return DB_POD_NAME unless DB_POD_NAME.strip.empty?
  unless DB_POD_LABEL.strip.empty?
    pod = find_pod_by_label(DB_NAMESPACE, DB_POD_LABEL)
    fail! "Could not find a pod matching label [#{DB_POD_LABEL}] in namespace [#{DB_NAMESPACE}]" if pod.nil?
    return pod
  end
  fail! 'Either DB_POD_NAME, DB_POD_LABEL, or DB_POD_KEYWORD must be set' if DB_POD_KEYWORD.strip.empty?
  pod = find_pod_by_keyword(DB_NAMESPACE, DB_POD_KEYWORD)
  fail! "Could not find a pod matching keyword [#{DB_POD_KEYWORD}] in namespace [#{DB_NAMESPACE}]" if pod.nil?
  pod
end

$stdout.sync = true

# x088: no APP_DATA_PATH configured → DB-only backup, skip the app-pod archive entirely
DB_ONLY = APP_DATA_PATH.strip.empty?

fail! 'S3_BUCKET is required' if S3_BUCKET.to_s.strip.empty?

ts              = Time.now.strftime('%Y%m%d%H%M%S')
db_dump_file    = "db-#{ts}.sql"
db_dump_file_gz = "#{db_dump_file}.gz"
app_files_tar   = "files-#{ts}.tar.gz"
final_zip       = "#{FILE_PREFIX}-#{ts}.zip"
local_zip       = "#{TMP_DIR}/#{final_zip}"
start_time      = Time.now

app_pod_name = DB_ONLY ? nil : resolve_app_pod_name
db_pod_name  = resolve_db_pod_name
puts "=== app-backup starting (app=#{SITE_LABEL}, prefix=#{FILE_PREFIX}, ts=#{ts}, mode=#{DB_ONLY ? 'db-only' : 'db+app'}) ==="
puts "DB pod: #{db_pod_name} (ns=#{DB_NAMESPACE})#{DB_ONLY ? '' : " | App pod: #{app_pod_name} (ns=#{APP_NAMESPACE})"}"

# [1] Dump the database inside the DB pod
puts "[1/6] Copying #{DB_DUMP_SCRIPT} into DB pod..."
ok, out = run("kubectl cp #{DB_DUMP_SCRIPT} -n #{DB_NAMESPACE} #{db_pod_name}:#{TMP_DIR}/")
fail!("kubectl cp dump script failed: #{tail(out)}") unless ok

puts "[2/6] Running #{DB_TYPE} dump inside DB pod..."
ok, out = run("kubectl exec -i -n #{DB_NAMESPACE} #{db_pod_name} -- bash #{TMP_DIR}/#{DB_DUMP_SCRIPT} #{DB_TYPE} #{db_dump_file} #{TMP_DIR}")
fail!("DB dump failed: #{tail(out)}") unless ok

puts "[3/6] Copying #{db_dump_file_gz} out of DB pod..."
ok, out = run("kubectl cp -n #{DB_NAMESPACE} #{db_pod_name}:#{TMP_DIR}/#{db_dump_file_gz} #{TMP_DIR}/#{db_dump_file_gz}")
fail!("kubectl cp DB dump out failed: #{tail(out)}") unless ok

# Clean up this run's own files from the DB pod's /tmp now that we have a local copy
cleanup_remote_tmp(DB_NAMESPACE, db_pod_name, [DB_DUMP_SCRIPT, db_dump_file_gz])

# [2] Tar the app's data directory inside the app pod, then copy it out (skipped in DB-only mode)
unless DB_ONLY
  puts "[4/6] Archiving #{APP_DATA_PATH} inside app pod..."
  ok, out = run("kubectl exec -i -n #{APP_NAMESPACE} #{app_pod_name} -- bash -c \"cd #{APP_DATA_PATH} && tar -czf #{TMP_DIR}/#{app_files_tar} .\"")
  fail!("Archiving app files failed: #{tail(out)}") unless ok

  puts "[5/6] Copying #{app_files_tar} out of app pod..."
  ok, out = run("kubectl cp -n #{APP_NAMESPACE} #{app_pod_name}:#{TMP_DIR}/#{app_files_tar} #{TMP_DIR}/#{app_files_tar}")
  fail!("kubectl cp app files out failed: #{tail(out)}") unless ok

  # Clean up this run's own file from the app pod's /tmp now that we have a local copy
  cleanup_remote_tmp(APP_NAMESPACE, app_pod_name, [app_files_tar])
end

# [3] Pack the dump (+ app files, if any) into a single zip
files_to_zip = DB_ONLY ? [db_dump_file_gz] : [db_dump_file_gz, app_files_tar]
puts "[6/6] Packing #{files_to_zip.join(' + ')} into #{final_zip}..."
ok, out = run("cd #{TMP_DIR} && zip -j #{final_zip} #{files_to_zip.join(' ')}")
fail!("zip failed: #{tail(out)}") unless ok

file_size_mb = (File.size(local_zip) / 1024.0 / 1024.0).round(2)

# [4] Upload to S3-compatible storage
remote_key = [S3_BUCKET_PATH.strip, final_zip].reject { |s| s.nil? || s.empty? }.join('/')
puts "Uploading #{final_zip} (#{file_size_mb} MB) to #{S3_BUCKET}/#{remote_key}..."

s3 = Aws::S3::Client.new(
  endpoint:          S3_STORAGE_URL,
  access_key_id:     S3_KEY,
  secret_access_key: S3_SECRET,
  region:            'auto',
  force_path_style:  false,
  http_open_timeout: 10,
  http_read_timeout: 60,
)

begin
  Timeout.timeout(S3_TRANSFER_TIMEOUT_SEC) do
    File.open(local_zip, 'rb') { |f| s3.put_object(bucket: S3_BUCKET, key: remote_key, body: f) }
  end
rescue => e
  fail! "S3 upload failed: #{e.message}"
end

duration     = (Time.now - start_time).round(1)
mins         = (duration / 60).to_i
secs         = (duration % 60).round(1)
duration_str = mins > 0 ? "#{mins}m #{secs}s" : "#{secs}s"

puts "Backup complete: #{S3_BUCKET}/#{remote_key} (#{file_size_mb} MB, #{duration_str})"

# [5] Notify Discord
send_discord_notify(
  DISCORD_WEBHOOK,
  'Backup Done',
  {
    'สถานะ'    => '✅ Success',
    'App'       => SITE_LABEL,
    'Namespace' => APP_NAMESPACE,
    'File'      => final_zip,
    'Bucket'    => S3_BUCKET,
    'Path'      => remote_key,
    'Size'      => "#{file_size_mb} MB",
    'Duration'  => duration_str,
  },
  5763719,
  footer: "onix-v2-jobs · app-backup"
)

[db_dump_file_gz, app_files_tar, final_zip].each { |f| File.delete("#{TMP_DIR}/#{f}") rescue nil }
