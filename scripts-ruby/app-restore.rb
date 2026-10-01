#!/usr/bin/env ruby
# Generic restore script — counterpart to app-backup.rb.
#
# Downloads a backup zip from S3-compatible storage (matching FILE_PREFIX,
# optionally filtered to one date), unpacks it, restores the database, and
# puts the app's files back in place inside the pod.
#
# Which backup file gets restored (pick ONE):
#   - RESTORE_FILE: the exact S3 key/filename to restore — skips the
#     "pick newest matching" logic entirely. Useful for testing a specific
#     backup or a precise disaster-recovery restore.
#   - RESTORE_DATE (or the first CLI arg): a date substring (e.g.
#     "20260928") to narrow down which backups count as candidates — the
#     newest match is used.
#   - neither set: the single newest file under FILE_PREFIX is used.

require 'aws-sdk-s3'
require 'time'
require 'timeout'
require 'fileutils'
require 'open3'

require './utils'

DB_TYPE        = ENV['DB_TYPE']        || 'postgresql'
DB_NAMESPACE   = ENV['DB_NAMESPACE']   || 'default'
DB_POD_NAME    = ENV['DB_POD_NAME']    || ''
DB_POD_KEYWORD = ENV['DB_POD_KEYWORD'] || ''
DB_POD_LABEL   = ENV['DB_POD_LABEL']   || ''

APP_NAMESPACE   = ENV['APP_NAMESPACE']   || DB_NAMESPACE
APP_POD_NAME    = ENV['APP_POD_NAME']    || ''
APP_POD_KEYWORD = ENV['APP_POD_KEYWORD'] || ''
APP_POD_LABEL   = ENV['APP_POD_LABEL']   || ''
APP_DATA_PATH   = ENV['APP_DATA_PATH']   || ''

S3_STORAGE_URL = ENV['S3_STORAGE_URL']
S3_KEY         = ENV['S3_KEY']
S3_SECRET      = ENV['S3_SECRET']
S3_BUCKET      = ENV['S3_BUCKET']
S3_BUCKET_PATH = ENV['S3_BUCKET_PATH'] || ''
FILE_PREFIX    = ENV['FILE_PREFIX']    || 'app'
S3_TRANSFER_TIMEOUT_SEC = (ENV['S3_TRANSFER_TIMEOUT_SEC'] || '1800').to_i

DISCORD_WEBHOOK = ENV['DISCORD_WEBHOOK']
RESTORE_FILE         = ENV['RESTORE_FILE']
RESTORE_DATE         = ARGV[0] || ENV['RESTORE_DATE']
RESTART_APP_POD      = ENV['RESTART_APP_POD'] == 'true'

TMP_DIR            = '/tmp'
DB_RESTORE_SCRIPT  = 'db-restore-bitnami.bash'

# Human-readable "which app/site is this" label for Discord — S3_BUCKET_PATH
# is the per-site bucket folder (e.g. "nap", "snipeit"), the clearest single
# identifier we have.
SITE_LABEL = S3_BUCKET_PATH.strip.empty? ? FILE_PREFIX : S3_BUCKET_PATH.strip

def fail!(message)
  puts "ERROR: #{message}"
  send_discord_notify(
    DISCORD_WEBHOOK,
    'Restore Done',
    {
      'สถานะ'    => '❌ Failed',
      'App'       => SITE_LABEL,
      'Namespace' => APP_NAMESPACE,
      'Error'     => message,
      'Prefix'    => FILE_PREFIX,
    },
    15158332,
    footer: "onix-v2-jobs · app-restore"
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

fail! 'APP_DATA_PATH is required' if APP_DATA_PATH.strip.empty?
fail! 'S3_BUCKET is required' if S3_BUCKET.to_s.strip.empty?

start_time = Time.now
puts "=== app-restore starting (app=#{SITE_LABEL}, prefix=#{FILE_PREFIX}, file=#{RESTORE_FILE || 'auto'}, date=#{RESTORE_DATE || 'latest'}) ==="

s3 = Aws::S3::Client.new(
  endpoint:          S3_STORAGE_URL,
  access_key_id:     S3_KEY,
  secret_access_key: S3_SECRET,
  region:            'auto',
  force_path_style:  false,
  http_open_timeout: 10,
  http_read_timeout: 60,
)

# [1] Find the backup file to restore
if RESTORE_FILE && !RESTORE_FILE.strip.empty?
  remote_key = RESTORE_FILE.strip.include?('/') ? RESTORE_FILE.strip : [S3_BUCKET_PATH.strip, RESTORE_FILE.strip].reject { |s| s.nil? || s.empty? }.join('/')
  puts "[1/7] Using explicit RESTORE_FILE: #{remote_key}"
  begin
    head = s3.head_object(bucket: S3_BUCKET, key: remote_key)
    puts "[1/7] Confirmed #{remote_key} exists (last modified #{head.last_modified})"
  rescue => e
    fail! "RESTORE_FILE [#{remote_key}] not found in S3: #{e.message}"
  end
else
  list_prefix = [S3_BUCKET_PATH.strip, FILE_PREFIX].reject { |s| s.nil? || s.empty? }.join('/')
  puts "[1/7] Listing s3://#{S3_BUCKET}/#{list_prefix}..."

  objects = []
  begin
    s3.list_objects_v2(bucket: S3_BUCKET, prefix: list_prefix).each_page { |page| objects.concat(page.contents) }
  rescue => e
    fail! "Listing S3 objects failed: #{e.message}"
  end
  fail! "No backup files found under #{list_prefix}" if objects.empty?

  if RESTORE_DATE && !RESTORE_DATE.strip.empty?
    matched = objects.select { |o| o.key.include?(RESTORE_DATE) }
    fail! "No backup files found for date [#{RESTORE_DATE}]" if matched.empty?
    objects = matched
  end

  target     = objects.max_by(&:last_modified)
  remote_key = target.key
  puts "[1/7] Selected #{remote_key} (last modified #{target.last_modified})"
end

final_zip = File.basename(remote_key)
local_zip = "#{TMP_DIR}/#{final_zip}"

# [2] Download and unpack
puts "[2/7] Downloading..."
begin
  Timeout.timeout(S3_TRANSFER_TIMEOUT_SEC) { s3.get_object(bucket: S3_BUCKET, key: remote_key, response_target: local_zip) }
rescue => e
  fail! "Download failed: #{e.message}"
end

puts "[3/7] Unpacking zip..."
ok, out = run("cd #{TMP_DIR} && unzip -o #{final_zip}")
fail!("unzip failed: #{tail(out)}") unless ok

db_dump_file_gz = Dir.glob("#{TMP_DIR}/db-*.sql.gz").max_by { |f| File.mtime(f) }
app_files_tar   = Dir.glob("#{TMP_DIR}/files-*.tar.gz").max_by { |f| File.mtime(f) }
fail! 'Could not find db-*.sql.gz inside the backup' if db_dump_file_gz.nil?
fail! 'Could not find files-*.tar.gz inside the backup' if app_files_tar.nil?
db_dump_basename   = File.basename(db_dump_file_gz)
app_files_basename = File.basename(app_files_tar)

app_pod_name = resolve_app_pod_name
db_pod_name  = resolve_db_pod_name
puts "DB pod: #{db_pod_name} (ns=#{DB_NAMESPACE}) | App pod: #{app_pod_name} (ns=#{APP_NAMESPACE})"

# [3] Restore the DB
puts '[4/7] Copying restore script + dump into DB pod...'
ok, out = run("kubectl cp #{DB_RESTORE_SCRIPT} -n #{DB_NAMESPACE} #{db_pod_name}:#{TMP_DIR}/")
fail!("kubectl cp restore script failed: #{tail(out)}") unless ok
ok, out = run("kubectl cp #{db_dump_file_gz} -n #{DB_NAMESPACE} #{db_pod_name}:#{TMP_DIR}/#{db_dump_basename}")
fail!("kubectl cp DB dump into pod failed: #{tail(out)}") unless ok

puts "[5/7] Running #{DB_TYPE} restore inside DB pod..."
ok, out = run("kubectl exec -i -n #{DB_NAMESPACE} #{db_pod_name} -- bash #{TMP_DIR}/#{DB_RESTORE_SCRIPT} #{DB_TYPE} #{db_dump_basename} #{TMP_DIR}")
fail!("DB restore failed: #{tail(out)}") unless ok

# [4] Restore the app's files
puts '[6/7] Copying files archive into app pod and extracting...'
ok, out = run("kubectl cp #{app_files_tar} -n #{APP_NAMESPACE} #{app_pod_name}:#{TMP_DIR}/#{app_files_basename}")
fail!("kubectl cp files archive into pod failed: #{tail(out)}") unless ok
# Extract into a fresh temp dir (tar creates it, so no permission conflict),
# then copy the contents over with cp -rf:
#   -f  some files (e.g. wp-config.php) are intentionally left read-only by
#       Bitnami even to their own owner — force deletes+recreates them
#       instead of failing to open them for writing
#   (no -p/-a) avoids cp also trying to preserve/touch the pre-existing
#       target directory's own timestamps, which fails the same way tar's
#       did ("Operation not permitted") since the exec user doesn't own
#       that mount point
extract_dir = "#{TMP_DIR}/restore-extract-#{Time.now.to_i}"
extract_cmd = "mkdir -p #{extract_dir} && tar -xzf #{TMP_DIR}/#{app_files_basename} -C #{extract_dir} && cp -rf #{extract_dir}/. #{APP_DATA_PATH}/ && rm -rf #{extract_dir}"
ok, out = run("kubectl exec -i -n #{APP_NAMESPACE} #{app_pod_name} -- bash -c \"#{extract_cmd}\"")
fail!("Extracting app files failed: #{tail(out)}") unless ok

if RESTART_APP_POD
  puts "[7/7] Restarting app pod #{app_pod_name}..."
  system("kubectl delete pod -n #{APP_NAMESPACE} #{app_pod_name}")
else
  puts '[7/7] Skipping app pod restart (set RESTART_APP_POD=true to enable)'
end

duration     = (Time.now - start_time).round(1)
mins         = (duration / 60).to_i
secs         = (duration % 60).round(1)
duration_str = mins > 0 ? "#{mins}m #{secs}s" : "#{secs}s"

puts "Restore complete: #{remote_key} (#{duration_str})"

send_discord_notify(
  DISCORD_WEBHOOK,
  'Restore Done',
  {
    'สถานะ'    => '✅ Success',
    'App'       => SITE_LABEL,
    'Namespace' => APP_NAMESPACE,
    'File'      => final_zip,
    'Bucket'    => S3_BUCKET,
    'Path'      => remote_key,
    'Duration'  => duration_str,
  },
  5763719,
  footer: "onix-v2-jobs · app-restore"
)

[local_zip, db_dump_file_gz, app_files_tar].each { |f| File.delete(f) rescue nil }
