#!/usr/bin/env ruby

require 'time'

if File.exist?('env.rb')
  require './env'
end

ENVIRONMENT  = ENV['ENVIRONMENT']  || 'Development'
PG_NAMESPACE = ENV['PG_NAMESPACE'] || 'default'
PG_OLD_POD   = ENV['PG_OLD_POD']  || 'postgresql-onix-0'
PG_NEW_POD   = ENV['PG_NEW_POD']  || 'postgresql-onix-v2-0'
PG_USER      = ENV['PG_USER']     || 'postgres'
PG_PASSWORD  = ENV['PG_PASSWORD'] || ''
PG_DB        = ENV['PG_DB']       || 'onix'
TMP_DIR      = '/tmp'

DUMP_SCRIPT    = 'pg-dump-db.bash'
RESTORE_SCRIPT = 'pg-restore-db.bash'

$stdout.sync = true

def run!(cmd, desc)
  puts ">>> #{desc}"
  result = system(cmd)
  raise "#{desc} failed (exit #{$?.exitstatus})" unless result
end

puts "=== job-db-sync starting: #{PG_OLD_POD} -> #{PG_NEW_POD} ==="
puts "=== ENVIRONMENT=[#{ENVIRONMENT}] NAMESPACE=[#{PG_NAMESPACE}] DB=[#{PG_DB}] ==="

ts          = Time.now.strftime('%Y%m%d%H%M%S')
dmp_file    = "db-sync-#{ts}.sql"
dmp_file_gz = "#{dmp_file}.gz"
local_gz    = "#{TMP_DIR}/#{dmp_file_gz}"
start_time  = Time.now
file_size_mb = 0

begin
  run!(
    "kubectl cp #{DUMP_SCRIPT} -n #{PG_NAMESPACE} #{PG_OLD_POD}:#{TMP_DIR}/",
    "[1/6] Copy dump script into #{PG_OLD_POD}"
  )

  run!(
    "kubectl exec -i -n #{PG_NAMESPACE} #{PG_OLD_POD} -- bash #{TMP_DIR}/#{DUMP_SCRIPT} #{PG_USER} #{dmp_file} #{TMP_DIR} #{PG_PASSWORD}",
    "[2/6] Run pg_dump inside #{PG_OLD_POD}"
  )

  run!(
    "kubectl cp -n #{PG_NAMESPACE} #{PG_OLD_POD}:#{TMP_DIR}/#{dmp_file_gz} #{local_gz}",
    "[3/6] Copy dump file to local"
  )

  file_size_mb = (File.size(local_gz) / 1024.0 / 1024.0).round(2)
  puts ">>> Dump file: #{local_gz} (#{file_size_mb} MB)"

  run!(
    "kubectl cp #{RESTORE_SCRIPT} -n #{PG_NAMESPACE} #{PG_NEW_POD}:#{TMP_DIR}/",
    "[4/6] Copy restore script into #{PG_NEW_POD}"
  )

  run!(
    "kubectl cp #{local_gz} -n #{PG_NAMESPACE} #{PG_NEW_POD}:#{TMP_DIR}/#{dmp_file_gz}",
    "[5/6] Copy dump file into #{PG_NEW_POD}"
  )

  run!(
    "kubectl exec -i -n #{PG_NAMESPACE} #{PG_NEW_POD} -- bash #{TMP_DIR}/#{RESTORE_SCRIPT} #{PG_USER} #{dmp_file_gz} #{TMP_DIR} #{PG_PASSWORD} #{PG_DB}",
    "[6/6] Run pg_restore inside #{PG_NEW_POD}"
  )

  duration = (Time.now - start_time).round(1)
  puts "=== DB sync complete: #{file_size_mb} MB in #{duration}s ==="

rescue => e
  duration = (Time.now - start_time).round(1)
  puts "=== DB sync FAILED after #{duration}s: #{e.message} ==="
  exit 1

ensure
  File.delete(local_gz) if File.exist?(local_gz) rescue nil
end
