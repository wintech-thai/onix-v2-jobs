#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require './utils'

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

def submit_job(stream, data, conn)
  jobId = data['Id']

  puts("INFO : ### Submitted job [#{jobId}] from stream [#{stream}]")
  update_job_status(conn, jobId, 'Submitted')
end

def start_job(data)
  tempDir = fallback(ENV['TEMP_DIR'], '')
  jobId = data['Id']
  jobType = data['Type']
  params = data['Parameters']
  environment = ENV['ENVIRONMENT'].downcase

  t = Time.now
  mmss = t.strftime("%H%M")

  jobMap = {
    'ScanItemGenerator' => ['scan-item-generator', 'cron-script'],
    'OtpEmailSend' => ['simple-email-send', 'cron-script']
  }

  cronName, containerName = jobMap[jobType]
  jobName = "#{cronName}-#{mmss}"

  scriptFile = "#{tempDir}/#{jobName}.bash"

  env_entries = params.map do |v|
    %Q{{"name":"#{v['Name']}", "value":"#{v['Value']}"}}
  end.join(",\n    ")  # comma + newline + indent

  #    {"name":"SCAN_ITEM_COUNT","value":"15"}

  cmd = <<-SHELL
  #!/bin/bash

  kubectl create job --from=cronjob/#{cronName} #{jobName} -n onix-v2-#{environment} --dry-run=client -o yaml | \
  kubectl patch --local -p '{"spec":{"template":{"spec":{"containers":[{"name":"#{containerName}","env":[
    {"name":"JOB_ID","value":"#{jobId}"},
    #{env_entries}
  ]}]}}}}' --type=strategic -f - -o yaml | \
  kubectl apply -f -
SHELL

  File.write(scriptFile, cmd)
  cmdOutput = %x{ bash "#{scriptFile}" }
  File.delete(scriptFile)

  puts("INFO : ### Created job [#{jobName}] --> [#{cmdOutput.chomp}]")
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name   = "k8s-job"
consumer_name = "k8s-job-dispatcher"
streams = [ "JobSubmitted:#{environment}:ScanItemGenerator" ]

puts("INFO : ### Start dispatching jobs.")
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

# ✅ Loop อ่าน message จากทุก stream
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

        submit_job(stream, data, conn)
        start_job(data)
      end
    end
  end
end

