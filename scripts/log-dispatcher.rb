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

$stdout.sync = true

def submit_log(data, conn, rawJson)
  puts(rawJson)

  fields = %w[
    org_id
    http_method
    status_code
    path
    query_string
    user_agent
    host
    scheme
    client_ip
    client_ip_cf
    environment
    custom_status
    custom_desc
    request_size
    response_size
    latency_ms
    role
    identity_type
    user_id
    user_name
    api_name
    controller_name
    serial
    pin
  ]

  placeholders = fields.each_index.map { |i| "$#{i + 1}" }.join(", ")

  # SQL Statement
  sql = <<-SQL
    INSERT INTO "AuditLogs" (log_id, #{fields.join(", ")}, created_date)
    VALUES (gen_random_uuid(), #{placeholders}, CURRENT_TIMESTAMP)
SQL

  path = data['Path']

  if path =~ %r{^/org/([^/]+)/([^/]+)/([^/]+)/([^/]+)}
    data['OrgId'] = $1
    data['ApiName'] = $2
    data['Serial'] = $3
    data['Pin'] = $4
    data['Controller'] = "ScanItem"
  elsif path =~ %r{^/api/([^/]+)/org/([^/]+)/action/([^/]+)}
    data['Controller'] = $1
    data['OrgId'] = $2
    data['ApiName'] = $3
    data['Serial'] = ""
    data['Pin'] = ""
  end

  values = [
    data['OrgId'],
    data['HttpMethod'],               # http_method
    data['StatusCode'],               # status_code
    data['Path'],                     # path
    data['QueryString'],              # query_string
    data['UserAgent'],                # user_agent
    data['Host'],                     # host
    data['Scheme'],                   # scheme
    data['ClientIp'],                 # client_ip
    data['CfClientIp'],               # client_ip_cf
    data['Environment'],              # environment
    data['CustomStatus'],             # custom_status
    data['CustomDesc'],               # custom_desc
    data['RequestSize'],              # request_size
    data['ResponseSize'],             # response_size
    data['LatencyMs'],                # latency_ms
    data['userInfo']['Role'],         # role
    data['userInfo']['IdentityType'], # identity_type
    data['userInfo']['UserId'],       # user_id
    data['userInfo']['UserName'],     # user_name
    data['ApiName'],
    data['Controller'],
    data['Serial'],
    data['Pin'],
  ]

  # Execute พร้อม binding
  conn.exec_params(sql, values)
end

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name   = "k8s-log"
consumer_name = "k8s-log-dispatcher"
logEndpoint = ENV['LOG_ENDPOINT']

streams = [
  "AuditLog:#{environment}",
]

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

        submit_log(data, conn, rawJson)
        send_audit_log_etl(rawJson, logEndpoint)
      end
    end
  end
end

