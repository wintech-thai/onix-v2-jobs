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

def process_payment_success_job(stream, data, conn)
  lines = [];
  jobId = data['Id']

  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  merchantId = hash['MERCHANT_ID']
  merchantCode = hash['MERCHANT_CODE']

  str = "INFO : [#{jobId}] : Notifying from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name   = "k8s-job"
consumer_name = "payment-notifier"
streams = [
  "JobSubmitted:#{environment}:Payment.Success",
]

puts("INFO : ### Start dispatching jobs for notification.")
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

        jobType = data['Type']
        if jobType == 'Payment.Success'
          process_payment_success_job(stream, data, conn)
        end

      end
    end
  end
end

