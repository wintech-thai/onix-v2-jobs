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

def get_webhook_config(conn, merchantId)
  sql = "SELECT webhook_url FROM \"WebhookConfigs\" WHERE (merchant_id = $1) AND (event_name = $2)"

  res = conn.exec_params(sql, [merchantId, 'Payment.Success'])
  if res.ntuples <= 0
    return nil
  end

  data = res.first
  return data
end

def call_webhook(webhookConfig, data, lines)
  # webhookConfig ประกอบไปด้วย field
  # 1. endpoint_url
  # 2. http_method
  # 3. timeout_sec
  # 4. headers_definition ที่เป็น stringของ JSON {"API-Key":"cccccxxxx"}

  # สิ่งที่ต้องการคือ ให้เรียก endpoint_url ด้วย http_method ที่กำหนด โดยมี header ตาม headers_definition และ body เป็น data ที่ส่งมา
  # ให้เช็คด้วยว่า endpoint_url เป็น http หรือ https และตั้ง timeout ตาม timeout_sec
  # ให้เก็บ log ของการเรียก webhook ว่าเรียกไปที่ไหน ใช้ method อะไร และผลลัพธ์เป็นอย่างไร เช่น status code, 20 chars แรกของ body ที่ response มาโดยเก็บไว้ใน lines ซึ่งเป็น array ของ string

end

def process_payment_success_job(stream, data, conn)
  lines = [];
  jobId = data['Id']

  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  merchantId = hash['MERCHANT_ID']
  merchantCode = hash['MERCHANT_CODE']

  str = "INFO : [#{jobId}] : Processing job from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str)
  lines.push(str)

  jobStatus = 'Submitted'
  update_job_status(conn, jobId, jobStatus)

  jobStatus = 'Running'
  update_job_status(conn, jobId, jobStatus)

  whc = get_webhook_config(conn, merchantId)
  if (whc.nil?)
    str = "ERROR : [#{jobId}] : No webhook configuration found for merchant [#{merchantId}] [#{merchantCode}]"
    puts(str)
    lines.push(str)

    message = lines.join("\n")
    update_job_done(conn, jobId, 0, 1, message)
    return 
  end

  isEnabled = whc['is_active']
  if (!isEnabled)
    str = "ERROR : [#{jobId}] : Webhook is not active for merchant [#{merchantId}] [#{merchantCode}]"
    puts(str)
    lines.push(str)

    message = lines.join("\n")
    update_job_done(conn, jobId, 0, 1, message)
    return 
  end


  webhookUrl = whc['endpoint_url']
  str = "INFO : [#{jobId}] : Calling webhook [#{webhookUrl}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str)
  lines.push(str)

  # Calling webhook here...
  call_webhook(whc, data, lines)


  str = "INFO : [#{jobId}] : Done processing job from stream [#{stream}] for merchant [#{merchantId}] [#{merchantCode}]"
  puts(str)
  lines.push(str)

  message = lines.join("\n")
  update_job_done(conn, jobId, 1, 0, message)
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name   = "k8s-job"
consumer_name = "k8s-job-dispatcher"
streams = [
  "JobSubmitted:#{environment}:Payment.Success",
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

        jobType = data['Type']
        if jobType == 'Payment.Success'
          process_payment_success_job(stream, data, conn)
        end

      end
    end
  end
end

