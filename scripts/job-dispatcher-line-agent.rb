#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'open3'
require 'net/http'
require 'json'

require './utils'

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

def get_yaml(param, appName)
  namespace = ENV['NAMESPACE']
  apiBaseUrl = ENV['API_BASE_URL']

  agentId   = param['AGENT_ID']
  agentCode = param['AGENT_CODE']
  imageTag  = "v0.0.1" #param['AGENT_IMAGE_TAG']
  imageRepo = "asia-southeast1-docker.pkg.dev/its-artifact-commons/please-payment/please-payment-agent"

  endPointNotification = param['NOTIFICATION_ENDPOINT']
  endPointNotification = endPointNotification.sub('https://<PAYMENT-REQUEST-SERVICE>', apiBaseUrl)

  endPointHeartbeat = param['HEARTBEAT_ENDPOINT']
  endPointHeartbeat = endPointHeartbeat.sub('https://<PAYMENT-REQUEST-SERVICE>', apiBaseUrl)

  # Env Variables
  envVars = {
    'AGENT_CODE'             => agentCode,
    'LINE_USERNAME'          => param['LINE_USERNAME'],
    'ENDPOINT_API_KEY'       => param['ENDPOINT_API_KEY'],
    'HEARTBEAT_ENDPOINT'     => endPointHeartbeat,
    'NOTIFICATION_ENDPOINT'  => endPointNotification,
  }

  envYaml = envVars.map do |key, value|
    <<~ENV.chomp
            - name: #{key}
              value: "#{value}"
    ENV
  end.join("\n")

  envYaml = envVars.map do |key, value|
    <<~YAML
  - name: #{key}
    value: "#{value}"
    YAML
  end.join("\n").gsub(/^/, " " * 12)

  yaml = <<~YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: #{appName}
  namespace: #{namespace}
  labels:
    app: line-agent
    agent-id: "#{agentId}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: #{appName}
  template:
    metadata:
      labels:
        app: #{appName}
        agent-id: "#{agentId}"
    spec:
      containers:
        - name: #{appName}
          image: #{imageRepo}:#{imageTag}
          imagePullPolicy: IfNotPresent
          env:
#{envYaml}
          ports:
            - containerPort: 3000
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
---
apiVersion: v1
kind: Service
metadata:
  name: #{appName}
  namespace: #{namespace}
  labels:
    app: #{appName}
    agent-id: "#{agentId}"
spec:
  selector:
    app: #{appName}
  ports:
    - name: http
      port: 80
      targetPort: 3000
      protocol: TCP
  type: ClusterIP
YAML

  yaml
end

def process_agent_job(jobType, stream, data, conn)
  lines = [];
  jobId = data['Id']

  params = data['Parameters']
  hash = params.map { |p| [p['Name'], p['Value']] }.to_h
  userId = hash['USER_ID']
  agentId = hash['AGENT_ID']

  str = "INFO : [#{jobId}] : Processing job from stream [#{stream}] for agent ID [#{agentId}], type=[#{jobType}]"
  puts(str)
  lines.push(str)

  jobStatus = 'Submitted'
  update_job_status(conn, jobId, jobStatus)

  jobStatus = 'Running'
  update_job_status(conn, jobId, jobStatus)

  str = "INFO : [#{jobId}] : Done processing job from stream [#{stream}] for agent ID [#{agentId}], type=[#{jobType}]"
  puts(str)
  lines.push(str)

  agentId = hash['AGENT_ID']
  appName = app_name = "line-agent-#{agentId[0,8]}"

  if (['Agent.Create', 'Agent.Update'].include?(jobType))
    #Do Somthing here
    yaml = get_yaml(hash, appName)

    stdout, stderr, status = Open3.capture3(
      "kubectl", "apply", "-f", "-",
      stdin_data: yaml
    )

    if status.success?
      lines.push(stdout)
    else
      lines.push(stderr)
      puts("===========\n")
      puts(yaml)
      puts("===========\n")
      puts("ERROR : #{stderr}")
    end
  elsif (jobType == "Agent.Delete")
    # Do something here
  elsif (jobType == "Agent.Restart")
    # Do something here
  end

  message = lines.join("\n")
  update_job_done(conn, jobId, 1, 0, message)
end

$stdout.sync = true

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name   = "k8s-job"
consumer_name = "k8s-job-dispatcher-line-agent"
streams = [
  "JobSubmitted:#{environment}:Agent.Create",
  "JobSubmitted:#{environment}:Agent.Update",
  "JobSubmitted:#{environment}:Agent.Delete",
  "JobSubmitted:#{environment}:Agent.Restart",
]

puts("INFO : ### Start dispatching jobs LINE agent.")
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
        process_agent_job(jobType, stream, data, conn)

      end
    end
  end
end

