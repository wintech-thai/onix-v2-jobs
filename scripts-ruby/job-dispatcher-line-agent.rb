#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'open3'
require 'net/http'
require 'json'
require 'base64'
require 'securerandom'

require './utils'

if File.exist?('env.rb')
  require './env'
end

def get_yaml(param, appName)
  namespace = ENV['NAMESPACE']
  apiBaseUrl = ENV['API_BASE_URL']

  agentId   = param['AGENT_ID']
  agentCode = param['AGENT_CODE']
  imageTag  = param['AGENT_IMAGE_TAG']
  imageRepo = "asia-southeast1-docker.pkg.dev/its-artifact-commons/please-payment/please-payment-agent"

  endPointNotification = (param['NOTIFICATION_ENDPOINT'] || '').sub('https://<PAYMENT-REQUEST-SERVICE>', apiBaseUrl.to_s)

  endPointHeartbeat = (param['HEARTBEAT_ENDPOINT'] || '').sub('https://<PAYMENT-REQUEST-SERVICE>', apiBaseUrl.to_s)

  envVars = {
    'ONIX_AGENT_ID'            => agentId,
    'ONIX_API_USER'            => param['LINE_USERNAME'],
    'ONIX_API_KEY'             => param['ENDPOINT_API_KEY'],
    'ONIX_ORG'                 => 'global',
    'ONIX_API_URL'             => endPointNotification,
    'ONIX_APPLICATION_TYPE'    => 'backend',
    'ONIX_FORWARD_TIMEOUT_MS'  => '1000',
    'INSTANCE_ID'              => agentId,
    'BOT_NAME'                 => agentId,
    'HTTP_PORT'                => '3000',
    'HTTP_API_USER'            => 'api',
    'HTTP_API_KEY'             => agentId,
    'REDIS_HOST'               => ENV['REDIS_HOST'],
    'REDIS_PORT'               => ENV['REDIS_PORT'],
    'LINE_DEVICE'              => 'ANDROIDSECONDARY',
    'LOG_LEVEL'                => 'debug',
    'BANK_OA_MIDS'             => 'u4ca19114ed596ee2f4e63335ec7143fb,u8cc52e369d2bca4a5ce8c506170c712e,uce372f6ada1d1a0855973fefc2942f9a,ub2a0ffaaab7e5bdd10814ec88afe67fc',
  }

  envYaml = envVars.map do |key, value|
    <<~YAML
  - name: #{key}
    value: "#{value}"
    YAML
  end.join("\n").gsub(/^/, " " * 12)

  <<~YAML
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
      agent-id: "#{agentId}"
  template:
    metadata:
      labels:
        app: #{appName}
        agent-id: "#{agentId}"
    spec:
      containers:
        - name: #{appName}
          image: "#{imageRepo}:#{imageTag}"
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
end

def process_agent_job(jobType, stream, data, conn)
  lines  = []
  jobId  = data['Id']
  params = data['Parameters']
  hash   = params.map { |p| [p['Name'], p['Value']] }.to_h
  agentId = hash['AGENT_ID']

  str = "INFO : [#{jobId}] : Processing job from stream [#{stream}] for agent ID [#{agentId}], type=[#{jobType}]"
  puts(str); lines << str

  update_job_status(conn, jobId, 'Submitted')
  update_job_status(conn, jobId, 'Running')

  str = "INFO : [#{jobId}] : Done processing job from stream [#{stream}] for agent ID [#{agentId}], type=[#{jobType}]"
  puts(str); lines << str

  appName = "line-agent-#{agentId[0, 8]}"

  if ['Agent.Create', 'Agent.Update'].include?(jobType)
    yaml = get_yaml(hash, appName)
    stdout, stderr, status = Open3.capture3('kubectl', 'apply', '-f', '-', stdin_data: yaml)
    if status.success?
      lines << stdout
    else
      lines << stderr
      puts "ERROR : #{stderr}"
    end
  elsif jobType == 'Agent.Delete'
    yaml = get_yaml(hash, appName)
    stdout, stderr, status = Open3.capture3('kubectl', 'delete', '-f', '-', stdin_data: yaml)
    if status.success?
      lines << stdout
    else
      lines << stderr
      puts "ERROR : #{stderr}"
    end
  elsif jobType == 'Agent.Disable'
    namespace = ENV['NAMESPACE']
    ['deployment', 'service'].each do |kind|
      stdout, stderr, status = Open3.capture3(
        'kubectl', 'delete', kind, appName, '-n', namespace, '--ignore-not-found=true'
      )
      if status.success?
        lines << stdout
      else
        lines << stderr
        puts "ERROR : #{stderr}"
      end
    end
  elsif jobType == 'Agent.Restart'
    stdout, stderr, status = Open3.capture3(
      'kubectl', 'rollout', 'restart', "deployment/#{appName}", '-n', ENV.fetch('NAMESPACE')
    )
    if status.success?
      lines << stdout
    else
      lines << stderr
    end
  end

  update_job_done(conn, jobId, 1, 0, lines.join("\n"))
end

$stdout.sync = true

environment   = ENV['ENVIRONMENT']
redisHost     = ENV['REDIS_HOST']
redisPort     = ENV['REDIS_PORT']
pgHost        = ENV['PG_HOST']
pgDb          = ENV['PG_DB']

group_name    = 'k8s-job'
consumer_name = 'k8s-job-dispatcher-line-agent'
streams = [
  "JobSubmitted:#{environment}:Agent.Create",
  "JobSubmitted:#{environment}:Agent.Update",
  "JobSubmitted:#{environment}:Agent.Delete",
  "JobSubmitted:#{environment}:Agent.Disable",
  "JobSubmitted:#{environment}:Agent.Restart",
]

KNOWN_AGENT_JOB_TYPES = %w[Agent.Create Agent.Update Agent.Delete Agent.Disable Agent.Restart].freeze
MAX_PG_RECONNECT = 3

puts "INFO : ### Start dispatching jobs LINE agent."
puts "INFO : ### ENVIRONMENT=[#{environment}]"
puts "INFO : ### REDIS_HOST=[#{redisHost}]"
puts "INFO : ### REDIS_PORT=[#{redisPort}]"

redis = Redis.new(host: redisHost, port: redisPort, read_timeout: 10, reconnect_attempts: 2)

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, '$', mkstream: true)
    puts "INFO : ### Created group [#{group_name}] for stream [#{stream_key}]"
  rescue Redis::CommandError => e
    puts "INFO : ### Group already created for stream [#{stream_key}]" if e.message.include?('BUSYGROUP')
  end
end

AGENT_HEALTH_CHECK_INTERVAL = 30
AGENT_NOT_READY_COOLDOWN_MIN = (ENV['AGENT_NOT_READY_COOLDOWN_MIN'] || '20').to_i

def get_line_agent_base_url(agent_id)
  clean = agent_id.gsub('-', '')
  prefix = clean.length >= 8 ? clean[0, 8].downcase : clean.downcase
  "http://line-agent-#{prefix}"
end

def check_agent_ready(agent_id)
  base_url = get_line_agent_base_url(agent_id)
  credentials = Base64.strict_encode64("api:#{agent_id}")
  uri = URI.parse("#{base_url}/status")
  http = Net::HTTP.new(uri.host, uri.port)
  http.open_timeout = 5
  http.read_timeout = 5
  req = Net::HTTP::Get.new(uri)
  req['Authorization'] = "Basic #{credentials}"
  resp = http.request(req)
  return false unless resp.code.to_i == 200
  body = JSON.parse(resp.body)
  ok = body['ok'] == true
  login = body['login']
  login_state = login.is_a?(Hash) ? login['state'] : login.to_s
  ok && login_state.to_s.downcase == 'ready'
rescue
  false
end

def create_agent_job(conn, agent_id, agent_code, event_type)
  job_id = SecureRandom.uuid
  now = Time.now.utc
  name = "#{event_type} #{agent_code || agent_id[0, 8]}"
  desc = event_type == 'Agent.NotReady' ? "LINE Agent [#{agent_code}] is not ready" : "LINE Agent [#{agent_code}] has recovered"
  conn.exec_params(
    "INSERT INTO \"Jobs\" (job_id, org_id, status, name, description, type, tags, progress_pct, succeed_cnt, failed_cnt, created_date, updated_date) " \
    "VALUES ($1, 'global', 'Submitted', $2, $3, 'Notification', 'line-agent', 0, 0, 0, $4, $4)",
    [job_id, name, desc, now]
  )
  job_id
rescue => e
  puts "WARN : [agent-health] create_agent_job failed: #{e.message}"
  nil
end

def push_agent_event(redis, environment, event_type, job_id, agent_id, agent_code)
  params = [
    { 'Name' => 'AGENT_ID',   'Value' => agent_id.to_s },
    { 'Name' => 'AGENT_CODE', 'Value' => agent_code.to_s },
    { 'Name' => 'EVENT_TYPE', 'Value' => event_type },
  ]
  payload = { 'Id' => job_id, 'Type' => event_type, 'Parameters' => params }.to_json
  stream = "JobSubmitted:#{environment}:#{event_type}"
  redis.xadd(stream, { message: payload })
  puts "INFO : [agent-health] Pushed #{event_type} job [#{job_id}] to stream [#{stream}]"
rescue => e
  puts "WARN : [agent-health] push_agent_event failed: #{e.message}"
end

Thread.new do
  environment = ENV['ENVIRONMENT']
  cooldown_sec = AGENT_NOT_READY_COOLDOWN_MIN * 60

  loop do
    begin
      # read active agents from DB via the conn (must handle conn being nil)
      active_agents = []
      begin
        res = conn&.exec("SELECT agent_id::text, code FROM \"Agents\" WHERE agent_status = 'Active'")
        active_agents = res&.to_a || []
      rescue => e
        puts "WARN : [agent-health] DB query failed: #{e.message}"
      end

      now_str = Time.now.strftime('%Y-%m-%d %H:%M:%S')

      active_agents.each do |row|
        agent_id   = row['agent_id']
        agent_code = row['code'] || agent_id[0, 8]
        redis_key  = "LineAgentStatusCheck:#{agent_id}"

        is_ready = check_agent_ready(agent_id)

        if !is_ready
          last_notified_str = begin; redis.get(redis_key); rescue; nil; end
          if last_notified_str
            elapsed = Time.now.to_i - last_notified_str.to_i
            if elapsed < cooldown_sec
              # still within cooldown, skip
              next
            end
          end
          # fire NotReady notification
          puts "WARN : [agent-health] Agent [#{agent_code}] NOT READY at #{now_str}"
          job_id = create_agent_job(conn, agent_id, agent_code, 'Agent.NotReady')
          push_agent_event(redis, environment, 'Agent.NotReady', job_id, agent_id, agent_code) if job_id
          begin; redis.set(redis_key, Time.now.to_i.to_s, ex: 86400); rescue; end

        else
          # agent is ready — check if we previously notified NotReady
          was_not_ready = begin; redis.exists?(redis_key); rescue; false; end
          if was_not_ready
            puts "INFO : [agent-health] Agent [#{agent_code}] RECOVERED at #{now_str}"
            job_id = create_agent_job(conn, agent_id, agent_code, 'Agent.Ready')
            push_agent_event(redis, environment, 'Agent.Ready', job_id, agent_id, agent_code) if job_id
            begin; redis.del(redis_key); rescue; end
          end
        end
      end
    rescue => e
      puts "WARN : [agent-health] loop error: #{e.message}"
    end

    sleep AGENT_HEALTH_CHECK_INTERVAL
  end
end

conn = nil
pg_reconnect_count = 0

loop do
  begin
    if conn.nil?
      puts "INFO : ### Connecting to PostgreSQL [#{pgHost}] [#{pgDb}]"
      conn = connect_db(pgHost, pgDb, ENV['PG_USER'], ENV['PG_PASSWORD'])
      if conn.nil?
        pg_reconnect_count += 1
        if pg_reconnect_count >= MAX_PG_RECONNECT
          puts "ERROR : ### Failed to reconnect #{MAX_PG_RECONNECT} times — exiting"
          exit 1
        end
        puts "ERROR : ### Unable to connect (attempt #{pg_reconnect_count}/#{MAX_PG_RECONNECT}) — retrying in 10s"
        sleep 10
        next
      end
      puts "INFO : ### Connected to PostgreSQL"
      pg_reconnect_count = 0

      # Drain PEL on reconnect
      ids     = Array.new(streams.size, '0')
      pending = redis.xreadgroup(group_name, consumer_name, streams, ids, count: 50) rescue nil
      if pending
        pending.each do |stream, messages|
          messages.each do |id, fields|
            begin
              data = JSON.parse(fields['message']) rescue nil
              if data && KNOWN_AGENT_JOB_TYPES.include?(data['Type'])
                process_agent_job(data['Type'], stream, data, conn)
              end
            rescue => e
              puts "WARN : PEL drain error [#{id}]: #{e.message}"
            ensure
              redis.xack(stream, group_name, id) rescue nil
            end
          end
        end
      end
    end

    File.write('/tmp/dispatcher-heartbeat', Time.now.to_i.to_s)

    entries = redis.xreadgroup(
      group_name,
      consumer_name,
      streams,
      Array.new(streams.size, '>'),
      count: 10,
      block: 5000
    )

    next unless entries

    pg_failed = false
    entries.each do |stream, messages|
      break if pg_failed
      messages.each do |id, fields|
        break if pg_failed
        begin
          data = JSON.parse(fields['message']) rescue nil
          if data && KNOWN_AGENT_JOB_TYPES.include?(data['Type'])
            process_agent_job(data['Type'], stream, data, conn)
          end
          redis.xack(stream, group_name, id) rescue nil
        rescue PG::Error => e
          puts "ERROR : ### PG connection error [#{id}]: #{e.message} — leaving in PEL for retry"
          begin; conn&.close; rescue; end
          conn = nil
          pg_failed = true
        rescue => e
          puts "ERROR : ### process error [#{id}]: #{e.message}"
          redis.xack(stream, group_name, id) rescue nil
        end
      end
    end

  rescue PG::Error => e
    puts "ERROR : ### PostgreSQL error: #{e.message} — reconnecting in 5s"
    begin; conn&.close; rescue; end
    conn = nil
    sleep 5

  rescue Redis::BaseError => e
    puts "ERROR : ### Redis error: #{e.message} — retrying in 5s"
    sleep 5

  rescue => e
    puts "ERROR : ### Unexpected error: #{e.message} — retrying in 5s"
    sleep 5
  end
end
