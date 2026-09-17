#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'json'
require 'digest'
require 'net/http'
require 'ipaddr'
require 'maxminddb'
require './utils'

if File.exist?('env.rb')
  require './env'
end

$stdout.sync = true

# ============================================================
# GeoIP Service
# ============================================================

class GeoIpService
  def initialize(db_path)
    @db_path = db_path
    @sha_path = "#{db_path}.sha256"
    @db = nil
    @sha256 = nil
    @mutex = Mutex.new
    @cache = {}
    @cache_max_size = 50_000
    load_existing_database
  end

  def lookup(ip)
    return nil unless public_ip?(ip)

    cached = @mutex.synchronize { @cache[ip] }
    return cached if cached

    db = @mutex.synchronize { @db }
    return nil unless db

    result = db.lookup(ip)
    return nil unless result.found?

    geo = {
      "CountryCode" => result.country&.iso_code,
      "Country" => result.country&.name,
      "City" => result.city&.name,
      "Latitude" => result.location&.latitude,
      "Longitude" => result.location&.longitude
    }

    @mutex.synchronize do
      @cache.clear if @cache.size >= @cache_max_size
      @cache[ip] = geo
    end

    geo
  rescue => e
    puts "WARN : ### GeoIP lookup failed IP=[#{ip}] => #{e.message}"
    nil
  end

  def current_sha256
    @mutex.synchronize { @sha256 }
  end

  def install(new_file, sha256)
    puts "INFO : ### Validating GeoIP database..."
    new_db = MaxMindDB.new(new_file)

    puts "INFO : ### Installing GeoIP database..."
    File.rename(new_file, @db_path)
    File.write(@sha_path, sha256)

    @mutex.synchronize do
      @db = new_db
      @sha256 = sha256
      @cache.clear
    end

    puts "INFO : ### GeoIP database installed"
    puts "INFO : ### GeoIP SHA256=[#{sha256}]"
  rescue => e
    puts "ERROR : ### Failed to install GeoIP database => #{e.message}"
    raise
  end

  private

  def load_existing_database
    unless File.exist?(@db_path)
      puts "WARN : ### GeoIP database not found [#{@db_path}]"
      return
    end

    begin
      db = MaxMindDB.new(@db_path)

      if File.exist?(@sha_path)
        sha256 = File.read(@sha_path).strip
      else
        puts "INFO : ### GeoIP SHA256 file not found. Calculating..."
        sha256 = Digest::SHA256.file(@db_path).hexdigest
        File.write(@sha_path, sha256)
      end

      @mutex.synchronize do
        @db = db
        @sha256 = sha256
      end

      puts "INFO : ### Loaded GeoIP database"
      puts "INFO : ### GeoIP SHA256=[#{sha256}]"
    rescue => e
      puts "ERROR : ### Unable to load existing GeoIP DB => #{e.message}"
    end
  end

  def public_ip?(ip)
    return false if ip.nil? || ip.empty?

    addr = IPAddr.new(ip)
    return false if addr.private?
    return false if addr.loopback?
    return false if addr.link_local?

    true
  rescue IPAddr::InvalidAddressError
    false
  end
end

# ============================================================
# HTTP helper
# ============================================================

def http_get(uri)
  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
    request = Net::HTTP::Get.new(uri.request_uri)
    response = http.request(request)

    raise "HTTP #{response.code} #{response.message}" unless response.is_a?(Net::HTTPSuccess)

    response.body
  end
end

# ============================================================
# Submit Audit Log
# ============================================================

def submit_log(data, conn, rawJson, geoip)
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
    remote_ip
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
    raw_data
  ]

  placeholders = fields.each_index.map { |i| "$#{i + 1}" }.join(", ")

  sql = <<-SQL
    INSERT INTO "AuditLogs"
    (
      log_id,
      #{fields.join(", ")},
      created_date
    )
    VALUES
    (
      gen_random_uuid(),
      #{placeholders},
      CURRENT_TIMESTAMP
    )
  SQL

  path = data['Path']

  # ==========================================================
  # Parse API Path
  # ==========================================================

  if path =~ %r{^/org/([^/]+)/([^/]+)/([^/]+)/([^/]+)}
    data['OrgId'] = $1
    data['ApiName'] = $2
    data['Serial'] = $3
    data['Pin'] = $4
    data['Controller'] = "ScanItem"
  elsif path =~ %r{^/admin-api/([^/]+)/org/([^/]+)/action/([^/]+)}
    data['Controller'] = $1
    data['OrgId'] = $2
    data['ApiName'] = $3
    data['Serial'] = ""
    data['Pin'] = ""
  elsif path =~ %r{^/api/([^/]+)/org/([^/]+)/action/([^/]+)}
    data['Controller'] = $1
    data['OrgId'] = $2
    data['ApiName'] = $3
    data['Serial'] = ""
    data['Pin'] = ""
  end

  # ==========================================================
  # GeoIP
  # ==========================================================

  geo_data = geoip.lookup(data['ClientIp'])

  # ==========================================================
  # Build raw_data JSON
  # ==========================================================

  raw_data_obj = {
    "@timestamp" => data['@timestamp'] || Time.now.utc.iso8601(9),
    "data" => {
      "ClientIp" => data['ClientIp'],
      "GeoIP" => geo_data,
      "CfClientIp" => data['CfClientIp'],
      "RemoteIp" => data['RemoteIp'],
      "StatusCode" => data['StatusCode'],
      "Path" => data['Path'],
      "QueryString" => data['QueryString'],
      "UserAgent" => data['UserAgent'],
      "Host" => data['Host'],
      "Environment" => data['Environment'],
      "CustomStatus" => data['CustomStatus'],
      "CustomDesc" => data['CustomDesc'],
      "RequestSize" => data['RequestSize'],
      "ResponseSize" => data['ResponseSize'],
      "LatencyMs" => data['LatencyMs'],
      "ApplicationType" => data['ApplicationType'],
      "OrgType" => data['OrgType'],
      "api" => {
        "ApiName" => data['ApiName'],
        "Controller" => data['Controller'],
        "OrgId" => data['OrgId']
      },
      "userInfo" => data['userInfo']
    }
  }

  # ==========================================================
  # PostgreSQL Values
  # ==========================================================

  values = [
    data['OrgId'],
    data['HttpMethod'],
    data['StatusCode'],
    data['Path'],
    data['QueryString'],
    data['UserAgent'],
    data['Host'],
    data['Scheme'],
    data['ClientIp'],
    data['CfClientIp'],
    data['RemoteIp'],
    data['Environment'],
    data['CustomStatus'],
    data['CustomDesc'],
    data['RequestSize'],
    data['ResponseSize'],
    data['LatencyMs'],
    data['userInfo']['Role'],
    data['userInfo']['IdentityType'],
    data['userInfo']['UserId'],
    data['userInfo']['UserName'],
    data['ApiName'],
    data['Controller'],
    data['Serial'],
    data['Pin'],
    raw_data_obj.to_json
  ]

  conn.exec_params(sql, values)
end

# ============================================================
# Environment
# ============================================================

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
group_name = "k8s-log"
consumer_name = "k8s-log-dispatcher"
logEndpoint = ENV['LOG_ENDPOINT']
sendToPg = ENV['SEND_TO_PG'] == "true"
sendToHttp = ENV['SEND_TO_HTTP'] == "true"

# ============================================================
# GeoIP Configuration
# ============================================================

geoip_path = ENV['GEOIP_DB_PATH'] || '/data/temp/GeoLite2-City.mmdb'
geoip_version_url = ENV['GEOIP_VERSION_URL'] || 'http://svc-geoip-db-sync/version.json'
geoip_download_url = ENV['GEOIP_DOWNLOAD_URL'] || 'http://svc-geoip-db-sync/GeoLite2-City.mmdb'
geoip_update_interval = (ENV['GEOIP_UPDATE_INTERVAL'] || '3600').to_i

geoip_directory = File.dirname(geoip_path)
Dir.mkdir(geoip_directory) unless Dir.exist?(geoip_directory)

# ============================================================
# Streams
# ============================================================

streams = [
  "AuditLog:#{environment}",
  "AgentStat:#{environment}"
]

# ============================================================
# Startup Logs
# ============================================================

puts("INFO : ### Start dispatching jobs.")
puts("INFO : ### ENVIRONMENT=[#{environment}]")
puts("INFO : ### REDIS_HOST=[#{redisHost}]")
puts("INFO : ### REDIS_PORT=[#{redisPort}]")
puts("INFO : ### SEND_TO_PG=[#{sendToPg}]")
puts("INFO : ### SEND_TO_HTTP=[#{sendToHttp}]")
puts("INFO : ### GEOIP_DB_PATH=[#{geoip_path}]")
puts("INFO : ### GEOIP_VERSION_URL=[#{geoip_version_url}]")
puts("INFO : ### GEOIP_DOWNLOAD_URL=[#{geoip_download_url}]")
puts("INFO : ### GEOIP_UPDATE_INTERVAL=[#{geoip_update_interval}s]")

# ============================================================
# PostgreSQL
# ============================================================

pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]

conn = connect_db(
  pgHost,
  pgDb,
  ENV["PG_USER"],
  ENV["PG_PASSWORD"]
)

if conn.nil?
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end

puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")

# ============================================================
# Redis
# ============================================================

redis = Redis.new(
  host: redisHost,
  port: redisPort
)

# ============================================================
# GeoIP Service
# ============================================================

geoip = GeoIpService.new(geoip_path)
# ============================================================
# GeoIP Update Thread
# ============================================================

geoip_update_thread = Thread.new do
  loop do
    begin
      puts "INFO : ### Checking GeoIP database version..."

      version_uri = URI.parse(geoip_version_url)
      version_json = http_get(version_uri)
      version = JSON.parse(version_json)
      remote_sha256 = version['actual_file_sha256']

      raise "version.json does not contain sha256" if remote_sha256.nil? || remote_sha256.empty?

      local_sha256 = geoip.current_sha256

      puts "INFO : ### GeoIP local SHA256=[#{local_sha256}]"
      puts "INFO : ### GeoIP remote SHA256=[#{remote_sha256}]"

      if local_sha256 == remote_sha256
        puts "INFO : ### GeoIP database is up-to-date"
      else
        puts "INFO : ### New GeoIP database detected"
        puts "INFO : ### Edition=[#{version['edition_id']}]"
        puts "INFO : ### Filename=[#{version['filename']}]"
        puts "INFO : ### ReleaseDate=[#{version['release_date']}]"

        tmp_path = "#{geoip_path}.tmp"
        File.delete(tmp_path) if File.exist?(tmp_path)

        puts "INFO : ### Downloading GeoIP database..."
        download_uri = URI.parse(geoip_download_url)

        Net::HTTP.start(
          download_uri.host,
          download_uri.port,
          use_ssl: download_uri.scheme == 'https'
        ) do |http|
          request = Net::HTTP::Get.new(download_uri.request_uri)

          http.request(request) do |response|
            unless response.is_a?(Net::HTTPSuccess)
              raise "GeoIP download failed HTTP #{response.code} #{response.message}"
            end

            File.open(tmp_path, 'wb') do |file|
              response.read_body { |chunk| file.write(chunk) }
            end
          end
        end

        puts "INFO : ### Verifying GeoIP SHA256..."
        actual_sha256 = Digest::SHA256.file(tmp_path).hexdigest
        puts "INFO : ### GeoIP downloaded SHA256=[#{actual_sha256}]"

        unless actual_sha256 == remote_sha256
          raise "GeoIP SHA256 mismatch expected=[#{remote_sha256}] actual=[#{actual_sha256}]"
        end

        puts "INFO : ### GeoIP SHA256 verified"
        geoip.install(tmp_path, remote_sha256)
        puts "INFO : ### GeoIP update completed"
      end
    rescue => e
      puts "ERROR : ### GeoIP updater failed => #{e.message}"

      begin
        tmp_path = "#{geoip_path}.tmp"
        File.delete(tmp_path) if File.exist?(tmp_path)
      rescue
      end
    end

    # Wait before checking the next version
    sleep geoip_update_interval
  end
end

# ============================================================
# Create Redis Consumer Groups
# ============================================================

streams.each do |stream_key|
  begin
    redis.xgroup(:create, stream_key, group_name, "$", mkstream: true)

    puts("INFO : ### Created group [#{group_name}] for stream [#{stream_key}]")
  rescue Redis::CommandError => e
    if e.message.include?("BUSYGROUP")
      puts("INFO : ### Group already created for stream [#{stream_key}]")
    else
      raise
    end
  end
end

# ============================================================
# Main Redis Consumer Loop
# ============================================================

loop do
  entries = redis.xreadgroup(
    group_name,
    consumer_name,
    streams,
    Array.new(streams.size, ">"),
    count: 10,
    block: 5000
  )

  next unless entries

  entries.each do |stream, messages|
    messages.each do |id, fields|
      puts("INFO : ### Got [#{id}] from stream [#{stream}], group [#{group_name}]")

      begin
        rawJson = fields["message"]
        data = JSON.parse(rawJson)

        if data
          submit_log(data, conn, rawJson, geoip) if sendToPg
          send_audit_log_etl(rawJson, logEndpoint) if sendToHttp

          # ACK only after processing successfully
          redis.xack(stream, group_name, id)

          puts("INFO : ### ACK [#{id}]")
        end
      rescue JSON::ParserError => e
        puts("WARN : ### Failed to parse JSON from stream [#{stream}] ID=[#{id}] => #{e.message}")

        # ACK invalid JSON so it does not loop forever
        redis.xack(stream, group_name, id)
      rescue => e
        puts("ERROR : ### Processing failed stream=[#{stream}] ID=[#{id}] => #{e.message}")

        # DO NOT ACK
        # Redis will keep this message pending.
        # It can be retried/reclaimed later.
      end
    end
  end
end
