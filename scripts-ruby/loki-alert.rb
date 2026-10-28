#!/usr/bin/env ruby

require "json"
require "yaml"
require "uri"
require "net/http"
require "time"
require "logger"

LOGGER = Logger.new($stdout)
LOGGER.level = ENV.fetch("LOG_LEVEL", "INFO").upcase.then do |level|
  Logger.const_get(level)
rescue NameError
  Logger::INFO
end
LOGGER.formatter = proc do |severity, time, _progname, message|
  "#{time.utc.iso8601} #{severity} #{message}\n"
end

CONFIG_RULE_PATH = ENV.fetch("CONFIG_RULE_PATH")
ENDPOINT_LOKI = ENV.fetch("ENDPOINT_LOKI").sub(%r{/+\z}, "")
ENDPOINT_ALM = ENV.fetch("ENDPOINT_ALM").sub(%r{/+\z}, "")

HTTP_TIMEOUT = Integer(ENV.fetch("HTTP_TIMEOUT", "10"))
DEFAULT_WINDOW = ENV.fetch("QUERY_WINDOW", "5m")
DEFAULT_LIMIT = Integer(ENV.fetch("LOKI_QUERY_LIMIT", "100"))
DEFAULT_ALERT_TTL = ENV.fetch("ALERT_TTL", "2m")
MAX_SAMPLE_LOGS = Integer(ENV.fetch("MAX_SAMPLE_LOGS", "3"))

# Convert durations such as 30s, 5m, 1h, 1d into seconds.
def duration_seconds(value)
  match = /\A(\d+)(s|m|h|d|w)\z/.match(value.to_s)
  raise ArgumentError, "Invalid duration: #{value}" unless match

  amount = match[1].to_i
  unit = { "s" => 1, "m" => 60, "h" => 3600,
           "d" => 86_400, "w" => 604_800 }.fetch(match[2])

  amount * unit
end

def http_request(uri, request)
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = uri.scheme == "https"
  http.open_timeout = HTTP_TIMEOUT
  http.read_timeout = HTTP_TIMEOUT
  http.write_timeout = HTTP_TIMEOUT if http.respond_to?(:write_timeout=)

  http.start { |connection| connection.request(request) }
end

def get_json(url, params = {})
  uri = URI(url)
  uri.query = URI.encode_www_form(params)

  response = http_request(uri, Net::HTTP::Get.new(uri))
  unless response.is_a?(Net::HTTPSuccess)
    raise "HTTP #{response.code} from #{uri}: #{response.body[0, 500]}"
  end

  JSON.parse(response.body)
end

def load_config
  config = YAML.safe_load_file(
    CONFIG_RULE_PATH,
    permitted_classes: [],
    permitted_symbols: [],
    aliases: false
  )

  raise "Config must be a YAML mapping" unless config.is_a?(Hash)

  rules = config.fetch("rules")
  raise "rules must be an array" unless rules.is_a?(Array)

  [config.fetch("global", {}), rules]
end

def query_loki(query, window, limit)
  now = Time.now
  from = now - duration_seconds(window)

  # Loki accepts Unix timestamps in nanoseconds.
  result = get_json(
    "#{ENDPOINT_LOKI}/loki/api/v1/query_range",
    {
      "query" => query,
      "start" => (from.to_r * 1_000_000_000).to_i.to_s,
      "end" => (now.to_r * 1_000_000_000).to_i.to_s,
      "limit" => limit.to_s,
      "direction" => "backward"
    }
  )

  unless result["status"] == "success"
    raise "Loki returned an unsuccessful query response"
  end

  result.fetch("data").fetch("result")
end

def extract_matches(results)
  matches = []

  results.each do |stream|
    labels = stream.fetch("stream", {})
    values = stream.fetch("values", [])

    values.each do |entry|
      timestamp, line = entry
      matches << {
        "timestamp" => timestamp,
        "line" => line.to_s,
        "stream" => labels
      }
    end
  end

  # Also count non-stream results, such as metric queries.
  # They can be returned as "value" rather than "values".
  results.each do |item|
    next if item.key?("stream") || !item.key?("value")

    matches << {
      "timestamp" => item.dig("value", 0),
      "line" => "Metric result: #{item['value'][1]}",
      "stream" => item.fetch("metric", {})
    }
  end

  matches
end

def build_alert(rule, global, matches, query, window)
  now = Time.now.utc
  ttl = rule["alert_ttl"] || global["alert_ttl"] || DEFAULT_ALERT_TTL

  labels = {
    "alertname" => rule.fetch("name").to_s,
    "severity" => (rule["severity"] || "warning").to_s
  }

  (rule["labels"] || {}).each do |key, value|
    labels[key.to_s] = value.to_s unless key.to_s == "alertname"
  end

  annotations = {
    "summary" => "#{rule.fetch('name')}: Loki query matched",
    "description" => "Matched #{matches.length} result(s) in the last #{window}.",
    "query" => query
  }

  (rule["annotations"] || {}).each do |key, value|
    annotations[key.to_s] = value.to_s
  end

  {
    "labels" => labels,
    "annotations" => annotations,
    "startsAt" => now.iso8601,
    "endsAt" => (now + duration_seconds(ttl)).iso8601,
    "generatorURL" => "#{ENDPOINT_LOKI}/"
  }
end

def send_alerts(alerts)
  return if alerts.empty?

  uri = URI("#{ENDPOINT_ALM}/api/v2/alerts")
  request = Net::HTTP::Post.new(uri)
  request["Content-Type"] = "application/json"
  request.body = JSON.generate(alerts)

  response = http_request(uri, request)

  unless response.is_a?(Net::HTTPSuccess)
    raise "Alertmanager HTTP #{response.code}: #{response.body[0, 500]}"
  end

  LOGGER.info("Sent #{alerts.length} alert(s) to Alertmanager")
end

def main
  global, rules = load_config
  window = ENV.fetch("QUERY_WINDOW", global["window"] || DEFAULT_WINDOW)
  limit = Integer(global.fetch("limit", DEFAULT_LIMIT))
  alerts = []

  rules.each do |rule|
    next if rule["enabled"] == false

    name = rule.fetch("name")
    query = rule.fetch("query")
    rule_window = rule["window"] || window

    begin
      # Validate the duration before querying.
      duration_seconds(rule_window)

      LOGGER.info("Querying rule=#{name} window=#{rule_window}")

      results = query_loki(query, rule_window, limit)
      matches = extract_matches(results)

      if matches.empty?
        LOGGER.info("No match rule=#{name}")
        next
      end

      LOGGER.warn(
        "MATCH rule=#{name} count=#{matches.length} " \
        "window=#{rule_window}"
      )

      matches.first(MAX_SAMPLE_LOGS).each do |match|
        LOGGER.warn(
          "MATCH_DETAIL rule=#{name} " \
          "timestamp=#{match['timestamp']} " \
          "stream=#{JSON.generate(match['stream'])} " \
          "line=#{match['line'].inspect}"
        )
      end

      alerts << build_alert(rule, global, matches, query, rule_window)
    rescue StandardError => e
      LOGGER.error("Rule failed rule=#{name}: #{e.class}: #{e.message}")
      raise if ENV["FAIL_FAST"] == "true"
    end
  end

  send_alerts(alerts)
rescue StandardError => e
  LOGGER.fatal("#{e.class}: #{e.message}")
  exit 1
end

main
