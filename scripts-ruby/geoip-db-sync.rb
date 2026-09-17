#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'json'
require 'fileutils'
require 'digest'
require 'time'

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

$stdout.sync = true

# Required ENV variables:
# REMOTE_BASE_URL=https://download.com
# REMOTE_VERSION_PATH=version.json
# LOCAL_DOWNLOAD_DIR=/data/geoip
# CHECK_INTERVAL=3600 (optional)

REMOTE_BASE_URL = ENV.fetch('REMOTE_BASE_URL')
REMOTE_VERSION_PATH = ENV.fetch('REMOTE_VERSION_PATH')
LOCAL_DOWNLOAD_DIR = ENV.fetch('DOWNLOAD_DIR')
CHECK_INTERVAL = ENV.fetch('CHECK_INTERVAL', '3600').to_i

FileUtils.mkdir_p(LOCAL_DOWNLOAD_DIR)

def log(message)
  puts "[#{Time.now.utc.iso8601}] #{message}"
end

def build_url(path)
  base = REMOTE_BASE_URL.end_with?('/') ? REMOTE_BASE_URL : "#{REMOTE_BASE_URL}/"
  URI.join(base, path).to_s
end

def http_get(url)
  uri = URI.parse(url)
  response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
    request = Net::HTTP::Get.new(uri)
    http.request(request)
  end

  unless response.is_a?(Net::HTTPSuccess)
    raise "HTTP GET failed for #{url}: #{response.code} #{response.message}"
  end

  response.body
end

def local_version_path
  File.join(LOCAL_DOWNLOAD_DIR, File.basename(REMOTE_VERSION_PATH))
end

def load_local_version
  return nil unless File.exist?(local_version_path)
  JSON.parse(File.read(local_version_path))
rescue => e
  log("Failed to parse local version.json: #{e.message}")
  nil
end

def save_version_file(content)
  File.write(local_version_path, content)
end

def sha256_of_file(path)
  Digest::SHA256.file(path).hexdigest
end

def edition_filename(version_data)
  edition_id = version_data['edition_id'] || version_data['filename'].split('_').first
  "#{edition_id}.mmdb"
end

def atomic_download(url, destination_path)
  temp_path = "#{destination_path}.tmp"

  uri = URI.parse(url)
  Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') do |http|
    request = Net::HTTP::Get.new(uri)
    http.request(request) do |response|
      unless response.is_a?(Net::HTTPSuccess)
        raise "Download failed: #{response.code} #{response.message}"
      end

      File.open(temp_path, 'wb') do |file|
        response.read_body do |chunk|
          file.write(chunk)
        end
      end
    end
  end

  FileUtils.mv(temp_path, destination_path, force: true)
end

def sync_geoip
  version_url = build_url(REMOTE_VERSION_PATH)
  log("Checking remote version: #{version_url}")

  remote_version_content = http_get(version_url)
  remote_version = JSON.parse(remote_version_content)
  local_version = load_local_version

  remote_sha = remote_version['actual_file_sha256']
  local_sha = local_version&.dig('actual_file_sha256')

  if local_sha == remote_sha
    log('GeoIP DB already up to date. No action required.')
    return
  end

  filename = remote_version.fetch('filename')
  mmdb_url = build_url(filename)
  local_mmdb_name = edition_filename(remote_version)
  local_mmdb_path = File.join(LOCAL_DOWNLOAD_DIR, local_mmdb_name)

  log("New version detected. Downloading #{filename}")
  atomic_download(mmdb_url, local_mmdb_path)

  downloaded_sha = sha256_of_file(local_mmdb_path)

  if downloaded_sha != remote_sha
    raise "SHA256 mismatch: expected #{remote_sha}, got #{downloaded_sha}"
  end

  save_version_file(remote_version_content)

  log("Successfully updated #{local_mmdb_name}")
end

log('Starting pp-geoip-sync loop...')

loop do
  begin
    sync_geoip
  rescue => e
    log("Sync failed: #{e.class} - #{e.message}")
    log(e.backtrace.first(5).join("\n"))
  end

  log("Sleeping for #{CHECK_INTERVAL} seconds...")
  sleep(CHECK_INTERVAL)
end
