#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'redis'
require 'json'
require 'net/http'
require 'base64'

require './utils'

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

$stdout.sync = true

def get_pending_pmr(conn)
  sql = <<~SQL
    SELECT org_id, request_id, ref_id1
    FROM "PaymentRequests"
    WHERE direction = 'PayIn'
      AND status = 'Pending'
      AND expire_date IS NOT NULL
      AND expire_date < NOW()
    ORDER BY expire_date ASC
  SQL

  conn.exec(sql)
end

def get_api_key(conn)
  #TODO : ใช้ tag ในการดึง api_key แทนการใช้ key_name
  sql = <<~SQL
    SELECT api_key
    FROM "ApiKeys"
    WHERE key_name = 'payment-cleanup'
      AND key_status = 'Active'
  SQL

  result = conn.exec(sql)
  result.first&.fetch('api_key', nil)
end

def submit_payment_reject_api(row, apiKey, apiBaseUrl)
  requestId = row['request_id']
  #orgId = row['org_id']
  #refId1 = row['ref_id1']

  param = {
    StatusCode: "ERR_EXPIRED_REQUEST",
    StatusReason: "This 'Pending' request has expired and will be automatically rejected by the system!!!",
  }

  apiUrl = "admin-api/AdminPaymentRequest/org/global/action/RejectPendingPayInRequestById/#{requestId}"

  headers = {
    "X-Forward-Mutual-Key" => ENV["MUTUAL_KEY"],
  }

  make_request2(:post, apiUrl, param, apiBaseUrl, apiKey, headers)
end

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']

apiBaseUrl = ENV['API_BASE_URL']

puts("INFO : ### Start payment cleanup jobs.")
puts("INFO : ### ENVIRONMENT=[#{environment}]")
puts("INFO : ### REDIS_HOST=[#{redisHost}]")
puts("INFO : ### REDIS_PORT=[#{redisPort}]")
puts("INFO : ### API_BASE_URL=[#{apiBaseUrl}]")

pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]
conn = connect_db(pgHost, pgDb, ENV["PG_USER"], ENV["PG_PASSWORD"])
if (conn.nil?)
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end
puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")

redis = Redis.new(host: redisHost, port: redisPort)

apiKey = get_api_key(conn)
if apiKey.nil?
  puts("ERROR : ### API key name [payment-cleanup] with status [Active] not found!!!")
  exit 102
end

pendingPmrRows = get_pending_pmr(conn)
pendingPmrRows.each do |row|
  requestId = row['request_id']
  orgId = row['org_id']
  refId1 = row['ref_id1']

  puts("INFO : ### Found expired pending PMR: org_id=[#{orgId}], request_id=[#{requestId}], ref_id1=[#{refId1}]")
  submit_payment_reject_api(row, apiKey, apiBaseUrl)
end
