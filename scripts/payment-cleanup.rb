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

def get_pending_pmr(conn)
  sql = <<~SQL
    SELECT org_id, request_id
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
  sql = <<~SQL
    SELECT api_key
    FROM "ApiKeys"
    WHERE key_name = 'payment-cleanup'
      AND key_status = 'Active'
  SQL

  result = conn.exec(sql)
  result.first&.fetch('api_key', nil)
end

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']

apiEndpoint = ENV['API_ENDPOINT']

puts("INFO : ### Start payment cleanup jobs.")
puts("INFO : ### ENVIRONMENT=[#{environment}]")
puts("INFO : ### REDIS_HOST=[#{redisHost}]")
puts("INFO : ### REDIS_PORT=[#{redisPort}]")
puts("INFO : ### API_ENDPOINT=[#{apiEndpoint}]")

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
  puts("INFO : ### Found expired pending PMR: org_id=[#{row['org_id']}], request_id=[#{row['request_id']}]")
end
