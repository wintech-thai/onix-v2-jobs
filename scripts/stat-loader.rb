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

def get_scan_item_aggregate(conn)
  # SELECT COUNT(*) FROM "ScanItems" GROUP BY org_id 

  sql = <<-SQL
    SELECT org_id, COUNT(*) AS scan_item_count
    FROM "ScanItems"
    GROUP BY org_id;
  SQL

  result = conn.exec(sql)

  # แปลงผลลัพธ์ให้อยู่ในรูป Hash เช่น { "org1" => 100, "org2" => 50 }
  aggregate = {}
  result.each do |row|
    aggregate[row['org_id']] = row['scan_item_count'].to_i
  end

  return aggregate
end

def update_scan_item_stat(conn, statData)
  balanceKey = Time.now.strftime("%Y%m%d")
  balanceKeyCurrent = '00000000'

  sql = <<-SQL
    INSERT INTO "Stats" (
      stat_id,
      org_id,
      stat_code,
      balance_date,
      balance_date_key,
      balance_begin,
      balance_end,
      created_date
    )
    VALUES (
      gen_random_uuid(), $1, $2, CURRENT_TIMESTAMP, $3, $4, $5, CURRENT_TIMESTAMP
    )
    ON CONFLICT (org_id, stat_code, balance_date_key)
    DO UPDATE SET
      balance_end = $5;
  SQL

  #ScanItemBalanceDaily, ScanItemBalanceCurrent
  statData.each do |org_id, total|
    puts "Org ID: [#{org_id}] => Total: [#{total}]"

    params = [
      org_id,
      'ScanItemBalanceCurrent',
      balanceKeyCurrent,
      0,
      total
    ]
    conn.exec_params(sql, params)

    params = [
      org_id,
      'ScanItemBalanceDaily',
      balanceKey,
      total,
      total
    ]
    conn.exec_params(sql, params)
  end
end

environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']

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

statData = get_scan_item_aggregate(conn)
update_scan_item_stat(conn, statData)
