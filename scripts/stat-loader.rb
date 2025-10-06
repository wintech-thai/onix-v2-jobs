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

def update_stat(conn, statData, statCodeDaily, statCodeCurrent)
  puts("")
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

  statData.each do |org_id, total|
    params = [
      org_id,
      statCodeCurrent,
      balanceKeyCurrent,
      0,
      total
    ]

    puts "#{statCodeCurrent} --> OrgID=[#{org_id}], BalanceKey:Total=>[#{balanceKeyCurrent}][#{total}]"
    conn.exec_params(sql, params)

    params = [
      org_id,
      statCodeDaily,
      balanceKey,
      total,
      total
    ]

    puts "#{statCodeDaily} --> OrgID=[#{org_id}], BalanceKey:Total=>[#{balanceKey}][#{total}]"
    conn.exec_params(sql, params)
  end
end

def get_customer_aggregate(conn)
  sql = <<-SQL
    SELECT org_id, COUNT(*) AS customer_count
    FROM "Entities"
    WHERE entity_type = 1
    GROUP BY org_id;
  SQL

  result = conn.exec(sql)

  # แปลงผลลัพธ์ให้อยู่ในรูป Hash เช่น { "org1" => 100, "org2" => 50 }
  aggregate = {}
  result.each do |row|
    aggregate[row['org_id']] = row['customer_count'].to_i
  end

  return aggregate
end

def get_scan_item_aggregate(conn)
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

def get_product_aggregate(conn)
  sql = <<-SQL
    SELECT org_id, COUNT(*) AS product_count
    FROM "Items"
    GROUP BY org_id;
  SQL

  result = conn.exec(sql)

  # แปลงผลลัพธ์ให้อยู่ในรูป Hash เช่น { "org1" => 100, "org2" => 50 }
  aggregate = {}
  result.each do |row|
    aggregate[row['org_id']] = row['product_count'].to_i
  end

  return aggregate
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

statDataScanItem = get_scan_item_aggregate(conn)
update_stat(conn, statDataScanItem, "ScanItemBalanceDaily  ", "ScanItemBalanceCurrent")

statDataCustomer = get_customer_aggregate(conn)
update_stat(conn, statDataCustomer, "CustomerBalanceDaily  ", "CustomerBalanceCurrent")

statDataProduct = get_product_aggregate(conn)
update_stat(conn, statDataProduct,  "ProductBalanceDaily   ", "ProductBalanceCurrent ")
