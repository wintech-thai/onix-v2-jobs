#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'csv'
require 'erb'
require './utils'
require "google/cloud/storage"

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

$stdout.sync = true

dataSections = [ 
  { table: "ScanItemActions", 
    keyfields: [ "org_id" ], 
    valueFields: [ "encryption_key", "encryption_iv" ]
  },
]

selectedSection = fallback(ENV['DATA_SECTION'], 'ALL')
environment = ENV['ENVIRONMENT']
redisHost = ENV['REDIS_HOST']
redisPort = ENV['REDIS_PORT']
orgId = fallback(ENV['ORG_ID'], 'default') 

jobId = fallback(ENV['JOB_ID'], '')

puts("INFO : ### ENVIRONMENT=[#{environment}]")
puts("INFO : ### REDIS_HOST=[#{redisHost}]")
puts("INFO : ### REDIS_PORT=[#{redisPort}]")
puts("INFO : ### ORG_ID=[#{orgId}]")

puts("INFO : ### Start loading data to Redis...")
puts("INFO : ### JOB_ID=[#{jobId}]")

def get_record_set(conn, section)
  table = section[:table]
  orgId = fallback(ENV['ORG_ID'], 'default') 

  keyfields = section[:keyfields]
  valueFields = section[:valueFields]
  selectedFields = keyfields + valueFields

  selectedColumn = selectedFields.join(", ")
  sql = "SELECT #{selectedColumn} FROM \"#{table}\" WHERE org_id = '#{orgId}'"
  #puts(sql)

  rs = conn.exec(sql)
  return rs
end

def get_cache_subkey(row, names)
  arr = []
  names.each do |field|
    value = row[field]
    arr << value
  end

  subkey = arr.join(":")
  return subkey
end

def get_cache_value(row, names)
  obj = {}

  names.each do |field|
    value = row[field]
    obj[field] = value
  end

  json = obj.to_json
  return json
end

def load_cache(rs, section, env, redisObj, ttlSec)
  table = section[:table]

  keyfields = section[:keyfields]
  valueFields = section[:valueFields]
  selectedFields = keyfields + valueFields

  cacheKeyPrefix = "CacheLoader:#{env}:#{table}"
  cnt = 0
  rs.each do |row|
    cacheSubKey = get_cache_subkey(row, keyfields)

    cacheKey = "#{cacheKeyPrefix}:#{cacheSubKey}"
    cacheValue = get_cache_value(row, valueFields)

    redisObj.setex(cacheKey, ttlSec, cacheValue)
    puts("@@@@ [#{cacheKey}] => [#{cacheValue}]")

    cnt = cnt + 1
  end

  return cnt
end

pgHost = ENV["PG_HOST"]
pgDb = ENV["PG_DB"]
conn = connect_db(pgHost, pgDb, ENV["PG_USER"], ENV["PG_PASSWORD"])
if (conn.nil?)
  puts("ERROR : ### Unable to connect to PostgreSQL --> Host=[#{pgHost}], DB=[#{pgDb}] !!!")
  exit 101
end
puts("INFO : ### Connected to PostgreSQL [#{pgHost}] [#{pgDb}]")

redisObj = getRedisObj()

update_job_status(conn, jobId, 'Running') unless jobId == ""

### Loop
total = 0
dataSections.each do |section|
  table = section[:table]
  if ((selectedSection == 'ALL') || (selectedSection == table))
    puts("INFO : ### Loading data section [#{table}]...")

    rs = get_record_set(conn, section)
    # 1 day TTL
    cnt = load_cache(rs, section, environment, redisObj, (1 * 24 * 60 * 60))
    total = total + cnt
  end
end

message = "Done loading data [#{total}] rows to Redis" 
update_job_done(conn, jobId, 1, 0, message) unless jobId == ""

puts("INFO : ### #{message}")
