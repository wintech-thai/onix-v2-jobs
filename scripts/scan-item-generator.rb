#!/usr/bin/env ruby

require 'pg'
require 'time'
require 'uri'
require 'csv'

if File.exist?('env.rb')
  #Default environment variables
  require './env'
end

$stdout.sync = true

totalItem = ENV['SCAN_ITEM_COUNT']
scanItemUrl = ENV['SCAN_ITEM_URL']
scanItemBucket = ENV['SCAN_ITEM_BUCKET']
scanItemOrg = ENV['SCAN_ITEM_ORG'] 

puts("INFO : ### Start generating scan-items.")

puts("INFO : ### SCAN_ITEM_COUNT=[#{totalItem}]")
puts("INFO : ### SCAN_ITEM_URL=[#{scanItemUrl}]")
puts("INFO : ### SCAN_ITEM_BUCKET=[#{scanItemBucket}]")
puts("INFO : ### SCAN_ITEM_ORG=[#{scanItemOrg}]")

puts("INFO : ### Done generating [#{totalItem}] items.")
