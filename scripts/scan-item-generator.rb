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

puts("INFO : ### Done generating [#{totalItem}] items.")
