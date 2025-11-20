#!/usr/bin/env ruby

require './utils'

items = [
  {"Name"=>"TOKEN", "Value"=>"86a35f61-5575-45c7-9e5a-a9f4afaf6306"}, 
  {"Name"=>"SERIAL", "Value"=>"KN0000098"}, 
  {"Name"=>"PIN", "Value"=>"SUM9J4F"}, 
  {"Name"=>"USER_ORG_ID", "Value"=>"napbiotec"}, 
  {"Name"=>"PRODUCT_CODE", "Value"=>"PROD-002"}, 
  {"Name"=>"PRODUCT_TAGS", "Value"=>"ทดสอบ"}, 
  {"Name"=>"PRODUCT_QUANTITY", "Value"=>"1"}, 
  {"Name"=>"WALLET_ID", "Value"=>"0b2e0a83-9e47-4bd4-ac4b-c9b7b977aaa7"}, 
  {"Name"=>"EVENT_TRIGGER", "Value"=>"CustomerRegistered"}
]

value = get_value_by_name(items, "TOKEN")
puts value   # => 40

