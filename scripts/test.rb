#!/usr/bin/env ruby

require './utils'

items = [
  { "Name": "CPU", "Value": 90 },
  { Name: "RAM", Value: 40 },
  { "Name": "Disk", "Value": 70 }
]

value = get_value_by_name(items, "RAM")
puts value   # => 40

