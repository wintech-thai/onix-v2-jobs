require 'json'
require "base64"
require 'net/http'
require 'redis'

def random_char()
  char = (65 + SecureRandom.random_number(26)).chr 
  return char
end

def random_string(length = 7)
  chars = ('A'..'Z').to_a + ('0'..'9').to_a
  Array.new(length) { chars.sample }.join
end

def connect_db(host, db, user, password)
  begin
      con = PG.connect(:host => host, 
          :dbname => db, 
          :user => user, 
          :password => password)

  rescue PG::Error => e
      puts("ERROR - Connect to DB [#{e.message}]")
  end

  return con
end

def escape_char(str)
  return "#{str}".tr("'", "")
end

def generate_serial(prefix:, digits:, start:)
  # ตรวจสอบว่า prefix เป็นตัวอักษรใหญ่ตัวเดียว
  unless prefix.match?(/\A[A-Z]\z/)
    raise ArgumentError, "Prefix need to be only one capital letter (A-Z)"
  end

  # สร้าง format เช่น "%06d" ถ้า digits = 6
  number_format = "%0#{digits}d"

  # คืนค่าเป็น String ที่ prefix + running number
  "#{prefix}#{format(number_format, start)}"
end

def fallback(param1, param2)
  param1.nil? || param1.strip.empty? ? param2 : param1
end
