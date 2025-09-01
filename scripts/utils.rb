require 'json'
require "base64"
require 'net/http'
require 'redis'

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
