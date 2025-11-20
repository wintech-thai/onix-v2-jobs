require 'json'
require "base64"
require 'net/http'
require 'redis'
require 'uri'

def render_report_file(reportObj, templateFile, reportFile)
    template = ERB.new(File.read(templateFile))
    content = template.result(reportObj.get_binding)

    File.write(reportFile, content)
    puts("DEBUG - Report written to [#{reportFile}]")
end

def send_audit_log_etl(jsonStr, endpoint)  
    uri = URI.parse(endpoint)  
  
    http = Net::HTTP.new(uri.host, uri.port)  
    http.use_ssl = (uri.scheme == "https")   # auto เปิด SSL ถ้าเป็น https
  
    request = Net::HTTP::Post.new(uri.request_uri)  
    request["Content-Type"] = "application/json"   # 👈 สำคัญ
    request.body = jsonStr                 
  
    response = http.request(request)
  
    if (response.code != '200')
      puts("ERROR : Failed to send audit log with error [#{response}]")
    end
end

def send_email(emailObj, apiKey, report)  
    uri = URI.parse("https://api.mailgun.net/v3/please-scan.com/messages")  
  
    http = Net::HTTP.new(uri.host, uri.port)  
    http.use_ssl = true
  
    request = Net::HTTP::Post.new(uri.request_uri)  
    request.basic_auth("api", apiKey)
    emailAddr = emailObj['to']

    data =  {
      from: emailObj['from'],
      to: emailAddr,
      bcc: emailObj['bcc'],
      subject: emailObj['subject'],
      text: emailObj['text'],
      html: report
    }
  
    request.set_form_data(data)
    response = http.request(request)
  
    if (response.code != '200')
      puts("ERROR : Failed to send email with error [#{response}]")
    else
      puts("INFO : Sent email to [#{emailAddr}]")
    end
end

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
  #unless prefix.match?(/\A[A-Z]\z/)
  #  raise ArgumentError, "Prefix need to be only one capital letter (A-Z)"
  #end

  # สร้าง format เช่น "%06d" ถ้า digits = 6
  number_format = "%0#{digits}d"

  # คืนค่าเป็น String ที่ prefix + running number
  "#{prefix}#{format(number_format, start)}"
end

def json?(str)
  JSON.parse(str)
  true
rescue JSON::ParserError
  false
end

def make_request(method, apiName, data, endpoint)
  host = endpoint
  apiKey = nil

  uri = URI.parse("#{host}/#{apiName}")  

  # แปลง method เช่น "post" → "Net::HTTP::Post"
  klass_name = "Net::HTTP::#{method.to_s.capitalize}"
  request_class = Object.const_get(klass_name)

  request = request_class.new(uri.request_uri)
  request['Content-Type'] = 'application/json'
  
  if (!apiKey.nil?)
    request.basic_auth("api", apiKey)
    #puts("===== Using API KEY =====")
  end

  if (!data.nil?)
    request.body = data.to_json
  end

  http = Net::HTTP.new(uri.host, uri.port)  
  http.use_ssl = (uri.scheme == "https")

  response = http.request(request)

  if (response.code != '200')
    puts("ERROR : Failed to send request with error [#{response}]")
    return
  end

  result = response.body
  if json?(result)
    result = JSON.parse(result)
  end

  return result
end

def get_value_by_name(items, name)
  item = items.find { |obj| obj['Name'] == name }
  item ? item['Value'] : nil
end

def fallback(param1, param2)
  param1.nil? || param1.strip.empty? ? param2 : param1
end

def update_job_status(conn, jobId, status)
  columnMap = {
    'Submitted' => 'pickup_date',
    'Running' => 'start_date',
    'Succeed' => 'end_date',
    'Failed' => 'end_date',
  }

  columnName = columnMap[status]
#puts("DEBUG --> [#{columnName}]")
  conn.exec_params("
    UPDATE \"Jobs\" SET status = $1, #{columnName} = CURRENT_TIMESTAMP, updated_date = CURRENT_TIMESTAMP
    WHERE job_id = $2", [status, jobId])
end

def update_job_done(conn, jobId, successCnt, failedCnt, message)
  conn.exec_params("
    UPDATE \"Jobs\" SET succeed_cnt = $1, failed_cnt = $2, job_message = $3, status = 'Done', end_date = CURRENT_TIMESTAMP
    WHERE job_id = $4", [successCnt, failedCnt, message, jobId])
end

def getRedisObj
  redis = nil
  begin
    redis = Redis.new(
      :host => ENV["REDIS_HOST"],
      :port => ENV["REDIS_PORT"]
    )

    client_ping = redis.ping
    if (client_ping)
      puts("INFO : Connected to Redis [#{ENV["REDIS_HOST"]}]")
    else
      raise 'Ping failed!!!'
    end
  rescue => e
    puts("ERROR: #{e}")
    redis = nil
  end

  return redis
end

