require 'json'
require 'httpclient'

#keep track of what we've used
@@gd_ids_processed = []
@@count = 0


@@client = HTTPClient.new
@@solr_update_url = 'http://solr-sdr-usfeddocs-dev:9034/usfeddocs/raw_source/update?wt=json'

fname_path = ARGV.shift
fname = fname_path.split('/').pop

fin = open(fname_path)

start = Time.now
count = 0
fin.each_with_index do |line, line_num|
  count += 1

  doc = {
          :id=>"#{fname}_#{line_num}", 
          :file_name=>fname,
          :line_number=>line_num,
          :text=>line
        }
  resp = @@client.post @@solr_update_url, [doc].to_json, "Content-Type"=>"application/json"
end
duration = Time.now - start
puts 'processed: '+count.to_s
puts 'duration: '+duration.to_s
          
