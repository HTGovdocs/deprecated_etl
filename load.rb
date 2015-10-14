#chunk the input file into groups and send it for indexing
require 'httpclient'
require 'pp'

recs = open(ARGV.shift)
log = open('load.log','w')
chunk_size = 10
solr_url = 'http://solr-sdr-usfeddocs-dev:9035/usfeddocs/update/json?commit=true'
rec_set = [] 
chunk = ''
client = HTTPClient.new

recs.each_with_index do | rec, rec_num |
  rec.chomp!
  rec_set << rec
  if (rec_num+1) % chunk_size == 0 
    chunk = '['+rec_set.join(',')+']'
    begin
      resp = client.post solr_url, chunk, "content-type"=>"application/json" 
    rescue
      sleep(2)
      retry
    end
    log.puts rec_num
    chunk = ''
    rec_set = []
  end
end

#anything left? send it
if rec_set.count > 0
  chunk = '['+rec_set.join(',')+']'
  resp = client.post solr_url, chunk, "content-type"=>"application/json" 
end
  
