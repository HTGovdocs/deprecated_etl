require 'json'
require 'htph'
require 'marc'
require 'httpclient'
require 'traject'
require 'traject/indexer/settings'
require 'pp'

#keep track of what we've used
@@count = 0

#minimum score to be considered a dupe
@@dupe_cutoff = 0.5 

@@client = HTTPClient.new
@@solr_update_url = 'http://solr-sdr-usfeddocs-dev:9035/usfeddocs/collection1/update?wt=json'

@@solr_source_url = 'http://solr-sdr-usfeddocs-dev:9034/usfeddocs/raw_source/select?wt=json&q='
@@indexer = Traject::Indexer.new
@@indexer.load_config_file('traject_config.rb')

@@get_rec_sql = "SELECT hf.file_path, hg.lineno FROM hathi_gd hg 
                  LEFT JOIN hathi_input_file hf ON hg.file_id = hf.id
                 WHERE hg.id = ? LIMIT 1"
@@set_etld_sql = "INSERT INTO etld_govdocs (govdoc_id) VALUES(?)"
@@get_enumchron_sql = "SELECT * FROM hathi_enumc 
                        LEFT JOIN hathi_str hs ON hathi_enumc.str_id = hs.id  
                       WHERE gd_id = ? LIMIT 1"
@@get_relationships_sql = "SELECT * FROM tmp_relationships
                           WHERE govdoc_id = ? AND (relationship != 'duplicates' OR score < ?)"

def build_record ids
  doc_id = ids.shift
  set_processed( doc_id )
  @@count += 1
  source, src_file = get_source_rec( doc_id )
  base_marc = MARC::Record.new_from_hash(JSON.parse(source))

   
  rec = @@indexer.map_record(base_marc)

  rec['id'] = doc_id

  rec['source_records'] = [source]
 
  #get the enumchron from the database
  rec['enumchron_display'] = get_enumchron(doc_id)

  rec['ht_ids'] = []
  if src_file =~ /zeph/ and source =~ /"r":"pd"/
    ht_id = get_ht_id(source)
    unless rec['ht_ids'].include? ht_id 
      rec['ht_ids'] << ht_id 
    end
  end

  #only duplicate clusters need to worry about this
  ids.each do | id | 
    src, src_file = get_source_rec( id )
    rec['source_records'] << src 
    if src_file =~ /zeph/ and source =~ /.r.:.pd./
      ht_id = get_ht_id(source)
      unless rec['ht_ids'].include? ht_id 
        rec['ht_ids'] << ht_id 
      end
    end
  end

  rec['relationships'] = get_relationships( doc_id )

  #PP.pp(rec, STDERR)
  return rec


end

def set_processed( doc_id )
  @@conn.prepared_update(@@set_etld_sql, [doc_id])
end

#we need a list of already processed govdocs
#Just the ids so keep them in memory
def get_processed( )
  processed = {}
  @@get_etld_sql = "SELECT * FROM etld_govdocs"
  @@conn.prepared_select(@@get_etld_sql, []) do | row |
    processed[row.get_object('govdoc_id')] = 1 
  end
  return processed
end 

def get_ht_id( rec )
  id = /"001":"(\d+)"/.match(rec)[1]
  puts "ht_id: #{id}"
  return id
rescue
  PP.pp rec
  raise
end
  
def get_enumchron( doc_id )
  enumchron = ''
  @@conn.prepared_select(@@get_enumchron_sql, [doc_id]) do | row |
    enumchron = row.get_object('str')
  end
  return enumchron
end

def get_relationships( doc_id )
  rels = []
  @@conn.prepared_select(@@get_relationships_sql, [doc_id, @@dupe_cutoff]) do |row|
    rels << row.get_object('cluster_id')
  end
  return rels
end
         

def get_source_rec( doc_id )
  line = '' 
  fname = ''
  @@conn.prepared_select(@@get_rec_sql, [doc_id]) do | row | #should just be one, unless I did something stupid
    fname = row.get_object('file_path')
    fname = fname.sub(/\.gz$/, '').split('/').pop
    lineno = row.get_object('lineno').to_i
    s_id = "#{fname}_#{lineno}"

    resp = @@client.get @@solr_source_url+s_id
    line = JSON.parse(resp.body)['response']['docs'][0]['text'][0]

    #this stuff was way too slow
    #line = `awk 'NR==#{lineno}{print;exit}' #{fname}`
    #line = line.split("\n")[0].chomp
  end
  if line == '' || fname == ''
    STDERR.puts "doc_id: #{doc_id}"
  end
  return line.chomp!, fname
end

def solr_index rec
  #PP.pp rec
  rec['marc_display'] = rec['source_records'][0]
  resp = @@client.post @@solr_update_url, [rec].to_json, "content-type"=>"application/json"
  #pp.pp resp
end



@@jdbc = HTPH::Hathijdbc::Jdbc.new();
@@conn = @@jdbc.get_conn();

finname = ARGV.shift
cluster_report = open(finname)

#foutname = argv.shift
#outfile = open(foutname, 'w')

start = Time.now

processed = get_processed()

cluster_report.each_with_index do | line, line_num |
  
  parts = line.chomp.split(/\t/)
  if parts[0] == 'duplicates' and parts[1].to_f >= @@dupe_cutoff 
    score = parts[1]
    ids = parts[2].split(',')
    #puts build_record(ids).to_json
    next if processed.has_key? ids[0] 
    solr_index build_record(ids)
  else
    if parts[0] == 'duplicates' 
      ids = parts[2].split(',')
    else
      ids = parts[1].split(',')
    end
    ids.each do |id| 
      if !processed.has_key? id 
        solr_index build_record([id])  
      end
    end
  end

  if line_num % 10000 == 0 || @@count % 1000 == 0
    puts "Line number: #{line_num}, count: #{@@count}"
  end
  #if @@count % 1000 == 1
  #  puts @@count
  #end

end

duration = Time.now - start
puts 'processed: '+@@count.to_s
puts 'duration: '+duration.to_s

