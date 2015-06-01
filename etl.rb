require 'json'
require 'htph'
require 'marc'
require 'httpclient'
require 'traject'
require 'traject/indexer/settings'

#keep track of what we've used
@@gd_ids_processed = []
@@count = 0

#minimum score to be considered a dupe
@@dupe_cutoff = 0.5 

@@client = HTTPClient.new
@@solr_update_url = 'http://solr-sdr-usfeddocs-dev:9034/usfeddocs/collection1/update?wt=json'

def build_record ids

  doc_id = ids.shift
  @@gd_ids_processed << doc_id
  @@count += 1
  source, src_file = get_source_rec( doc_id )
  base_marc = MARC::Record.new_from_hash(JSON.parse(source))

  @@indexer = Traject::Indexer.new
  @@indexer.load_config_file('traject_config.rb')
 
  rec = @@indexer.map_record(base_marc)

  rec['id'] = doc_id

  rec['source_records'] = {doc_id=>source}
 
  #get the enumchron from the database
  rec['enumchron_display'] = get_enumchron(doc_id)

  rec['ht_ids'] = []
  if src_file =~ /zeph/
    rec['ht_ids'] << get_ht_id(source)
  end

  #only duplicate clusters need to worry about this
  ids.each do | id | 
    src, src_file = get_source_rec( id )
    rec['source_records'][id] = src 
    if src_file =~ /zeph/
      rec['ht_ids'] << get_ht_id(src)
    end
    @@gd_ids_processed << id 
  end

  rec['relationships'] = get_relationships( doc_id )

  #PP.pp(rec, STDERR)
  return rec


end

def get_ht_id( rec )
  id = /"001":"(\d+)"/.match(rec)[1]
  return id
rescue
  PP.pp rec
  raise
end
  
def get_enumchron( doc_id )
  enumchron = ''
  @@get_enumchron_sql = "SELECT * FROM hathi_enumc 
                          LEFT JOIN hathi_str hs ON hathi_enumc.str_id = hs.id  
                         WHERE gd_id = ? LIMIT 1"
  @@conn.prepared_select(@@get_enumchron_sql, [doc_id]) do | row |
    enumchron = row.get_object('str')
  end
  return enumchron
end

def get_relationships( doc_id )
  rels = []
  @@get_relationships_sql = "SELECT * FROM tmp_relationships
                             WHERE govdoc_id = ? AND (relationship != 'duplicates' OR score < ?)"
  @@conn.prepared_select(@@get_relationships_sql, [doc_id, @@dupe_cutoff]) do |row|
    rels << row.get_object('cluster_id')
  end
  return rels
end
         

def get_source_rec( doc_id )
  @@get_rec_sql = "SELECT hf.file_path, hg.lineno FROM hathi_gd hg 
                    LEFT JOIN hathi_input_file hf ON hg.file_id = hf.id
                   WHERE hg.id = ? LIMIT 1"
  line = '' 
  fname = ''
  @@conn.prepared_select(@@get_rec_sql, [doc_id]) do | row | #should just be one, unless I did something stupid
    fname = row.get_object('file_path')
    fname.sub!(/\.gz$/, '')
    lineno = row.get_object('lineno').to_i + 1 #line numbers seem to be off by 1
  
    line = `awk 'NR==#{lineno}{print;exit}' #{fname}`
    line = line.split("\n")[0].chomp
  end
  if line == '' || fname == ''
    STDERR.puts "doc_id: #{doc_id}"
  end
  return line, fname
end

def solr_index rec
  resp = @@client.post @@solr_update_url, [rec].to_json, "Content-Type"=>"application/json"
  #PP.pp resp.status
end



@@jdbc = HTPH::Hathijdbc::Jdbc.new();
@@conn = @@jdbc.get_conn();

finname = ARGV.shift
cluster_report = open(finname)

#foutname = ARGV.shift
#outfile = open(foutname, 'w')

start = Time.now

cluster_report.each_with_index do | line, line_num |
  parts = line.chomp.split(/\t/)
  #PP.pp parts
  puts parts[1]
  if parts[0] == 'duplicates' and parts[1].to_f >= @@dupe_cutoff 
    score = parts[1]
    ids = parts[2].split(',')
    #puts build_record(ids).to_json
    solr_index build_record(ids)
  else
    if parts[0] == 'duplicates' 
      ids = parts[2].split(',')
    else
      ids = parts[1].split(',')
    end
    ids.each { |id| solr_index build_record([id]) }
  end

  if @@count >= 2500 
    break
  end

  
end

duration = Time.now - start
puts 'processed: '+@@gd_ids_processed.count.to_s
puts 'duration: '+duration.to_s

