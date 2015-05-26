require 'json'
require 'htph'

#keep track of what we've used
@@gd_ids_processed = []
@@count = 0

def build_record ids

  doc_id = ids.shift
  @@gd_ids_processed << doc_id
  @@count += 1
  base_rec = JSON.parse(get_source_rec( doc_id ))
  rec_copy = base_rec.clone #silly for singletons, makes sense for large dupe clusters
  base_rec['source_records'] = {doc_id=>rec_copy}
  
  
  #get the enumchron from the database
  base_rec['enumchron_display'] = get_enumchron(doc_id)

  #only duplicate clusters need to worry about this
  ids.each do | id | 
    base_rec['source_records'][id] = get_source_rec( id )
    @@gd_ids_processed << id 
  end

  base_rec['relationships'] = get_relationships( doc_id )

  return base_rec

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
                             WHERE govdoc_id = ? AND score < 1.0"
  @@conn.prepared_select(@@get_relationships_sql, [doc_id]) do |row|
    rels << row.get_object('cluster_id')
  end
  return rels
end
         

def get_source_rec( doc_id )
  @@get_rec_sql = "SELECT hf.file_path, hg.lineno FROM hathi_gd hg 
                    LEFT JOIN hathi_input_file hf ON hg.file_id = hf.id
                   WHERE hg.id = ? LIMIT 1"
  line = '' 
  @@conn.prepared_select(@@get_rec_sql, [doc_id]) do | row | #should just be one, unless I did something stupid
    fname = row.get_object('file_path')
    fname.sub!(/\.gz$/, '')
    lineno = row.get_object('lineno').to_i + 1 #line numbers seem to be off by 1
  
    line = `awk 'NR=#{lineno}{print;exit}' #{fname}`
    line = line.split("\n")[0].chomp
  end
  if line == '' then line = '["source_missing"]' end
  return line 
end



@@jdbc = HTPH::Hathijdbc::Jdbc.new();
@@conn = @@jdbc.get_conn();

finname = ARGV.shift
cluster_report = open(finname)

foutname = ARGV.shift
outfile = open(foutname, 'w')

start = Time.now

cluster_report.each_with_index do | line, line_num |
  parts = line.split(/\t/)

  if parts[0] == 'duplicates' and parts[1].to_f >= 1.0
    score = parts[1]
    ids = parts[2].split(',')
    #puts build_record(ids).to_json
    outfile.puts build_record(ids).to_json
  else
    if parts[0] == 'duplicates' 
      ids = parts[2].split(',')
    else
      ids = parts[1].split(',')
    end
    ids.each { |id| outfile.puts build_record([id]).to_json }
  end

  if @@count > 20000
    break
  end

  
end

duration = Time.now - start
puts 'processed: '+@@gd_ids_processed.count.to_s
puts 'duration: '+duration.to_s

