require 'htph'

@@jdbc = HTPH::Hathijdbc::Jdbc.new();
@@conn = @@jdbc.get_conn();

@@add_dupe_sql = "INSERT INTO tmp_relationships 
                  (cluster_id, relationship, govdoc_id, score, file_name, line_number)
                VALUES(?,?,?,?,?,?)"


fname = ARGV.shift
fin = open(fname)

fin.each_with_index do |line, line_num| 
  cluster_id = SecureRandom.uuid 
  parts = line.split(/\t/)
  if parts[0] == 'duplicates'
    score = parts[1] 
    ids = parts[2].split(',')
  else
    score = 0.0
    ids = parts[1].split(',')
  end
  ids.each do | gd_id | 
    @@conn.prepared_update(@@add_dupe_sql, [cluster_id, parts[0], gd_id, score, fname, line_num])   
  end
end

