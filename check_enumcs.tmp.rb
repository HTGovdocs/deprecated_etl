#are there 1.0 duplicate clusters with govdocs missing oclc numbers?
require 'htph'
@db = HTPH::Hathidb::Db.new();
@conn = @db.get_conn();
@get_enumc = "SELECT count(DISTINCT str_id) as c from hathi_enumc where gd_id IN(?)"
cluster_gd_ids = open(ARGV.shift)

cluster_gd_ids.each do | line |
  line.chomp!
  @conn.query(@get_enumc.gsub(/\?/, line.split(/\t/)[2])) do | row |
    count = row[:c]
    if count.to_i > 1
      puts line
    end
  end
end
