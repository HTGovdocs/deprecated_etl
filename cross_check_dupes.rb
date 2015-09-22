#are there 1.0 duplicate clusters with govdocs missing oclc numbers?
require 'htph'
@jdbc = HTPH::Hathijdbc::Jdbc.new();
@conn = @jdbc.get_conn();
@get_oclc = "SELECT count(*) as c from hathi_oclc where gd_id IN(?)"
cluster_gd_ids = open(ARGV.shift)



cluster_gd_ids.each do | line |
  line.chomp!
  sql = @get_oclc.gsub('?', line.split(/\t/)[2])
  @conn.prepared_select(sql) do | row |
    count = row.get_object('c')
    if count.to_i == 0
      puts line
    end
  end
end
