#are there solos missing oclc numbers?
require 'htph'
@jdbc = HTPH::Hathijdbc::Jdbc.new();
@conn = @jdbc.get_conn();
@get_oclc = "SELECT count(*) as c from hathi_oclc where gd_id = ?"
cluster_gd_ids = open(ARGV.shift)



cluster_gd_ids.each do | line |
  line.chomp!
  @conn.prepared_select(@get_oclc, [line.split(/\t/)[1]]) do | row |
    count = row.get_object('c')
    if count.to_i == 0
      puts line
    end
  end
end
