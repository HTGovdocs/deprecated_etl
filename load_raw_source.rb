require 'json'
require 'httpclient'
require 'htph'

@jdbc = HTPH::Hathijdbc::Jdbc.new()
@conn = @jdbc.get_conn()
@add_rec = "INSERT INTO gd_source_recs
              (file_path, line_number, source)
            VALUES(?,?,?)"

@count = 0

start = Time.now
count = 0

sources = JSON.parse(open(ARGV.shift, 'r').read)

sources.keys.each do | fname_path |

  fin = open(fname_path)

  fin.each_with_index do |line, line_num|
    count += 1
    @conn.prepared_update(@add_rec, [fname_path, line_num, line])
  end
end
duration = Time.now - start
puts 'processed: '+count.to_s
puts 'duration: '+duration.to_s
          
