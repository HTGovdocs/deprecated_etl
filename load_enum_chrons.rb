require 'json'
require 'htph'

defs = JSON.parse(open('source_defs.json', 'r').read)

@@jdbc = HTPH::Hathijdbc::Jdbc.new();
@@conn = @@jdbc.get_conn();

@@add_ec_sql = "INSERT INTO enum_chrons 
                  (input_file_name, line_number, chronology, enumeration)
                VALUES(?,?,?,?)"

fin = open(ARGV.shift)

fin.each do | line | 
  fields = line.split("\t")
  @@conn.prepared_update(@@add_ec_sql,
                          [fields[0],
                          fields[1].to_i - 1, #elsewhere, line num starts at 0
                          fields[2],
                          fields[3]])
end
