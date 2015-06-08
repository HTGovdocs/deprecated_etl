require 'json'

#!!!!!!!
#deprecated. Not used. Just using the table in ht_repository. 
defs = JSON.parse(open('source_defs.json', 'r').read)

source_file = ARGV.shift

#ARGV.each do | fin |
defs.keys.each do | fin |
  next if source_file and source_file != fin
 
  f = open(fin, 'r')
  fdef = defs[fin]
  lineno = 0
  f.each do |l| 
    lineno += 1
    rec = JSON.parse(l)
    fdef['items'].each do | item |
      #find the item
      rec['fields'].each do | f | 
        next unless f[fdef['items'][0]] #for now assume only 1 item field
        next if f[fdef['items'][0]]["ind1"] == "1"  #typically means its unformatted
        #have the right field
        chron = ''
        enum = ''
        f[fdef['items'][0]]['subfields'].each do | subfield |
          if fdef['chronology'].include? subfield.keys[0] and chron == ''
            chron = subfield[subfield.keys[0]] 
          end

          if fdef['enumeration'].include? subfield.keys[0] and enum == ''
            enum = subfield[subfield.keys[0]]
          end
        end 
        puts "#{fin}\t#{lineno}\t#{chron}\t#{enum}"
      end
    end
  end
end
