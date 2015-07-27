require 'json'
require 'htph'
require 'marc'
require 'httpclient'
require 'traject'
require 'traject/indexer/settings'
require 'pp'
require 'securerandom'

class GDIndexer

  def initialize()
    @client = HTTPClient.new
    @solr_update_url = 'http://solr-sdr-usfeddocs-dev:9035/usfeddocs/collection1/update?wt=json'

    @solr_source_url = 'http://solr-sdr-usfeddocs-dev:9034/usfeddocs/raw_source/select?wt=json&q='
    @extractor = Traject::Indexer.new
    @extractor.load_config_file('traject_config.rb')

    @get_rec_sql = "SELECT hf.file_path, hg.lineno FROM hathi_gd hg 
                      LEFT JOIN hathi_input_file hf ON hg.file_id = hf.id
                     WHERE hg.id = ? LIMIT 1"
    @get_enumchron_sql = "SELECT * FROM hathi_str WHERE id = ? LIMIT 1"

    @id_log = open("ids.log.tmp","w")

    @jdbc = HTPH::Hathijdbc::Jdbc.new();
    @conn = @jdbc.get_conn();
  end


  def build_record ids, o_str_id, e_str_id
    doc_id = ids.shift
    
    source, src_file = self.get_source_rec( doc_id )
    rec = {}

    rec['source_records'] = [source]
   
    #get the enumchron from the database
    if e_str_id 
      rec['enumchron_display'] = self.get_enumchron(e_str_id)
    else
      rec['enumchron_display'] = ''
    end

    rec['ht_ids_fv'] = []
    rec['ht_ids_lv'] = []
    #in HT and full view, public domain
    if src_file =~ /zeph/ and source =~ /.r.:.pd./
      ht_id = self.get_ht_id(source)
      rec['ht_availability'] = 'full view'
      unless rec['ht_ids_fv'].include? ht_id 
        rec['ht_ids_fv'] << ht_id 
      end
    #in HT but restricted
    elsif src_file =~ /zeph/
      ht_id = self.get_ht_id(source)
      rec['ht_availability'] = 'limited view' unless rec['ht_availability'] == 'full view'
      unless rec['ht_ids_lv'].include? ht_id 
        rec['ht_ids_lv'] << ht_id 
      end
    end

    #adding additional source records to cluster
    #only duplicate clusters need to worry about this
    #we could ignore this if not for the ht aspect
    ids.each do | id | 
      src, src_file = self.get_source_rec( id )
      rec['source_records'] << src 
      #in HT and full view, public domain
      if src_file =~ /zeph/ and src =~ /.r.:.pd./
        ht_id = self.get_ht_id(src)
        rec['ht_availability'] = 'full view'
        unless rec['ht_ids_fv'].include? ht_id 
          rec['ht_ids_fv'] << ht_id 
        end
      #in HT but restricted
      elsif src_file =~ /zeph/
        ht_id = self.get_ht_id(src)
        rec['ht_availability'] = 'limited view' unless rec['ht_availability'] == 'full view'
        unless rec['ht_ids_lv'].include? ht_id 
          rec['ht_ids_lv'] << ht_id 
        end
      end

      #we prefer a HT record for the indexed display fields
      if src_file =~ /zeph/ and !rec.has_key? 'text'
        base_marc = MARC::Record.new_from_hash(JSON.parse(src))
        rec.merge!(@extractor.map_record(base_marc))
      end
    end

    #we havent trajected into display fields, so we'll use the first
    if !rec.has_key? 'text' 
      base_marc = MARC::Record.new_from_hash(JSON.parse(source))
      rec.merge!(@extractor.map_record(base_marc))
    end

    rec['marc_display'] = rec['source_records'][0]
    ids.unshift( doc_id )
    rec['gd_ids'] = ids
    rec['id'] = SecureRandom.uuid()
    #@id_log.puts rec['id']

    return rec
  end

  def get_ht_id( rec )
    id = /"001":"(\d+)"/.match(rec)[1]
    puts "ht_id: #{id}"
    return id
  rescue
    PP.pp rec
    raise
  end
    
  def get_enumchron( e_str_id )
    enumchron = ''
    @conn.prepared_select(@get_enumchron_sql, [e_str_id]) do | row |
      enumchron = row.get_object('str')
    end
    return enumchron
  end

  def get_source_rec( doc_id )
    line = '' 
    fname = ''
    @conn.prepared_select(@get_rec_sql, [doc_id]) do | row | #should just be one, unless I did something stupid
      fname = row.get_object('file_path')
      fname = fname.sub(/\.gz$/, '').split('/').pop
      lineno = row.get_object('lineno').to_i
      s_id = "#{fname}_#{lineno}"

      resp = @client.get @solr_source_url+s_id
      line = JSON.parse(resp.body)['response']['docs'][0]['text'][0]

    end
    if line == '' || fname == ''
      STDERR.puts "doc_id: #{doc_id}"
    end
    return line.chomp!, fname
  end

  def solr_index ids, o_str_id, e_str_id, line_num
    #PP.pp rec
    puts "Line number: #{line_num}"
    rec = self.build_record(ids, o_str_id, e_str_id) 
    resp = @client.post @solr_update_url, [rec].to_json, "content-type"=>"application/json"
    #todo: deal with error "400"
  end

end #class GDIndexer

finname = ARGV.shift
cluster_report = open(finname)

#foutname = argv.shift
#outfile = open(foutname, 'w')

start = Time.now

indexer = GDIndexer.new()

cluster_report.each_with_index do | line, line_num |
   
  o_str_id, e_str_id, gd_ids = line.chomp.split(/\t/)
  if e_str_id == 'NULL'
    e_str_id = false
  end
  ids = gd_ids.split(',') 

  indexer.solr_index( ids, o_str_id, e_str_id, line_num)  

end

duration = Time.now - start
puts 'duration: '+duration.to_s

