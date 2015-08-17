require 'json'
require 'htph'
require 'marc'
require 'httpclient'
require 'traject'
require 'traject/indexer/settings'
require 'pp'
require 'securerandom'

@@index_timer = 0


class Collator
  attr_accessor :source_rec_timer, :marc_hash_timer
  
  def initialize()
    @client = HTTPClient.new
    @solr_update_url = 'http://solr-sdr-usfeddocs-dev:9034/usfeddocs/collection1/update?wt=json'

    @extractor = Traject::Indexer.new
    @extractor.load_config_file('traject_config.rb')

    @sources_sql = "SELECT s.source, s.file_path, hg.id as doc_id FROM hathi_gd hg
                     LEFT JOIN gd_source_recs s ON s.file_input_id = hg.file_id AND s.line_number = hg.lineno
                    WHERE hg.id IN(?)" 
    @get_enumchron_sql = "SELECT DISTINCT(hs.str) as enum_chron FROM hathi_enumc he
                     LEFT JOIN hathi_str hs ON he.str_id = hs.id 
                     WHERE he.gd_id IN(?) LIMIT 1"

    @id_log = open("ids.log.tmp","w")

    @db = HTPH::Hathidb::Db.new();
    @conn = @db.get_conn();
    @source_rec_timer = 0
    @marc_hash_timer = 0
  end


  def build_record ids
    rec = {}
    rec['source_records'] = []
    sources = self.get_sources( ids )
    
    #get the enumchron from the database
    rec['enumchron_display'] = self.get_enumchron(ids)

    rec['ht_ids_fv'] = []
    rec['ht_ids_lv'] = []

    sources.each do | doc_id, s |
      rec['source_records'] << s[:source]
      if s[:fname] =~ /zeph/ 
        #use this HT rec as the base
        if !rec.has_key? 'text' 
          start = Time.now
          base_marc = MARC::Record.new_from_hash(JSON.parse(s[:source]))
          rec.merge!(@extractor.map_record(base_marc))
          @marc_hash_timer = @marc_hash_timer + (Time.now - start)
        end

        #in HT and full view, public domain
        if s[:source] =~ /.r.:.pd./
          ht_id = self.get_ht_id(s[:source])
          rec['ht_availability'] = 'full view'
          unless rec['ht_ids_fv'].include? ht_id 
            rec['ht_ids_fv'] << ht_id 
          end
        #in HT but restricted
        else
          ht_id = self.get_ht_id(s[:source])
          rec['ht_availability'] = 'limited view' unless rec['ht_availability'] == 'full view'
          unless rec['ht_ids_lv'].include? ht_id 
            rec['ht_ids_lv'] << ht_id 
          end
        end
      end
    end

    #we havent trajected into display fields, so we'll use the first
    if !rec.has_key? 'text' 
      start = Time.now
      base_marc = MARC::Record.new_from_hash(JSON.parse(rec['source_records'][0]))
      rec.merge!(@extractor.map_record(base_marc))
      @marc_hash_timer = @marc_hash_timer + (Time.now - start)
    end

    rec['marc_display'] = rec['source_records'][0]
    rec['gd_ids'] = sources.keys
    rec['id'] = SecureRandom.uuid()

    return rec
  end

  def get_ht_id( rec )
    id = /"001":"(\d+)"/.match(rec)[1]
    return id
  rescue
    PP.pp rec
    raise
  end
    
  def get_enumchron( gd_ids )
    enumchron = ''
    ss = @get_enumchron_sql.gsub(/\?/, gd_ids.join(',')) #yuck
    @conn.query(ss) do | row |
      enumchron = row[:enum_chron]
    end
    return enumchron
  end


  def get_sources( doc_ids )
    start = Time.now
    sources = {}
    ss = @sources_sql.gsub(/\?/, doc_ids.join(',')) #yuck
    @conn.query(ss) do | row | 
      doc_id = row[:doc_id]
      fname = row[:file_path]
      source = row[:source].chomp
      sources[doc_id.to_s] = {:fname => fname, :source => source }
    end
    @source_rec_timer = @source_rec_timer + (Time.now - start)
    return sources
  end

end #class Collator

