require 'json'
require 'htph'
require 'marc'
require 'httpclient'
require 'traject'
require 'traject/indexer/settings'
require 'pp'
require 'securerandom'
require 'viaf'

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
    @viaf = Viaf.new()
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
          rec_json = JSON.parse(s[:source])
          base_marc = MARC::Record.new_from_hash(rec_json)
          rec.merge!(@extractor.map_record(base_marc))
          rec.merge!(normalize_viaf(rec_json))
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
      rec_json = JSON.parse(rec['source_records'][0])
      base_marc = MARC::Record.new_from_hash(rec_json)
      rec.merge!(@extractor.map_record(base_marc))
      rec.merge!(normalize_viaf(rec_json))
      @marc_hash_timer = @marc_hash_timer + (Time.now - start)
    end

    rec['marc_display'] = rec['source_records'][0]
    rec['gd_ids'] = sources.keys
    rec['id'] = SecureRandom.uuid()

    return rec
  end

  #normalizes 110/260 fields and gets viaf_ids
  def normalize_viaf source_json
    #what we are building (kind of a dumb structure, but it's going into solr)
    normalized_fields = {'publisher_viaf_ids'=>[], 'publisher_headings'=>[], 'publisher_normalized'=>[],
                         'author_viaf_ids'=>[], 'author_headings'=>[], 'author_normalized'=>[],
                         'author_addl_viaf_ids'=>[], 'author_addl_headings'=>[], 'author_addl_normalized'=>[]}
                    
    marc_fields = {"260"=>"publisher","110"=>"author","710"=>"author_addl"} #we're doing corporate author and publisher
    marc_fields.keys.each do | fnum |
      corp_fields = source_json["fields"].find {|f| f.has_key? fnum}
      next if !corp_fields
      corp_fields.each do | field_name, corp_field |
        indicator = corp_field["ind1"].chomp    
        subfields = []
        corp_field["subfields"].each_with_index do |s, position|
          if (fnum == "260" and s.keys[0] == "b") or fnum != "260"
            subfields.push s.values[0].chomp
          end
        end
        
        viafs = @viaf.get_viaf( subfields ) #hash: viaf_id => normalized heading 
      
        if viafs.size > 0 
          normalized_fields[marc_fields[fnum]+'_viaf_ids'] << viafs.keys
          #get_viaf gave us the heading too
          normalized_fields[marc_fields[fnum]+'_headings'] << viafs.values
        end
        #get_viaf already did this, but didn't return it. oops?
        #normalize the subfields, then normalize the normalized subfields
        normalized_fields[marc_fields[fnum]+'_normalized'] << normalize_corporate(subfields.map{ |sf| normalize_corporate(sf)}.join(' '), false) 
      end #each matching field, e.g. multiple 710s or 260s.  
      normalized_fields[marc_fields[fnum]+'_viaf_ids'].flatten!
      normalized_fields[marc_fields[fnum]+'_headings'].flatten!
      normalized_fields[marc_fields[fnum]+'_normalized'].flatten!
      
    end #each match for [260,110,710]
    
    return normalized_fields

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

