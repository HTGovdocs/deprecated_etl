require 'pp'
require 'library_stdnums'

# A sample traject configration, save as say `traject_config.rb`, then
# run `traject -c traject_config.rb marc_file.marc` to index to
# solr specified in config file, according to rules specified in
# config file


# To have access to various built-in logic
# for pulling things out of MARC21, like `marc_languages`
require 'traject/macros/marc21_semantics'
extend  Traject::Macros::Marc21Semantics

# To have access to the traject marc format/carrier classifier
require 'traject/macros/marc_format_classifier'
extend Traject::Macros::MarcFormats

# In this case for simplicity we provide all our settings, including
# solr connection details, in this one file. But you could choose
# to separate them into antoher config file; divide things between
# files however you like, you can call traject with as many
# config files as you like, `traject -c one.rb -c two.rb -c etc.rb`
settings do
  #provide "solr.url", "http://solr-sdr-usfeddocs-dev:9032/usfeddocs/collection1"
  provide "reader_class_name", "Traject::NDJReader"
  provide "marc_source.type", "json"
end

#everything
to_field "text",  extract_all_marc_values

#title
to_field "title",             extract_marc("245a")
to_field "title_display",       extract_marc("245a", :trim_punctuation => true)

to_field "subtitle_t",          extract_marc("245b")
to_field "subtitle_display",    extract_marc("245b", :trim_punctuation => true)


to_field "title_addl_t",        extract_marc("245nps:130:240abcdefgklmnopqrs:210ab:222ab:242abcehnp:243abcdefgklmnopqrs:246abcdefgnp:247abcdefgnp")
to_field "title_added_entry_t", extract_marc("700gklmnoprst:710fgklmnopqrst:711fgklnpst:730abdefgklmnopqrst:740anp:505t:780abcrst:785abcrst:773abrst")

to_field "title_sort",          marc_sortable_title

#title stuff not used for the interface
to_field "title1_t",            extract_marc("245abk")

# Note we can mention the same field twice, these
# ones will be added on to what's already there. Some custom
# logic for extracting 505$t, but only from 505 field that
# also has $r -- we consider that more likely to be a titleish string
to_field "title3_t" do |record, accumulator|
  record.each_by_tag('505') do |field|
    if field['r']
      accumulator.concat field.subfields.collect {|sf| sf.value if sf.code == 't'}.compact
    end
  end
end

#author

to_field "author_t",            extract_marc("100abcdgqu:110abcdgnu:111acdegjnqu")
to_field "author_addl_t",       extract_marc("700abcdegqu:710abcdegnu:711acdegjnqu:720a:505r:245c:191abcdegqu")
to_field "author_display",      extract_marc("100abcdq:110abcdgnu:111acdegjnqu")
to_field "author_display_facet",      extract_marc("100abcdq:110abcdgnu:111acdegjnqu")
to_field "author_sort",         marc_sortable_author

#not needed
to_field "author_facet",        extract_marc("100abcdq:110abcdgnu:111acdenqu:700abcdq:710abcdgnu:711acdenqu", :trim_punctuation => true)


#publisher
#to_field "publisher_t",         extract_marc("260b")
to_field "publisher_t",        extract_marc("260abef:261abef:262ab:264ab")

#place of publication
to_field "published_display",   extract_marc("260a:264|1*|abc", :trim_punctuation => true)

#pubdate
to_field "pub_date",            marc_publication_date

#sudoc
to_field "sudoc_display",       extract_marc("086a")

#lc call number
to_field "lc_callnum_display",  extract_marc("050ab")

#physical description 300
to_field "material_type_display", extract_marc("300")


#OCLC number
to_field "oclcnum_t",           oclcnum


#series titles [4xx, 830]
to_field "title_series_t",      extract_marc("440a:490av:800abcdt:400abcd:810abcdt:410abcd:811acdeft:411acdef:830adfgklmnoprstv:760ast:762ast")


#enum/chron
to_field "enumchron_display",   extract_marc("ecd")

#subject
to_field "subject_t",           extract_marc("600:610:611:630:650:651avxyz:653aa:654abcvyz:655abcvxyz:690abcdxyz:691abxyz:692abxyz:693abxyz:656akvxyz:657avxyz:652axyz:658abcd")
to_field "subject_topic_facet",           extract_marc("600:610:611:630:650:651avxyz:653aa:654abcvyz:655abcvxyz:690abcdxyz:691abxyz:692abxyz:693abxyz:656akvxyz:657avxyz:652axyz:658abcd")


#format
to_field "format",              marc_formats

to_field "isbn_t",		extract_marc('020a', :separator=>nil) do |rec, acc|
     orig = acc.dup
     acc.map!{|x| StdNum::ISBN.allNormalizedValues(x)}
     acc << orig
     acc.flatten!
     acc.uniq!
end
	


