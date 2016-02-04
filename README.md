# !!! No longer used. !!!

See registry and transformation_logging repos. 

# etl

1. load raw source
  mysql -hmysql-htprep -ujstever -p<pwd> --execute "use ht_repository; SELECT id, file_path FROM hathi_input_file;" --bat > data/sources.txt
  nohup bundle exec ruby load_raw_source.rb data/sources.txt

2. get oclc/enumchron dump from mysql
  see sql/get_oclc_ec_clusters.sql

3. get dupe/solo clusters without oclc numbers 
    nohup bundle exec ruby cross_check_dupes.rb /htdata/govdocs/clusters/duplicates_20150827.tsv > dupes_no_oclc.txt

    nohup bundle exec ruby cross_check_solos.rb /htdata/govdocs/clusters/solos_20150827.tsv > solos_no_oclc.txt

4. compile records 
  bundle exec ruby -J-Xmx8192m etl_oclc_threaded.rb oclc_ec_clusters.txt <data_out> <num_threads (6)>
  bundle exec ruby -J-Xmx8192m etl_cluster_threaded.rb dupes_no_oclc.txt <data_out> <num threads (6)>
  bundle exec ruby -J-Xmx8192m etl_solo_threaded.rb solos_no_oclc.txt <data_out> <num threads (6)>

5. load compiled data
  bundle exec ruby -J-Xmx8192m load.rb <data_out from step 4>

