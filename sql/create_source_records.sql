use ht_repository;
drop table if exists gd_source_recs;

CREATE TABLE gd_source_recs (
  file_path VARCHAR(255) NOT NULL,
  line_number INT NOT NULL,
  file_input_id INT null,
  gd_id INT null,
  source MEDIUMTEXT NOT NULL
);

/* after adding records 

CREATE INDEX file_line (file_path, line_number) ON gd_source_recs;

/* this won't get all of them, fix that tar.gz stuff manually
UPDATE gd_source_recs s, hathi_input_file f SET s.file_input_id = f.id
  WHERE s.file_path = f.file_path;

