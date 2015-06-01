use ht_repository;

/*-- Drop existing tables.
--drop table if exists tmp_dupe_index;
*/
drop table if exists tmp_relationships;

CREATE TABLE tmp_relationships (
  id        INT          not null auto_increment,
  cluster_id VARCHAR(255) not null,
  relationship VARCHAR(50) not null,
  govdoc_id INT(11) not null,
  score DOUBLE not null,
  file_name VARCHAR(255) not null,
  line_number INT          not null,
  primary key (id),
  INDEX `cluster_id_ind` (`cluster_id`),
  INDEX `govdoc_id_ind` (`govdoc_id`)
);

