use ht_repository;

/*-- Drop existing tables.
--drop table if exists etld_govdocs;
*/
drop table if exists etld_govdocs;

CREATE TABLE etld_govdocs (
  id        INT          not null auto_increment,
  govdoc_id VARCHAR(255) not null,
  primary key (id),
  INDEX `govdoc_id_ind` (`govdoc_id`)
);

