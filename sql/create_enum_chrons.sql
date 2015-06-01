use ht_repository;

/*-- Drop existing tables.
--drop table if exists enum_chrons;
*/
drop table if exists enum_chrons;

CREATE TABLE enum_chrons (
  id        INT          not null auto_increment,
  file_input_id  INT           null,
  input_file_name VARCHAR(255) not null,
  line_number INT          not null,
  chronology text  not null,
  enumeration text not null,
  primary key (id)
);

