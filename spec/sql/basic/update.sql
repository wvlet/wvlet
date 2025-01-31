drop table if exists tmp_table;

create table tmp_table (
  id int,
  name string
);

create table if not exists tmp_table (
  id
  int,
  name
  string
);

create table if not exists tmp_table as
select *
from (values (1, 'a'), (2, 'b')) as t(id, name)
;

create or replace table tmp_table as
select *
from (values (1, 'a'), (2, 'b')) as t(id, name)
;

insert
  into tmp_table
select *
from tmp_table limit 1
;

