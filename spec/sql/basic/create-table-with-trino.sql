-- Trino-specific CREATE TABLE WITH properties syntax tests

-- CREATE TABLE WITH properties only
create table test_table_with_props
with (
  bucketed_on = ARRAY['symbol'],
  bucket_count = 4,
  max_time_range = '30d'
);

-- CREATE TABLE WITH properties AS SELECT syntax
create table test_table_with_props_as
with (
  bucketed_on = ARRAY['symbol'],
  bucket_count = 4,
  max_time_range = '30d'
) as
select *
from (values (1, 'a'), (2, 'b')) as t(id, name)
;

-- Example from the GitHub issue
CREATE TABLE d_75a1c.t_45a5c
WITH (
   bucketed_on = ARRAY['symbol'],
   bucket_count = 4,
   max_time_range = '30d'
) AS SELECT
  f_9d304
, f_60a8c
, f_fbda8
, f_4a2a5
, f_86eac
, f_0528a
, f_05859
FROM
  d_05883.t_e7d52
WHERE TD_TIME_RANGE(f_9d304, '1990-01-01', '1999-12-31')
;