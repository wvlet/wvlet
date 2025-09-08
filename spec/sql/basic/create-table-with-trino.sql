-- Trino-specific CREATE TABLE WITH properties syntax tests

-- CREATE TABLE WITH properties only
create table test_table_with_props
with (
  bucketed_on = ARRAY['symbol'],
  bucket_count = 4,
  max_time_range = '30d'
);

-- CREATE TABLE with columns AND WITH properties (Gemini's suggestion)
create table test_table_columns_and_props (
  id int,
  name string
) with (
  format = 'ORC',
  transactional = true
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

-- Example from the GitHub issue (simplified for testing)
CREATE TABLE test_table_from_issue
WITH (
   bucketed_on = ARRAY['symbol'],
   bucket_count = 4,
   max_time_range = '30d'
) AS SELECT
  col1, col2, col3
FROM (VALUES (1, 'a', 'x'), (2, 'b', 'y')) AS t(col1, col2, col3)
;

-- Test all combinations: columns + WITH properties + AS SELECT
CREATE TABLE test_table_all_clauses (
  processed_col string
) WITH (
   partition_by = ARRAY['date'],
   format = 'PARQUET'
) AS SELECT
  UPPER(col2) as processed_col
FROM (VALUES ('hello'), ('world')) AS t(col2)
;