-- Trino CREATE TABLE LIKE syntax tests

-- Basic LIKE syntax (default is EXCLUDING PROPERTIES)
CREATE TABLE test_table_basic (
  LIKE source_table
);

-- LIKE with explicit EXCLUDING PROPERTIES
CREATE TABLE test_table_explicit_exclude (
  LIKE source_table EXCLUDING PROPERTIES
);

-- LIKE with INCLUDING PROPERTIES
CREATE TABLE test_table_include_props (
  LIKE source_table INCLUDING PROPERTIES
);

-- Mixed columns and LIKE - column before LIKE
CREATE TABLE test_mixed_before (
  id BIGINT,
  LIKE source_table,
  created_at TIMESTAMP
);

-- Mixed columns and LIKE - column after LIKE
CREATE TABLE test_mixed_after (
  LIKE source_table,
  processed_at TIMESTAMP
);

-- CREATE OR REPLACE with LIKE
CREATE OR REPLACE TABLE test_replace_with_like (
  LIKE source_table INCLUDING PROPERTIES
);

-- CREATE TABLE IF NOT EXISTS with LIKE
CREATE TABLE IF NOT EXISTS test_if_not_exists_like (
  LIKE source_table
);

-- LIKE with WITH properties
CREATE TABLE test_like_with_props (
  LIKE source_table
) WITH (
  format = 'ORC',
  partitioned_by = ARRAY['date']
);

-- LIKE with INCLUDING PROPERTIES and WITH properties
CREATE TABLE test_like_include_and_with (
  LIKE source_table INCLUDING PROPERTIES
) WITH (
  format = 'PARQUET'
);

-- CREATE TABLE AS with LIKE
CREATE TABLE test_like_as_select (
  LIKE source_table
) AS SELECT 1 as id, 'test' as name;

-- CREATE TABLE AS with LIKE and WITH properties
CREATE TABLE test_like_as_select_with_props (
  LIKE source_table INCLUDING PROPERTIES
) WITH (
  bucketed_on = ARRAY['id'],
  bucket_count = 16
) AS SELECT 1 as id, 'test' as name;

-- Multiple LIKE clauses (if supported)
CREATE TABLE test_multiple_likes (
  col1 INT,
  LIKE table1,
  col2 VARCHAR(100),
  LIKE table2 INCLUDING PROPERTIES
);

-- Qualified table names in LIKE
CREATE TABLE test_qualified_like (
  LIKE schema.source_table INCLUDING PROPERTIES
);

-- Fully qualified table names in LIKE
CREATE TABLE test_fully_qualified_like (
  LIKE catalog.schema.source_table
);