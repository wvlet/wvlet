-- Test cases for COMMENT syntax in CREATE TABLE statements
-- This addresses the parsing issue where COMMENT before WITH properties fails

-- Basic COMMENT syntax
CREATE TABLE test_comment_basic (
   id bigint COMMENT 'Primary identifier'
);

-- COMMENT with WITH properties (the original failing case)
CREATE TABLE test_comment_with_properties (
   f_9d304 bigint COMMENT 'Unixtime' WITH ( key_name = 'time' )
);

-- Multiple columns with different combinations
CREATE TABLE test_comment_variations (
   id bigint NOT NULL COMMENT 'Primary key',
   name varchar COMMENT 'User name' WITH ( encoding = 'utf8' ),
   created_at timestamp DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation time',
   score double WITH ( precision = '2' ) COMMENT 'User score',
   status varchar NOT NULL DEFAULT 'active' COMMENT 'Account status' WITH ( enum_values = 'active,inactive,suspended' )
);

-- Test with VALUES expressions (recommended in CLAUDE.md for SQL specs)
CREATE TABLE test_comment_with_data AS 
SELECT *
FROM (
   VALUES 
   (1, 'Alice', TIMESTAMP '2023-01-01 00:00:00', 95.5, 'active'),
   (2, 'Bob', TIMESTAMP '2023-01-02 12:30:00', 87.2, 'inactive')
) AS t(id, name, created_at, score, status);

-- Edge cases
CREATE TABLE test_comment_edge_cases (
   -- Comment with single quotes inside
   col1 varchar COMMENT 'User''s full name',
   -- Comment with special characters
   col2 int COMMENT 'Count (total)',
   -- Empty comment
   col3 boolean COMMENT '',
   -- Long comment
   col4 text COMMENT 'This is a very long comment that describes the purpose and usage of this column in great detail for documentation purposes'
);

-- Test table creation with LIKE and comments preserved
CREATE TABLE test_like_with_comments (
  LIKE test_comment_basic INCLUDING PROPERTIES
);

-- Test IF NOT EXISTS with comments
CREATE TABLE IF NOT EXISTS test_comment_if_not_exists (
   id serial COMMENT 'Auto-increment ID',
   data jsonb COMMENT 'JSON data' WITH ( compression = 'gzip' )
);