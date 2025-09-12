-- Test SHOW STATS FOR syntax support
-- This tests the parsing of SHOW STATS FOR statements

-- Create test table first
create table if not exists test_table (id int, name varchar(50));

-- Test SHOW STATS FOR table
show stats for test_table;

-- Test SHOW STATS FOR qualified table name
show stats for schema.test_table;

-- TODO: Test SHOW STATS FOR query (using VALUES to avoid dependencies)
-- show stats for (values (1, 'test'), (2, 'example'));