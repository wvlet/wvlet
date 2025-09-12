-- Test SHOW CREATE TABLE syntax support
-- This tests the parsing of SHOW CREATE TABLE statements

-- Create test table first
create table if not exists test_table (id int, name varchar(50));

-- Test SHOW CREATE TABLE with simple table name
show create table test_table;

-- Test SHOW CREATE TABLE with qualified table name
show create table schema.test_table;

-- Test SHOW CREATE TABLE with fully qualified table name
show create table catalog.schema.test_table;
