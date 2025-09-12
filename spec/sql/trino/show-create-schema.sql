-- Test SHOW CREATE SCHEMA syntax support
-- This tests the parsing of SHOW CREATE SCHEMA statements

-- Test SHOW CREATE SCHEMA with simple schema name
show create schema test_schema;

-- Test SHOW CREATE SCHEMA with qualified schema name
show create schema catalog.test_schema;