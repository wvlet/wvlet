-- Test case for CREATE SCHEMA and DROP SCHEMA syntax
-- Note: This tests parsing only, not execution

-- Basic CREATE SCHEMA
create schema test_schema1;

-- CREATE SCHEMA IF NOT EXISTS
create schema if not exists test_schema2;

-- DROP SCHEMA
drop schema test_schema1;

-- DROP SCHEMA IF EXISTS
drop schema if exists test_schema2;