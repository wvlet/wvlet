-- Test CREATE TABLE AS functionality

-- Clean up from previous runs
drop table if exists test_ctas_basic;
drop table if exists test_ctas_ifnotexists;
drop table if exists test_ctas_replace;

-- Basic CREATE TABLE AS
create table test_ctas_basic as
select * from (values (1, 'hello'), (2, 'world')) as t(id, name);

-- Verify the table was created
select * from test_ctas_basic order by id;

-- CREATE TABLE IF NOT EXISTS (should succeed)
create table if not exists test_ctas_ifnotexists as
select * from test_ctas_basic;

-- Try again (should be a no-op, not an error)
create table if not exists test_ctas_ifnotexists as
select 3 as id, 'new' as name;

-- Verify the table wasn't replaced
select * from test_ctas_ifnotexists order by id;

-- CREATE OR REPLACE TABLE
create or replace table test_ctas_replace as
select 10 as id, 'replaced' as name;

-- Verify the table was replaced
select * from test_ctas_replace;

-- Replace again with different data
create or replace table test_ctas_replace as
select * from (values (20, 'replaced again'), (30, 'another row')) as t(id, name);

-- Verify the table was replaced again
select * from test_ctas_replace order by id;

-- Clean up
drop table test_ctas_basic;
drop table test_ctas_ifnotexists;
drop table test_ctas_replace;