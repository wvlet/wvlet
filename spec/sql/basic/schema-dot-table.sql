-- Test case for schema.table with quoted identifiers
-- First create a table to test with
drop table if exists "test_schema"."customers";
drop schema if exists "test_schema";
create schema "test_schema";
create table "test_schema"."customers" as 
select * from (values 
  (1, 'Chris', 'Smith'),
  (2, 'John', 'Doe'),
  (3, 'Chris', 'Johnson')
) as t(cdp_customer_id, first_name, last_name);

-- Now test the query with schema.table notation
select
  a."cdp_customer_id"
from "test_schema"."customers" a
where a."first_name" = 'Chris';

-- Clean up
drop table "test_schema"."customers";
drop schema "test_schema"