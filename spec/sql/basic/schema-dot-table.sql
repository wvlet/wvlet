-- Test case for schema.table with quoted identifiers using DuckDB's main schema
-- DuckDB has a built-in 'main' schema that doesn't need to be created

-- Create a test table in the main schema
drop table if exists "main"."customers";
create table "main"."customers" as 
select * from (values 
  (1, 'Chris', 'Smith'),
  (2, 'John', 'Doe'),
  (3, 'Chris', 'Johnson')
) as t(cdp_customer_id, first_name, last_name);

-- Now test the query with schema.table notation
select
  a."cdp_customer_id"
from "main"."customers" a
where a."first_name" = 'Chris';

-- Clean up
drop table "main"."customers"