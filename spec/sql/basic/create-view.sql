-- Test CREATE VIEW and CREATE OR REPLACE VIEW syntax support
-- This tests the parsing of view creation statements

-- Create test tables for view operations
drop table if exists sales_data;
drop table if exists customer_data;

create table sales_data as
select * from (values 
  (1, 'North', 1000.0, 2024),
  (2, 'South', 1500.0, 2024), 
  (3, 'East', 2000.0, 2024),
  (4, 'West', 1200.0, 2024),
  (5, 'North', 800.0, 2023)
) as t(id, region, amount, year);

create table customer_data as
select * from (values 
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob', 'bob@example.com'),
  (3, 'Charlie', 'charlie@example.com')
) as t(id, name, email);

-- Test basic CREATE VIEW
create view simple_view as 
select region, amount from sales_data where year = 2024;

-- Test CREATE VIEW with complex query
create view regional_summary as
select 
  region, 
  sum(amount) as total_sales,
  avg(amount) as avg_sales,
  count(*) as num_sales
from sales_data 
where year = 2024
group by region
order by total_sales desc;

-- Test CREATE OR REPLACE VIEW 
create or replace view sales_summary as 
select region, sum(amount) as total_sales
from sales_data 
where year = 2024
group by region
order by total_sales desc;

-- Test CREATE OR REPLACE VIEW with join
create or replace view customer_summary as
select 
  c.id,
  c.name,
  c.email,
  coalesce(s.total_amount, 0) as total_purchases
from customer_data c
left join (
  select customer_id, sum(amount) as total_amount 
  from sales_data 
  group by customer_id
) s on c.id = s.customer_id;

-- Test CREATE VIEW with subquery
create view high_value_regions as
select region, total_sales
from (
  select region, sum(amount) as total_sales
  from sales_data
  group by region
) regional_totals
where total_sales > 1200;

-- Test DROP VIEW
drop view if exists simple_view;
drop view if exists regional_summary;
drop view if exists sales_summary;
drop view if exists customer_summary;
drop view if exists high_value_regions;

-- Clean up tables
drop table sales_data;
drop table customer_data;