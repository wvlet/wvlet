-- Test DELETE FROM WHERE syntax support
-- This tests the parsing of DELETE statements with WHERE clauses

-- Create a test table for DELETE operations
drop table if exists test_delete_table;

create table test_delete_table as
select * from (values 
  (1, 'active', 100.0, 'electronics'),
  (2, 'cancelled', 50.0, 'books'), 
  (3, 'active', 25.0, 'electronics'),
  (4, 'pending', 75.0, 'outdated'),
  (5, 'cancelled', 10.0, 'outdated'),
  (6, 'active', 200.0, 'clothing')
) as t(id, status, price, category);

-- Test basic DELETE with simple WHERE clause
delete from test_delete_table where status = 'cancelled';

-- Test DELETE with multiple conditions using AND
delete from test_delete_table where price < 10 and category = 'outdated';

-- Test DELETE with multiple conditions using OR  
delete from test_delete_table where status = 'pending' or category = 'outdated';

-- Test DELETE with numeric comparison
delete from test_delete_table where price > 150.0;

-- Test DELETE with complex WHERE expression
delete from test_delete_table where (status = 'active' and price < 50.0) or category = 'books';

-- Clean up
drop table test_delete_table;