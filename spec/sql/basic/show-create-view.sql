-- Test SHOW CREATE VIEW syntax support
-- This tests the parsing of SHOW CREATE VIEW statements

-- Create test table first
create table if not exists test_table as select 1 as id, 'test' as name;

-- Create test view
create or replace view test_view as select * from test_table;

-- Test SHOW CREATE VIEW with simple view name
show create view test_view;