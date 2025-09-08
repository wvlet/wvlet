-- Test SHOW CREATE VIEW syntax support
-- This tests the parsing of SHOW CREATE VIEW statements

-- Create test tables and views first
create table if not exists test_table as select 1 as id, 'test' as name;

-- Create test views for SHOW CREATE VIEW commands
create view test_view as select * from test_table;
create view default.test_view as select * from test_table;

-- Test SHOW CREATE VIEW with simple view name
show create view test_view;

-- Test SHOW CREATE VIEW with qualified view name  
show create view default.test_view;

-- Test SHOW CREATE VIEW with fully qualified view name (this may fail but should parse)
show create view catalog.schema.test_view;