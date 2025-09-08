-- Test SHOW CREATE VIEW syntax support
-- This tests the parsing of SHOW CREATE VIEW statements

-- Test SHOW CREATE VIEW with simple view name
show create view test_view;

-- Test SHOW CREATE VIEW with qualified view name
show create view default.test_view;

-- Test SHOW CREATE VIEW with fully qualified view name
show create view catalog.schema.test_view;