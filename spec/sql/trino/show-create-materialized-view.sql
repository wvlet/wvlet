-- Test SHOW CREATE MATERIALIZED VIEW syntax support
-- This tests the parsing of SHOW CREATE MATERIALIZED VIEW statements

-- Test SHOW CREATE MATERIALIZED VIEW with simple view name
show create materialized view test_mv;

-- Test SHOW CREATE MATERIALIZED VIEW with qualified view name
show create materialized view schema.test_mv;

-- Test SHOW CREATE MATERIALIZED VIEW with fully qualified view name
show create materialized view catalog.schema.test_mv;
