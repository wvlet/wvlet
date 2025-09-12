-- Test SHOW CREATE FUNCTION syntax support
-- This tests the parsing of SHOW CREATE FUNCTION statements

-- Test SHOW CREATE FUNCTION with simple function name
show create function test_func;

-- Test SHOW CREATE FUNCTION with qualified function name
show create function schema.test_func;

-- Test SHOW CREATE FUNCTION with fully qualified function name
show create function catalog.schema.test_func;
