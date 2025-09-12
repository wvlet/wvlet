-- Test SHOW GRANTS syntax support
-- This tests the parsing of SHOW GRANTS statements

-- Test SHOW GRANTS without ON clause
show grants;

-- Test SHOW GRANTS ON table
show grants on test_table;

-- Test SHOW GRANTS ON qualified table name
show grants on schema.test_table;

-- Test SHOW GRANTS ON fully qualified table name
show grants on catalog.schema.test_table;
