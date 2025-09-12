-- Test SHOW BRANCHES syntax support
-- This tests the parsing of SHOW BRANCHES statements

-- Test SHOW BRANCHES without FROM/IN clause
show branches;

-- Test SHOW BRANCHES FROM TABLE
show branches from table test_table;

-- Test SHOW BRANCHES IN TABLE
show branches in table test_table;

-- Test SHOW BRANCHES FROM TABLE with qualified name
show branches from table schema.test_table;
