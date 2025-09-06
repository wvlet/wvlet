-- Test case for SHOW COLUMNS FROM syntax
-- This addresses parsing errors with SHOW COLUMNS statements

-- Basic SHOW COLUMNS FROM table
SHOW COLUMNS FROM information_schema.tables;

-- SHOW COLUMNS FROM with fully qualified table name (reproduces the original pattern)
SHOW COLUMNS FROM "catalog"."schema"."table";

-- SHOW COLUMNS FROM with two-part qualified name
SHOW COLUMNS FROM schema.table_name;

-- SHOW COLUMNS FROM with single table name
SHOW COLUMNS FROM simple_table;