-- Test case for database-qualified table names in FROM clauses
-- These test the parsing of qualified names that are currently supported

-- Test parsing of qualified names using information_schema tables (which exist)
SELECT table_name FROM information_schema.tables LIMIT 1;

-- Test three-part qualified name parsing
SELECT column_name FROM information_schema.columns LIMIT 1;

-- Test qualified names in JOIN with information_schema
SELECT t.table_name, c.column_name 
FROM information_schema.tables t
JOIN information_schema.columns c ON t.table_name = c.table_name
LIMIT 1;