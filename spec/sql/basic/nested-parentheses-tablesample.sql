-- Test case for TABLESAMPLE with nested parentheses (Trino-specific syntax)
-- This tests the pattern: ((table alias) tablesample method (percentage))
-- Note: This syntax works in Trino but not in DuckDB

select * from ((information_schema.tables a1) tablesample bernoulli (5));