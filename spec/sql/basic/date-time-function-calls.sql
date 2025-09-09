-- Test that the parser can distinguish between DATE/TIME/TIMESTAMP function calls and literals
-- This validates the fix for the SQL parser to properly handle these tokens based on context

-- Test 1: Function call syntax
-- Parser should treat these as function calls when followed by parentheses
-- Generated SQL should have "date"(...), "time"(...), "timestamp"(...) with quotes
-- Note: These tests are for parsing only - execution ignored since DuckDB lacks these functions
SELECT
  date('2025-08-17') as date_func,
  time('10:30:00') as time_func,
  timestamp('2025-08-17 10:30:00') as timestamp_func;

-- Test 2: Additional function call patterns
SELECT date('2025-08-17'), time('10:30:00'), timestamp('2025-08-17 10:30:00');

-- Test 3: Literal syntax
-- Parser should treat these as typed literals when followed by string literals
SELECT
  DATE '2025-08-17' as date_literal,
  TIME '10:30:00' as time_literal,
  TIMESTAMP '2025-08-17 10:30:00' as timestamp_literal;

-- Test 4: Mix literals with regular functions to verify parsing continues to work
SELECT
  DATE '2025-08-17' as date_literal,
  length('test') as length_func,
  upper('hello') as upper_func;

-- Test 5: Original issue pattern - function call in WHERE clause
-- This was the specific pattern from issue #1238 that was failing
SELECT 1 FROM (VALUES (1)) AS t(x) WHERE date(cast(1755446400 as varchar)) >= DATE '2025-08-17'