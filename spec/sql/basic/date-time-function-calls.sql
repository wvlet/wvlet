-- Test that the parser can now distinguish between function calls and literals for DATE/TIME/TIMESTAMP
-- The parser should treat DATE/TIME/TIMESTAMP as function names when followed by parentheses
-- and as literal type prefixes when followed by a string literal

-- Test 1: Function call syntax (parser test - execution will be ignored in SqlBasicSpec)
-- The generated SQL should have "date"(...), "time"(...), "timestamp"(...) - note the quotes
SELECT
  date('2025-08-17') as date_func,
  time('10:30:00') as time_func,
  timestamp('2025-08-17 10:30:00') as timestamp_func;

-- Test 2: Literals (these work in DuckDB)
SELECT
  DATE '2025-08-17' as date_literal,
  TIME '10:30:00' as time_literal,
  TIMESTAMP '2025-08-17 10:30:00' as timestamp_literal;

-- Test 3: Mix literals with other functions that do exist
SELECT
  DATE '2025-08-17' as date_literal,
  length('test') as working_func,
  upper('test') as another_working_func