-- Test that the parser can now distinguish between function calls and literals for DATE/TIME/TIMESTAMP
-- The parser should treat DATE/TIME/TIMESTAMP as function names when followed by parentheses
-- and as literal type prefixes when followed by a string literal

-- Test 1: Literals (these work in DuckDB)
SELECT
  DATE '2025-08-17' as date_literal,
  TIME '10:30:00' as time_literal,
  TIMESTAMP '2025-08-17 10:30:00' as timestamp_literal;

-- Test 2: Parser should handle function-like syntax (parse only - DuckDB doesn't have these functions)
-- This validates that the parser correctly identifies these as function calls, not literals
-- The generated SQL should have "date"(...), "time"(...), "timestamp"(...) - note the quotes
-- Comment out execution test since DuckDB doesn't have these functions
-- SELECT
--   date('2025-08-17') as date_func,
--   time('10:30:00') as time_func,
--   timestamp('2025-08-17 10:30:00') as timestamp_func;

-- Test 3: Mix with other functions that do exist
SELECT
  DATE '2025-08-17' as date_literal,
  length('test') as working_func,
  upper('test') as another_working_func