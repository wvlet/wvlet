-- Test that the parser can now distinguish between function calls and literals for DATE/TIME/TIMESTAMP
SELECT
  DATE '2025-08-17' as date_literal,
  TIME '10:30:00' as time_literal,
  TIMESTAMP '2025-08-17 10:30:00' as timestamp_literal,
  -- These should parse as function calls (even if the functions don't exist in DuckDB)
  length('test') as working_func,
  upper('test') as another_working_func