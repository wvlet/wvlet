-- Test date function calls vs date literals (simple test without functions that don't exist in DuckDB)
SELECT
  DATE '2025-08-17' as date_literal,
  TIME '10:30:00' as time_literal,
  TIMESTAMP '2025-08-17 10:30:00' as timestamp_literal