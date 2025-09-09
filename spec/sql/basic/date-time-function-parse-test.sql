-- Additional test for DATE/TIME/TIMESTAMP function parsing
-- This file provides additional test coverage for the parser fix

-- Test function call syntax - parser should generate "date"(...), "time"(...), "timestamp"(...)
-- These will be ignored in SqlBasicSpec since DuckDB doesn't have these functions
SELECT date('2025-08-17'), time('10:30:00'), timestamp('2025-08-17 10:30:00');

-- Test literals continue to work
SELECT 
  DATE '2025-08-17',
  TIME '10:30:00',  
  TIMESTAMP '2025-08-17 10:30:00';

-- Verify that regular function parsing still works
SELECT length('test'), upper('hello');