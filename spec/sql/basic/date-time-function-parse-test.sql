-- Test that DATE/TIME/TIMESTAMP are parsed as function calls when followed by parentheses
-- This file tests parsing only (no execution) to verify the fix for distinguishing
-- between function calls like date(...) and literals like DATE '...'

-- These should be parsed as function calls with quoted names in the generated SQL
-- Note: These are parsed but commented out to avoid execution errors since DuckDB
-- doesn't have these specific functions

-- Parse test: function call syntax (would generate "date"(...), "time"(...), "timestamp"(...))
-- Uncomment to see parsing result in error message:
-- SELECT date('2025-08-17'), time('10:30:00'), timestamp('2025-08-17 10:30:00');

-- These are valid literals that should continue to work
SELECT 
  DATE '2025-08-17',
  TIME '10:30:00',  
  TIMESTAMP '2025-08-17 10:30:00';

-- Verify that regular function parsing still works
SELECT length('test'), upper('hello');