-- Test null comparison warnings in SQL files
-- This file should generate warnings for null comparisons

-- Simple null comparisons that should generate warnings
SELECT 
    1 = NULL,           -- Should warn: 1 is null
    NULL = 2,           -- Should warn: 2 is null
    'hello' != NULL,    -- Should warn: 'hello' is not null
    NULL != 'world';    -- Should warn: 'world' is not null

-- Expressions with null comparisons (should generate warnings)
SELECT
    (1 + 2) = NULL,     -- Should warn: (1 + 2) is null
    NULL != (3 * 4);    -- Should warn: (3 * 4) is not null

-- Note: IS NULL/IS NOT NULL are currently transformed to =/!= by the SQL parser
-- so they also generate warnings. This might be addressed in a future fix.