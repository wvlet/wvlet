-- Test IS NULL and IS NOT NULL syntax
-- These should NOT generate warnings

-- IS NULL tests
SELECT 
    1 IS NULL,
    'hello' IS NULL,
    NULL IS NULL,
    (1 + 2) IS NULL;

-- IS NOT NULL tests  
SELECT
    1 IS NOT NULL,
    'world' IS NOT NULL,
    NULL IS NOT NULL,
    (3 * 4) IS NOT NULL;

-- Mixed expressions
SELECT
    CASE 
        WHEN 'test' IS NULL THEN 'is null'
        WHEN 'test' IS NOT NULL THEN 'is not null'
    END as null_check;

-- These should still generate warnings (using = and != with NULL)
SELECT
    1 = NULL,           -- Should warn
    'hello' != NULL;    -- Should warn
