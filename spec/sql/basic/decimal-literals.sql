-- Test case for DECIMAL 'value' literal syntax
-- This addresses parsing errors with DECIMAL literals in SQL expressions

-- Basic DECIMAL literal
SELECT DECIMAL '0.95';

-- DECIMAL literal in function call (reproduces the original error)
SELECT round(approx_quantile(col1, DECIMAL '0.95'), 3) as p95
FROM (VALUES (1.0), (2.0), (3.0), (4.0), (5.0)) as t(col1);

-- DECIMAL literal in arithmetic expressions
SELECT DECIMAL '100.50' + DECIMAL '200.25' as total;

-- DECIMAL literal with different precision
SELECT DECIMAL '123.456789' as precise_value;

-- DECIMAL literal in comparison
SELECT * FROM (VALUES (1.5), (0.95), (2.3)) as t(value) 
WHERE value > DECIMAL '1.0';