-- Test case for boolean literal syntax in SQL expressions
-- This addresses parsing errors with true/false in WHERE clauses and expressions

-- Basic boolean literals
SELECT true as bool_true, false as bool_false;

-- Boolean literals in WHERE clause (reproduces the original error)
SELECT col1 FROM (VALUES (1), (2), (3)) as t(col1) WHERE true;
SELECT col1 FROM (VALUES (1), (2), (3)) as t(col1) WHERE false;

-- Boolean literals in CASE expressions  
SELECT 
  CASE WHEN true THEN 'always' ELSE 'never' END as case_true,
  CASE WHEN false THEN 'never' ELSE 'always' END as case_false;

-- Boolean literals in boolean operations
SELECT true AND false as and_result, true OR false as or_result;

-- Boolean literals in function calls
SELECT COALESCE(null, true) as coalesced_bool;