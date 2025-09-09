-- Test INSERT with parenthesized SELECT - this was failing with parser error
-- Now it should work correctly

CREATE TABLE test_table (id INT, value TEXT);

-- This used to fail with "Expected R_PAREN, but found STAR" 
INSERT INTO test_table
(
  SELECT 1 as id, 'test' as value
);

-- Verify both syntaxes work
INSERT INTO test_table (id, value)
SELECT 2 as id, 'test2' as value;