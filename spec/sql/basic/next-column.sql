-- Test that 'next' can be used as a column name in Hive SQL
-- Since 'next' is a non-reserved keyword, it should be allowed

-- Test 1: Using 'next' as a column name in SELECT
SELECT next FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, next);

-- Test 2: Using 'next' as an alias
SELECT col1 AS next FROM (VALUES ('test')) AS t(col1);

-- Test 3: Using 'next' in WHERE clause
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, next) WHERE next = 'a';

-- Test 4: Using 'next' in ORDER BY
SELECT * FROM (VALUES (2, 'b'), (1, 'a')) AS t(id, next) ORDER BY next;

-- Test 5: Using 'next' in GROUP BY
SELECT next, COUNT(*) FROM (VALUES ('a'), ('a'), ('b')) AS t(next) GROUP BY next;

-- Test 6: Using 'next' in VALUES with column names
SELECT * FROM (VALUES (1, 'value', 'prev_value')) AS test_table(id, next, prev);

-- Test 7: Using 'next' with table prefix
SELECT t1.next FROM (VALUES (1, 'a')) AS t1(id, next);

-- Test 8: Using 'next' in JOIN conditions
SELECT * FROM 
  (VALUES (1, 'a')) AS t1(id, next) 
  JOIN 
  (VALUES (1, 'x')) AS t2(id, value) 
  ON t1.next = t2.value;

-- Test 9: Using NEXT as keyword in FETCH clause (should still work)
SELECT * FROM (VALUES (1), (2), (3)) AS t(id) FETCH NEXT 2 ROWS ONLY;
