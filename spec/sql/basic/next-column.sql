-- Test that 'next' can be used as a column name in Hive SQL
-- Since 'next' is a non-reserved keyword, it should be allowed

-- Test 1: Using 'next' as a column name in SELECT
SELECT next FROM table1;

-- Test 2: Using 'next' as an alias
SELECT col1 AS next FROM table1;

-- Test 3: Using 'next' in WHERE clause
SELECT * FROM table1 WHERE next = 1;

-- Test 4: Using 'next' in ORDER BY
SELECT * FROM table1 ORDER BY next;

-- Test 5: Using 'next' in GROUP BY
SELECT next, COUNT(*) FROM table1 GROUP BY next;

-- Test 6: Using 'next' in table creation
CREATE TABLE test_table (
    id INT,
    next VARCHAR(100),
    prev VARCHAR(100)
);

-- Test 7: Using 'next' in INSERT
INSERT INTO table1 (id, next) VALUES (1, 'value');

-- Test 8: Using 'next' with table prefix
SELECT t1.next FROM table1 t1;

-- Test 9: Using 'next' in JOIN conditions
SELECT * FROM table1 t1 JOIN table2 t2 ON t1.next = t2.id;

-- Test 10: Using NEXT as keyword in FETCH clause (should still work)
SELECT * FROM table1 FETCH NEXT 10 ROWS ONLY;