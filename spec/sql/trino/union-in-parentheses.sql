-- Test various UNION scenarios inside parenthesized subqueries
-- This tests the fix for UNION parsing in relationRest method

-- Simple UNION in parentheses
SELECT * FROM (
  SELECT 1 as id, 'first' as name
  UNION
  SELECT 2 as id, 'second' as name
);

-- Multiple UNION operations
SELECT * FROM (
  SELECT 1 as id, 'first' as name
  UNION
  SELECT 2 as id, 'second' as name
  UNION
  SELECT 3 as id, 'third' as name
);

-- UNION ALL in parentheses
SELECT * FROM (
  SELECT 1 as id, 'first' as name
  UNION ALL
  SELECT 2 as id, 'second' as name
);

-- Complex case similar to the original error
CREATE TABLE test_table AS SELECT *
FROM
  (
    (
      SELECT
        concat(CAST(col1 AS varchar), 'suffix1') as field1,
        concat(CAST(col2 AS varchar), 'suffix2') as field2
      FROM table1
    ) UNION (
      SELECT
        concat(CAST(col3 AS varchar), 'suffix3') as field1,
        concat(CAST(col4 AS varchar), 'suffix4') as field2
      FROM table2
    ) UNION (
      SELECT
        concat(CAST(col5 AS varchar), 'suffix5') as field1,
        concat(CAST(col6 AS varchar), 'suffix6') as field2
      FROM table3
    )
  )
ORDER BY field1 ASC;

-- Nested parentheses with UNION
SELECT * FROM (
  (
    SELECT 1 as x UNION SELECT 2 as x
  ) 
  UNION 
  (
    SELECT 3 as x UNION SELECT 4 as x
  )
);