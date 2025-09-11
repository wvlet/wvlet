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

-- Complex case with VALUES similar to the original error pattern
SELECT *
FROM
  (
    (
      SELECT 'value1' as field1, 'data1' as field2
      FROM (VALUES (1)) t(x)
    ) UNION (
      SELECT 'value2' as field1, 'data2' as field2  
      FROM (VALUES (2)) t(x)
    ) UNION (
      SELECT 'value3' as field1, 'data3' as field2
      FROM (VALUES (3)) t(x)
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