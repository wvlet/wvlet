-- Test complex CLUSTER BY structure similar to failing test
WITH test_cte AS (
  SELECT
    some_func(a, b, c) AS (x, y, z)
  FROM (
    SELECT a, b, c
    FROM test_table
    CLUSTER BY a
  ) t
)
SELECT * FROM test_cte;