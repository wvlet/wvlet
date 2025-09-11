-- Test CAST expressions in GROUP BY clause
WITH test_cte AS (
  SELECT 
    f_b6423,
    f_38157
  FROM t_01537
)
SELECT 
  f_b6423,
  CAST(YEAR(CAST(f_38157 AS date)) AS varchar) as year,
  CAST(MONTH(CAST(f_38157 AS date)) AS varchar) as month
FROM test_cte
GROUP BY f_b6423, CAST(YEAR(CAST(f_38157 AS date)) AS varchar), CAST(MONTH(CAST(f_38157 AS date)) AS varchar);

-- Test with TRY_CAST
SELECT 
  col1,
  TRY_CAST(col2 AS integer) as int_col
FROM my_table
GROUP BY col1, TRY_CAST(col2 AS integer);

-- Test with CASE expression
SELECT 
  col1,
  CASE WHEN col2 > 0 THEN 'positive' ELSE 'non-positive' END as category
FROM my_table  
GROUP BY col1, CASE WHEN col2 > 0 THEN 'positive' ELSE 'non-positive' END;

-- Test with EXTRACT
SELECT
  col1,
  EXTRACT(year FROM col2) as year_part
FROM my_table
GROUP BY col1, EXTRACT(year FROM col2);