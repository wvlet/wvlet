-- Test CAST expressions in GROUP BY clause
SELECT 
  col1,
  CAST(col2 AS varchar) as col2_str
FROM (VALUES (1, 'a'), (2, 'b')) AS t(col1, col2)
GROUP BY col1, CAST(col2 AS varchar);

-- Test with TRY_CAST
SELECT 
  col1,
  TRY_CAST(col2 AS integer) as int_col
FROM (VALUES ('a', '1'), ('b', '2')) AS t(col1, col2)
GROUP BY col1, TRY_CAST(col2 AS integer);

-- Test with EXTRACT
SELECT
  col1,
  EXTRACT(year FROM col2) as year_part
FROM (VALUES (1, DATE '2023-01-01'), (2, DATE '2024-01-01')) AS t(col1, col2)
GROUP BY col1, EXTRACT(year FROM col2);

-- Test with ARRAY constructor
SELECT
  col1,
  ARRAY[col1, col1 + 1] as arr
FROM (VALUES (1), (2)) AS t(col1)
GROUP BY col1, ARRAY[col1, col1 + 1];

-- Test with INTERVAL literal
SELECT
  col1,
  col2 + INTERVAL '1' DAY as next_day
FROM (VALUES (1, DATE '2023-01-01'), (2, DATE '2024-01-01')) AS t(col1, col2)
GROUP BY col1, col2 + INTERVAL '1' DAY;