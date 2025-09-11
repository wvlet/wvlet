-- Test GROUPING SETS functionality with actual data
-- Basic GROUPING SETS syntax with multiple grouping combinations
WITH test_data AS (
  SELECT * FROM VALUES 
    ('A', 'X', 'US', 100),
    ('A', 'Y', 'US', 200),
    ('B', 'X', 'CA', 150),
    ('B', 'Y', 'CA', 250)
  AS t(col1, col2, col3, col4)
)
SELECT col1, col2, SUM(col4)
FROM test_data
GROUP BY GROUPING SETS ((col1, col2), (col1), (col2), ());

-- CUBE syntax with data
WITH sales_data AS (
  SELECT * FROM VALUES
    ('North', 'US', 'NYC', 1000),
    ('North', 'US', 'Boston', 800),
    ('South', 'US', 'Miami', 600),
    ('South', 'CA', 'Toronto', 500)
  AS t(region, country, city, sales)
)
SELECT region, country, SUM(sales)
FROM sales_data
GROUP BY CUBE (region, country);

-- ROLLUP syntax with data
WITH financial_data AS (
  SELECT * FROM VALUES
    (2023, 1, 1, 1000),
    (2023, 1, 2, 1200),
    (2023, 2, 1, 1500),
    (2024, 1, 1, 1100)
  AS t(year, quarter, month, revenue)
)
SELECT year, quarter, SUM(revenue)
FROM financial_data
GROUP BY ROLLUP (year, quarter);

-- Test empty grouping set
WITH simple_data AS (
  SELECT * FROM VALUES (1, 100), (2, 200) AS t(col1, col2)
)
SELECT SUM(col2) as total
FROM simple_data
GROUP BY GROUPING SETS (());