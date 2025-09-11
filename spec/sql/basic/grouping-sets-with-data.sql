-- Test GROUPING SETS with actual data
WITH sales AS (
  SELECT * FROM VALUES 
    ('A', 'X', 100),
    ('A', 'Y', 200), 
    ('B', 'X', 150),
    ('B', 'Y', 250)
  AS t(region, product, amount)
)
SELECT region, product, SUM(amount) as total
FROM sales
GROUP BY GROUPING SETS ((region, product), (region), ());

-- Test CUBE with data
WITH data AS (
  SELECT * FROM VALUES
    ('North', 'Q1', 100),
    ('North', 'Q2', 200),
    ('South', 'Q1', 150) 
  AS t(region, quarter, sales)
)
SELECT region, quarter, SUM(sales) as total_sales
FROM data
GROUP BY CUBE (region, quarter);

-- Test ROLLUP with data  
WITH revenue AS (
  SELECT * FROM VALUES
    (2023, 1, 1000),
    (2023, 2, 2000),
    (2024, 1, 1500)
  AS t(year, quarter, amount)
)
SELECT year, quarter, SUM(amount) as total_revenue
FROM revenue  
GROUP BY ROLLUP (year, quarter);