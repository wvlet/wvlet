-- Test GROUPING SETS functionality
-- Basic GROUPING SETS syntax with multiple grouping combinations
SELECT col1, col2, col3, SUM(col4)
FROM table1
GROUP BY GROUPING SETS ((col1, col2), (col1), (col2), ());

-- GROUPING SETS with qualified column names
SELECT a.col1, b.col2, COUNT(*)
FROM table1 a JOIN table2 b ON a.id = b.id
GROUP BY GROUPING SETS ((a.col1, b.col2), (a.col1), ());

-- CUBE syntax
SELECT region, country, city, SUM(sales)
FROM sales_data
GROUP BY CUBE (region, country, city);

-- ROLLUP syntax
SELECT year, quarter, month, SUM(revenue)
FROM financial_data
GROUP BY ROLLUP (year, quarter, month);

-- Mixed regular GROUP BY with expressions
SELECT year_col, col1, AVG(col2)
FROM data_table
GROUP BY GROUPING SETS ((year_col, col1), (year_col), ());

-- Complex GROUPING SETS with multiple sets and expressions
SELECT 
  from_contact,
  from_addresses,
  from_emails,
  from_phones,
  COUNT(*) as record_count
FROM contact_data
GROUP BY GROUPING SETS (
  (from_contact),
  (from_addresses), 
  (from_emails),
  (from_phones),
  (from_contact, from_addresses),
  ()
);

-- Empty grouping set for grand total
SELECT country, region, SUM(amount)
FROM sales
GROUP BY GROUPING SETS ((country, region), (country), ());

-- Empty CUBE and ROLLUP
SELECT col1, SUM(col2)
FROM table1
GROUP BY CUBE ();

SELECT col1, SUM(col2)
FROM table1
GROUP BY ROLLUP ();