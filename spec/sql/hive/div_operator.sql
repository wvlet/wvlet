-- Test DIV integer division operator (Hive/MySQL syntax)
-- Also test DuckDB's // operator for integer division

-- Basic DIV operator test
SELECT 10 DIV 3 AS int_div_result;

-- DIV operator with expressions
SELECT (20 + 5) DIV 4 AS expr_div_result;

-- DIV operator in complex expressions
SELECT 
  100 DIV 7 AS simple_div,
  100 / 7 AS float_div,
  100 DIV 7 * 7 AS div_multiply;

-- DuckDB integer division operator
SELECT 10 // 3 AS duckdb_int_div;

-- Mixed operators
SELECT 
  20 DIV 3 AS hive_div,
  20 // 3 AS duckdb_div,
  20 / 3 AS float_div;

-- The original failing query from CDP (simplified to test DIV operator)
SELECT
  cast(conv(substr(cdp_customer_id,1,2),16,10) as bigint)*3600 div 32 AS time
FROM test_table;

-- Additional test cases with DIV
SELECT
  100 div 10 as lowercase_div,
  100 DIV 10 as uppercase_div,
  100 Div 10 as mixed_case_div;

-- Complex expression with DIV
SELECT
  (a + b) * c DIV d AS complex_expr
FROM test_table;