-- Hive UDTF with column aliases syntax tests

-- Basic each_top_k with AS clause for column aliases
SELECT
  each_top_k(
    20, cdp_customer_id, tag_score,
    cdp_customer_id, tag
  ) AS (rank, tag_score, cdp_customer_id, tag)
FROM cdp_tmp_word_tagging_behavior_behv_orders;

-- UDTF with multiple column aliases
SELECT
  func_name(arg1, arg2, arg3) AS (col1, col2, col3, col4)
FROM table1;

-- Nested UDTF with column aliases  
SELECT
  outer_func(
    inner_func(x, y) AS (a, b),
    z
  ) AS (result1, result2)
FROM table2;