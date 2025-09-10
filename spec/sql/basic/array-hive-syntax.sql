-- Test Hive-style array constructor with parentheses
-- Simple array with string literals
SELECT array('bias');

-- Array with multiple string literals
SELECT array('car_color', 'car_engine');

-- Array with mixed types
SELECT array('a', 'b', 'c');

-- Array with numbers
SELECT array(1, 2, 3);

-- Nested array function call
SELECT array_concat(
    array('bias'),
    array('car_color', 'car_engine', 'car_grade')
);

-- Traditional bracket syntax should still work
SELECT array['x', 'y', 'z'];

-- Mixed usage in complex query
SELECT array_concat(
    array('prefix'),
    array['middle1', 'middle2'],
    array('suffix')
);

-- Test LATERAL VIEW explode syntax (Hive specific)
-- Simple LATERAL VIEW with explode
SELECT
    id,
    col
  FROM table1
  LATERAL VIEW explode(array_col) t AS col;

-- LATERAL VIEW with multiple column aliases
SELECT
    id,
    key,
    value
  FROM table1
  LATERAL VIEW explode(map_col) t AS key, value;

-- Complex example with subquery
SELECT
    article_id,
    word,
    freq
  FROM (
    SELECT
      article_id,
      tf(word) AS word2freq
    FROM
      cdp_tmp_word_tagging_behavior_behv_orders_articles_tokens
    GROUP BY
      article_id
  ) t
  LATERAL VIEW explode(word2freq) t2 AS word, freq;

-- Multiple LATERAL VIEW clauses
SELECT
    id,
    col1,
    col2
  FROM table1
  LATERAL VIEW explode(array_col1) t1 AS col1
  LATERAL VIEW explode(array_col2) t2 AS col2;