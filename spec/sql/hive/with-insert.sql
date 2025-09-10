-- Test WITH ... INSERT syntax (Hive-specific)

-- Basic WITH followed by INSERT INTO
WITH temp_data AS (
  SELECT * FROM source_table
)
INSERT INTO target_table
SELECT * FROM temp_data;

-- WITH followed by INSERT OVERWRITE TABLE
WITH temp_data AS (
  SELECT * FROM source_table
)
INSERT OVERWRITE TABLE target_table
SELECT * FROM temp_data;

-- Multiple CTEs followed by INSERT
WITH
  cte1 AS (SELECT * FROM table1),
  cte2 AS (SELECT * FROM table2)
INSERT INTO result_table
SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id;

-- Complex example from the error report
WITH uni AS (
  SELECT
    article_id,
    word
  FROM
    cdp_tmp_word_tagging_behavior_behv_orders_articles_tokens
  WHERE
    unigram = 1
),
bi AS (
  SELECT
    article_id,
    word
  FROM
    cdp_tmp_word_tagging_behavior_behv_orders_articles_tokens
  WHERE
    unigram = 0
)
INSERT OVERWRITE TABLE `cdp_tmp_word_tagging_behavior_behv_orders_articles_tokens_filtered`
SELECT article_id, word FROM uni
UNION ALL
SELECT article_id, word FROM bi
WHERE bi.word IN (SELECT DISTINCT word FROM cdp_tmp_word_tagging_category_mapping_en WHERE instr(word, ' ') > 0);

-- WITH followed by INSERT INTO TABLE with backquoted identifiers
WITH data AS (
  SELECT id, name FROM users
)
INSERT INTO TABLE `schema`.`table`
SELECT * FROM data;

-- Recursive WITH followed by INSERT (if supported)
WITH RECURSIVE hierarchy AS (
  SELECT id, parent_id, name FROM employees WHERE parent_id IS NULL
  UNION ALL
  SELECT e.id, e.parent_id, e.name
  FROM employees e
  JOIN hierarchy h ON e.parent_id = h.id
)
INSERT INTO org_chart
SELECT * FROM hierarchy;

