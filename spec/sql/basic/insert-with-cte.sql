-- Test INSERT INTO with CTE as the data source

-- Basic INSERT INTO with CTE using VALUES
INSERT INTO target_table
WITH cte AS (
  VALUES (1, 'a'), (2, 'b'), (3, 'c')
)
SELECT * FROM cte;

-- INSERT INTO with multiple CTEs using VALUES
INSERT INTO results
WITH 
  cte1 AS (VALUES (1, 'Alice'), (2, 'Bob')),
  cte2 AS (VALUES (1, 100), (2, 200))
SELECT * FROM cte1 JOIN cte2 ON cte1.column1 = cte2.column1;

-- Complex example from the error report (simplified without NOW())
INSERT INTO t_57ac2
WITH
  t_99f97 AS (
   SELECT 1234567890 AS f_9d304
) 
SELECT
  t_99f97.f_9d304,
  concat(CAST(t_99f97.f_9d304 AS VARCHAR), '@example.com') f_c9456
FROM
  t_99f97;

-- INSERT INTO with column list and CTE
INSERT INTO users (id, email)
WITH generated AS (
  SELECT 1 as id, 'test@example.com' as email
)
SELECT * FROM generated;

-- INSERT INTO with parenthesized CTE query
INSERT INTO target_table
(WITH cte AS (VALUES (1, 2), (3, 4)) SELECT * FROM cte);

-- Nested CTEs in INSERT using VALUES
INSERT INTO summary_table
WITH 
  base AS (VALUES ('A', 1), ('B', 2), ('A', 3), ('B', 4)),
  aggregated AS (
    SELECT column1 as category, COUNT(*) as cnt 
    FROM base 
    GROUP BY column1
  )
SELECT * FROM aggregated WHERE cnt > 1;

-- INSERT with VALUES followed by WITH (should fail - just for testing parser)
-- INSERT INTO t VALUES (1, 2) WITH cte AS (SELECT 1) SELECT * FROM cte;