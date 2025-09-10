-- Test WITH as a subquery
SELECT * FROM (
    WITH cte AS (SELECT 1 as x)
    SELECT * FROM cte
) AS subquery;

-- Test nested WITH
WITH outer_cte AS (
    WITH inner_cte AS (SELECT 1 as x)
    SELECT * FROM inner_cte
)
SELECT * FROM outer_cte;