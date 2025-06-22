-- Test Array aggregation with Trino
WITH data AS (
    SELECT 1 AS id, 10 AS value
    UNION ALL
    SELECT 1 AS id, 20 AS value
    UNION ALL
    SELECT 2 AS id, 30 AS value
)
SELECT 
    id,
    array_agg(value) AS values_array
FROM data
GROUP BY id