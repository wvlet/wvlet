-- Simplified test for CLUSTER BY in subquery
WITH t AS (
  SELECT cluster_id FROM (VALUES ('c1'), ('c2')) AS s(cluster_id)
  CLUSTER BY cluster_id
)
SELECT * FROM t;