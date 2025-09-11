-- Test that Hive CLUSTER keyword can be used as column name
-- This addresses the specific error: lower(cluster) should work

-- Test CLUSTER as column name
SELECT cluster FROM (VALUES ('cluster1'), ('cluster2')) AS t(cluster);
SELECT lower(cluster) FROM (VALUES ('CLUSTER1'), ('CLUSTER2')) AS t(cluster);

-- Test in complex expressions like the original error case
SELECT 
  concat(trim(BOTH FROM lower(cluster)), '_', trim(BOTH FROM lower(business_unit)), '_', CAST(f_7d1fe AS varchar)) AS f_c871f
FROM (VALUES ('CLUSTER1', 'BU1', 123), ('CLUSTER2', 'BU2', 456)) AS t(cluster, business_unit, f_7d1fe);