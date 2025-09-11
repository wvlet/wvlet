-- Test that Hive partition keywords can be used as column names
-- This addresses the error: lower(cluster) should work for CLUSTER, DISTRIBUTE, SORT

-- Test CLUSTER as column name
SELECT cluster FROM (VALUES ('cluster1'), ('cluster2')) AS t(cluster);
SELECT lower(cluster) FROM (VALUES ('CLUSTER1'), ('CLUSTER2')) AS t(cluster);

-- Test DISTRIBUTE as column name  
SELECT distribute FROM (VALUES ('dist1'), ('dist2')) AS t(distribute);
SELECT upper(distribute) FROM (VALUES ('dist1'), ('dist2')) AS t(distribute);

-- Test SORT as column name
SELECT sort FROM (VALUES ('asc'), ('desc')) AS t(sort);
SELECT length(sort) FROM (VALUES ('ascending'), ('descending')) AS t(sort);

-- Test all three together
SELECT cluster, distribute, sort 
FROM (VALUES ('c1', 'd1', 's1'), ('c2', 'd2', 's2')) AS t(cluster, distribute, sort);

-- Test in complex expressions like the original error case
SELECT 
  concat(trim(BOTH FROM lower(cluster)), '_', trim(BOTH FROM lower(business_unit)), '_', CAST(f_7d1fe AS varchar)) AS f_c871f
FROM (VALUES ('CLUSTER1', 'BU1', 123), ('CLUSTER2', 'BU2', 456)) AS t(cluster, business_unit, f_7d1fe);

-- Test nested function calls with these keywords
SELECT 
  concat(lower(cluster), '_', lower(distribute), '_', lower(sort)) AS combined
FROM (VALUES ('C1', 'D1', 'S1'), ('C2', 'D2', 'S2')) AS t(cluster, distribute, sort);