-- Test CAST JSON AS MAP with zone bracket accessor
-- This tests the fix for the parsing issue where 'zone' keyword failed in bracket expressions

-- Basic CAST with MAP type and zone access
SELECT CAST(JSON '{"zone": "us-west", "region": "california"}' AS MAP(VARCHAR, VARCHAR))['zone'] as zone_value;

-- CAST with MAP and different bracket accessors including zone using WITH clause
WITH data AS (
  SELECT CAST(JSON '{"zone": "us-west", "region": "california"}' AS MAP(VARCHAR, VARCHAR)) as m
)
SELECT
  m['zone'] as zone_val,
  m['region'] as region_val
FROM data;

-- CAST with MAP in WHERE clause using zone accessor
SELECT * FROM (VALUES (1)) t(x) 
WHERE CAST(JSON '{"zone": "active", "status": "ok"}' AS MAP(VARCHAR, VARCHAR))['zone'] = 'active';

-- Multiple MAP accesses with zone keyword
SELECT 
  CAST(JSON '{"zone": "prod", "status": "active"}' AS MAP(VARCHAR, VARCHAR))['zone'] as env_zone,
  CAST(JSON '{"zone": "test", "mode": "development"}' AS MAP(VARCHAR, VARCHAR))['zone'] as test_zone;
