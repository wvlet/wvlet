-- Test CAST JSON AS MAP with zone bracket accessor
-- This tests the fix for the parsing issue where 'zone' keyword failed in bracket expressions

-- Basic CAST with MAP type and zone access
SELECT CAST(JSON '{"zone": "us-west", "region": "california"}' AS MAP(VARCHAR, VARCHAR))['zone'] as zone_value;

-- CAST with MAP and different bracket accessors including zone
SELECT 
  CAST(JSON '{"zone": "us-west", "region": "california"}' AS MAP(VARCHAR, VARCHAR))['zone'] as zone_val,
  CAST(JSON '{"zone": "us-west", "region": "california"}' AS MAP(VARCHAR, VARCHAR))['region'] as region_val;

-- CAST with MAP in WHERE clause using zone accessor
SELECT * FROM (VALUES (1)) t(x) 
WHERE CAST(JSON '{"zone": "active", "status": "ok"}' AS MAP(VARCHAR, VARCHAR))['zone'] = 'active';

-- Nested CAST with MAP zone access
SELECT CAST(
  CAST(JSON '{"data": {"zone": "prod", "env": "production"}}' AS MAP(VARCHAR, VARCHAR))['data'] 
  AS MAP(VARCHAR, VARCHAR)
)['zone'] as nested_zone;