-- Test LAG function with IGNORE NULLS (Trino style)
SELECT LAG(col) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAG function with RESPECT NULLS (Trino style)  
SELECT LAG(col) RESPECT NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD function with IGNORE NULLS (Trino style)
SELECT LEAD(col) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD function with RESPECT NULLS (Trino style)
SELECT LEAD(col) RESPECT NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAG with offset and default value
SELECT LAG(col, 2, 0) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD with offset and default value
SELECT LEAD(col, 1, 'default') RESPECT NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test FIRST_VALUE with IGNORE NULLS
SELECT FIRST_VALUE(col) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAST_VALUE with IGNORE NULLS  
SELECT LAST_VALUE(col) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test NTH_VALUE with IGNORE NULLS
SELECT NTH_VALUE(col, 2) IGNORE NULLS OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Complex case: LAG within CASE expression
SELECT 
  CASE WHEN LAG(status) IGNORE NULLS OVER (PARTITION BY user_id ORDER BY created_at) = 'active' 
       THEN 'returning_user'
       ELSE 'new_user'
  END as user_type
FROM user_events;