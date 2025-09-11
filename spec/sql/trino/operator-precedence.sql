-- Test operator precedence for UNION, INTERSECT, and EXCEPT
-- According to SQL standard: INTERSECT has higher precedence than UNION
-- A INTERSECT B UNION C should be parsed as (A INTERSECT B) UNION C

-- Test INTERSECT with UNION precedence
SELECT * FROM (
  SELECT 1 as x
  INTERSECT 
  SELECT 1 as x
  UNION
  SELECT 2 as x
);

-- Test EXCEPT with UNION precedence  
SELECT * FROM (
  SELECT 1 as x
  EXCEPT
  SELECT 2 as x  
  UNION
  SELECT 3 as x
);

-- Test multiple operations
SELECT * FROM (
  SELECT 1 as x
  UNION
  SELECT 2 as x
  INTERSECT
  SELECT 2 as x
  EXCEPT
  SELECT 3 as x
);