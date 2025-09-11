-- Test using MAP as table alias
-- This should parse successfully after fixing the parser

-- Simple table alias
SELECT * FROM users map;

-- Explicit AS alias
SELECT * FROM users AS map;

-- JOIN with MAP alias
SELECT * 
FROM users u 
LEFT JOIN profiles map ON (u.id = map.user_id);

-- Complex query with MAP alias in subquery and main query
SELECT u.name, map.bio
FROM users u
LEFT JOIN (
  SELECT user_id, bio 
  FROM profiles 
  WHERE active = true
) map ON (u.id = map.user_id)
WHERE u.status = 'active';

-- Multiple tables with one having MAP alias
SELECT u.name, map.bio, c.company_name
FROM users u
LEFT JOIN profiles map ON (u.id = map.user_id)  
LEFT JOIN companies c ON (u.company_id = c.id);

-- MAP alias in RIGHT JOIN
SELECT map.bio, u.name
FROM profiles map
RIGHT JOIN users u ON (map.user_id = u.id);

-- MAP alias with column references
SELECT map.id, map.bio, map.created_at
FROM profiles map
WHERE map.active = true;

-- MAP as a column alias
SELECT u.id AS map FROM users u;

-- MAP as a column alias for a literal
SELECT 123 AS map;

-- MAP as a column alias in complex expressions
SELECT CONCAT(u.first_name, ' ', u.last_name) AS map
FROM users u;