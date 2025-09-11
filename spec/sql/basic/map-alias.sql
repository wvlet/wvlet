-- Test using MAP as table alias
-- This should parse successfully after fixing the parser

-- Simple table alias with VALUES
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) map;

-- Explicit AS alias
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) AS map;

-- JOIN with MAP alias
SELECT * 
FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name)
LEFT JOIN (VALUES (1, 'Engineer'), (2, 'Designer')) AS map(user_id, job) ON (u.id = map.user_id);

-- Complex query with MAP alias in subquery and main query
SELECT u.name, map.bio
FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name)
LEFT JOIN (
  SELECT user_id, bio 
  FROM (VALUES (1, 'Tech enthusiast'), (2, 'Creative mind')) AS profiles(user_id, bio)
) map ON (u.id = map.user_id);

-- Multiple tables with one having MAP alias
SELECT u.name, map.job, c.company_name
FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name)
LEFT JOIN (VALUES (1, 'Engineer'), (2, 'Designer')) AS map(user_id, job) ON (u.id = map.user_id)
LEFT JOIN (VALUES (1, 'TechCorp'), (2, 'DesignCo')) AS c(id, company_name) ON (u.id = c.id);

-- MAP alias in RIGHT JOIN
SELECT map.job, u.name
FROM (VALUES (1, 'Engineer'), (2, 'Designer')) AS map(user_id, job)
RIGHT JOIN (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name) ON (map.user_id = u.id);

-- MAP alias with column references
SELECT map.id, map.job, map.salary
FROM (VALUES (1, 'Engineer', 100000), (2, 'Designer', 80000)) AS map(id, job, salary)
WHERE map.salary > 50000;

-- MAP as a column alias
SELECT u.id AS map FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name);

-- MAP as a column alias for a literal
SELECT 123 AS map;

-- MAP as a column alias in complex expressions
SELECT CONCAT(u.name, ' - ID:', CAST(u.id AS VARCHAR)) AS map
FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS u(id, name);

-- Test with uppercase MAP alias
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) MAP;

-- Test with mixed-case MAP alias
SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(id, name) mAp;