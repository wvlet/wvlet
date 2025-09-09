-- Test aliased subqueries in JOIN clauses
-- This was failing with "Expected R_PAREN, but found DOUBLE_QUOTE_STRING"

-- Simple aliased subquery in LEFT JOIN with double quotes
SELECT *
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, name)
LEFT JOIN (
  SELECT * FROM (VALUES (1, 'x'), (3, 'y')) AS t2(id, value)
) "alias1" ON t1.id = alias1.id;

-- Aliased subquery without AS keyword
SELECT *
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, name)
LEFT JOIN (
  SELECT * FROM (VALUES (1, 'x'), (3, 'y')) AS t2(id, value)
) alias2 ON t1.id = alias2.id;

-- Complex nested subquery with alias using double underscores
SELECT *
FROM (VALUES (1, 10), (2, 20)) AS t1(id, amount)
LEFT JOIN (
  SELECT 
    id,
    SUM(value) as total
  FROM (VALUES (1, 5), (1, 10), (2, 15)) AS t3(id, value)
  GROUP BY id
) "__behavior" ON t1.id = __behavior.id;

-- Multiple aliased subqueries in joins
SELECT *
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, name)
LEFT JOIN (
  SELECT * FROM (VALUES (1, 'x')) AS t2(id, val)
) "first_sub" ON t1.id = first_sub.id
LEFT JOIN (
  SELECT * FROM (VALUES (2, 100)) AS t3(id, num)
) "second_sub" ON t1.id = second_sub.id;

-- Aliased subquery with AS keyword
SELECT *
FROM (
  SELECT * FROM (VALUES (1, 'test')) AS t1(id, name)
) AS "__audience"
LEFT JOIN (
  SELECT * FROM (VALUES (1, 'data')) AS t2(id, info)
) AS "__behavior" ON __audience.id = __behavior.id;