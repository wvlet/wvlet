-- Test cases for AT keyword used as table alias
-- Based on the MAP alias test pattern

-- Simple table alias
SELECT * FROM VALUES (1, 'test') as t(id, name), VALUES (2, 'at') at (id2, name2)

-- AT with explicit AS keyword  
SELECT * FROM VALUES (1, 'test') AS at

-- AT in JOIN clauses (the main issue)
SELECT * FROM VALUES (1, 'alice') t(id, name) 
LEFT JOIN VALUES (1, 'profile') AT ON (t.id = AT.id)

-- AT in complex subqueries
SELECT * FROM (SELECT * FROM VALUES (1, 'test') at) sub

-- AT in RIGHT JOIN
SELECT * FROM VALUES (1, 'alice') t(id, name) 
RIGHT JOIN VALUES (2, 'profile') at ON (t.id = at.id)

-- AT in INNER JOIN  
SELECT * FROM VALUES (1, 'alice') t(id, name) 
INNER JOIN VALUES (1, 'profile') at ON (t.id = at.id)

-- Multiple JOINs with AT
SELECT * FROM VALUES (1, 'alice') t(id, name) 
LEFT JOIN VALUES (1, 'profile') at ON (t.id = at.id)
LEFT JOIN VALUES (1, 'settings') s ON (t.id = s.id)

-- AT as column alias in field access
SELECT at.id, at.name FROM VALUES (1, 'test') at

-- Mixed case with both AT TIME ZONE and AT as alias 
-- (verify both contexts work)
SELECT at.id, TIMESTAMP '2023-01-01 12:00:00' AT TIME ZONE 'UTC' as utc_time
FROM VALUES (1, 'test') at