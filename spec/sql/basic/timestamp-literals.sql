-- Test TIMESTAMP literal syntax support

-- Basic TIMESTAMP literal
SELECT TIMESTAMP '2022-03-16 12:00:00';

-- TIMESTAMP literal with timezone
SELECT TIMESTAMP '2022-03-16 12:00:00 Canada/Eastern';

-- TIMESTAMP literal in function call (the original failing case)
SELECT to_unixtime(TIMESTAMP '2022-03-16 12:00:00 Canada/Eastern');

-- TIMESTAMP literal in WHERE clause 
SELECT * FROM (VALUES (1), (2)) AS t(x) 
WHERE TIMESTAMP '2022-01-01 00:00:00' < TIMESTAMP '2022-12-31 23:59:59';

-- TIMESTAMP literal in comparison
SELECT TIMESTAMP '2022-03-16 12:00:00' <= TIMESTAMP '2022-03-16 13:00:00';

-- Mixed with DATE literals
SELECT DATE '2022-03-16', TIMESTAMP '2022-03-16 12:00:00';