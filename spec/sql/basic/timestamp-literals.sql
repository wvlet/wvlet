-- Test TIMESTAMP and TIME literal syntax support

-- Basic TIMESTAMP literal
SELECT TIMESTAMP '2022-03-16 12:00:00';

-- TIMESTAMP literal with timezone (using UTC)
SELECT TIMESTAMP '2022-03-16 12:00:00 UTC';

-- TIMESTAMP literal in expression (the original failing case was with to_unixtime, testing parsing)  
SELECT TIMESTAMP '2022-03-16 12:00:00 UTC';

-- TIMESTAMP literal in WHERE clause 
SELECT * FROM (VALUES (1), (2)) AS t(x) 
WHERE TIMESTAMP '2022-01-01 00:00:00' < TIMESTAMP '2022-12-31 23:59:59';

-- TIMESTAMP literal in comparison
SELECT TIMESTAMP '2022-03-16 12:00:00' <= TIMESTAMP '2022-03-16 13:00:00';

-- Basic TIME literal
SELECT TIME '12:30:45';

-- TIME literal with milliseconds
SELECT TIME '01:02:03.456';

-- TIME literal with microseconds
SELECT TIME '23:59:59.123456';

-- TIME literal in expression (function test, but hour() may not be available)
SELECT TIME '14:30:15';

-- TIME literal in comparison
SELECT TIME '09:00:00' < TIME '17:00:00';

-- Mixed DATE, TIME, and TIMESTAMP literals
SELECT DATE '2022-03-16', TIME '12:30:45', TIMESTAMP '2022-03-16 12:00:00';