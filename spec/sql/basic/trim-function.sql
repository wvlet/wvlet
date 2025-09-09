-- Test TRIM function with Trino syntax
-- https://github.com/wvlet/wvlet/issues/1231

-- Basic TRIM (default removes spaces from both sides)
SELECT trim('  hello  ');

-- TRIM with specific characters FROM
SELECT trim('!' FROM '!foo!');

-- LEADING option - removes from the beginning
SELECT trim(LEADING FROM '  abcd');
SELECT trim(LEADING ' ' FROM '  abcd');

-- TRAILING option - removes from the end  
SELECT trim(TRAILING FROM 'abcd  ');
SELECT trim(TRAILING ' ' FROM 'abcd  ');

-- BOTH option (default) - removes from both sides
SELECT trim(BOTH FROM '  abcd  ');
SELECT trim(BOTH ' ' FROM '  abcd  ');
SELECT trim(BOTH '$' FROM '$var$');

-- TRIM with multi-character patterns
SELECT trim(TRAILING 'ER' FROM upper('worker'));

-- Nested function calls with TRIM
SELECT trim(BOTH '0' FROM concat('00', '123', '00'));

-- TRIM in WHERE clause
SELECT * FROM (VALUES ('  test  '), ('test'), ('  test')) AS t(col)
WHERE trim(col) = 'test';