-- Test REPLACE function parsing

-- Basic REPLACE function usage
SELECT replace('hello world', 'world', 'SQL') as result1;

-- REPLACE with COALESCE (similar to the original error)
SELECT replace(COALESCE('test:string', '-'), ':', '') as result2;

-- REPLACE with TRY_CAST (from the original error pattern)
SELECT replace(COALESCE(TRY_CAST('123' AS varchar), '-'), ':', '') as result3;

-- Multiple REPLACE functions in SELECT
SELECT 
  replace(COALESCE('test:data', '-'), ':', '') as clean1,
  replace(COALESCE('more:data', '-'), ':', '') as clean2;

-- REPLACE in complex expressions
SELECT 
  f_id,
  replace(COALESCE(TRY_CAST(f_value AS varchar), '-'), ':', '') as cleaned_value
FROM VALUES (1, '123:456'), (2, 'abc:def') AS t(f_id, f_value);