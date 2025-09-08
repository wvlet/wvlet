-- Basic TRY_CAST functionality tests

-- Simple TRY_CAST with string to integer
SELECT TRY_CAST('123' AS bigint) as result1;

-- TRY_CAST with invalid conversion (should return NULL)
SELECT TRY_CAST('abc' AS bigint) as result2;

-- TRY_CAST in SELECT with column alias
SELECT 
  TRY_CAST(regexp_extract('job_id:12345', 'job_id:(.*)', 1) AS bigint) as job_id,
  TRY_CAST('42' AS int) as number;

-- TRY_CAST with function call
SELECT TRY_CAST(substring('test123', 5) AS int) as extracted_num;

-- TRY_CAST with various data types
SELECT TRY_CAST('3.14159' AS double) as pi_double;
SELECT TRY_CAST('123.456' AS decimal) as decimal_num;

-- TRY_CAST with NULL input
SELECT TRY_CAST(NULL AS int) as null_result;

-- TRY_CAST in WHERE clause
SELECT * FROM VALUES (1, 'valid'), (2, 'invalid') AS t(id, str_val) 
WHERE TRY_CAST(t.str_val AS int) IS NULL;

-- TRY_CAST with edge cases and out-of-range values
SELECT TRY_CAST('999999999999999999999' AS int) as overflow_result;