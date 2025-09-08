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