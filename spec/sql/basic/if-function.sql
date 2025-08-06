-- Test if expression with lowercase
select
  if(1 > 0, 'true', 'false') as result1,
  if(0 > 1, 'true', 'false') as result2;

-- upper case IF
SELECT
  IF(1 > 0, 'true', 'false') as result1,
  IF(0 > 1, 'true', 'false') as result2;

-- Two-argument IF function, supported only in Trino
SELECT
  IF(1 > 0, 'condition is true') as result1,
  IF(0 > 1, 'condition is true') as result2;
