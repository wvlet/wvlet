-- This is the exact pattern from the original issue that was failing
SELECT
  f_7f449,
  1755446400 f_b86fc,
  1755446400 f_86032
FROM (
  SELECT f_7f449
  FROM t_2ac11
  WHERE (date(cast(1755446400 as varchar)) >= DATE '2025-08-17')
)