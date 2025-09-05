-- Test double-colon type casting syntax in SQL
SELECT 
  1::int AS int_cast,
  '42'::int AS str_to_int,
  NULL::int AS null_cast,
  (1 + 2)::bigint AS expr_cast