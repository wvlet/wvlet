-- Test case reproducing exact TABLESAMPLE error from query logs
-- This addresses the complex pattern with parenthesized tables, TABLESAMPLE, and JOINs

SELECT COUNT_IF((NOT f_39ac8)) f_f1302
FROM
  (
   SELECT IF((t1.table_name IS NOT NULL), COALESCE((t1.table_name = t2.table_name), false), true) f_39ac8
   FROM
     (( information_schema.tables t1   ) TABLESAMPLE BERNOULLI (5)
   INNER JOIN information_schema.columns t2 ON (t1.table_name = t2.table_name))
UNION ALL    
   SELECT IF((t1.table_schema IS NOT NULL), COALESCE((t1.table_schema = t2.table_schema), false), true) f_39ac8
   FROM
     (( information_schema.tables t1   ) TABLESAMPLE BERNOULLI (5)
   INNER JOIN information_schema.columns t2 ON (t1.table_name = t2.table_name))
) ;