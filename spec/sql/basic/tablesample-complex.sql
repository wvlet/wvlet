-- Test case reproducing exact TABLESAMPLE error from query logs
-- This addresses the complex pattern with parenthesized tables, TABLESAMPLE, and JOINs

SELECT COUNT_IF((NOT f_39ac8)) f_f1302
FROM
  (
   SELECT IF((t_01537.f_dc542 IS NOT NULL), COALESCE((t_01537.f_dc542 = t_dbabd.f_dc542), false), true) f_39ac8
   FROM
     (( information_schema.tables t1   ) TABLESAMPLE BERNOULLI (5)
   INNER JOIN information_schema.columns t2 ON (t1.table_name = t2.table_name))
UNION ALL    
   SELECT IF((t_01537.f_eef50 IS NOT NULL), COALESCE((t_01537.f_eef50 = t_dbabd.f_eef50), false), true) f_39ac8
   FROM
     (( information_schema.tables t1   ) TABLESAMPLE BERNOULLI (5)
   INNER JOIN information_schema.columns t2 ON (t1.table_name = t2.table_name))
) ;