-- Test the original failing case
SELECT 
  CAST((CASE WHEN (t_94658.f_4cc4a IS NULL) 
            THEN LAG(t_94658.f_4cc4a) IGNORE NULLS OVER (PARTITION BY t_94658.f_76985 ORDER BY t_94658.f_b6270 ASC) 
            ELSE t_94658.f_4cc4a 
        END) AS varchar) f_714c9
FROM some_table t_94658;