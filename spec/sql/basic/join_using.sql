-- Test JOIN with USING clause (should not be confused with table sampling USING SAMPLE syntax)

CREATE TABLE test AS 
WITH t1 AS (SELECT 1 as id, 'a' as data)
, t2 AS (SELECT 1 as id, 'b' as data)
SELECT * FROM 
(t1 d LEFT JOIN t2 s USING (id));

-- Test multiple variations of JOIN USING
SELECT * FROM table1 t1 LEFT JOIN table2 t2 USING (data_set_id);

SELECT * FROM table1 t1 INNER JOIN table2 t2 USING (user_id, account_id);

-- Ensure table sampling with USING SAMPLE still works
SELECT * FROM table1 USING SAMPLE 10 PERCENT;