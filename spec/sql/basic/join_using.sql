-- Test JOIN with USING clause (should not be confused with table sampling USING SAMPLE syntax)

-- Test basic JOIN USING syntax
WITH t1 AS (SELECT 1 as id, 'a' as data)
, t2 AS (SELECT 1 as id, 'b' as data)
SELECT * FROM 
(t1 d LEFT JOIN t2 s USING (id));

-- Test multiple JOIN USING variations  
WITH users AS (SELECT 1 as user_id, 1 as account_id, 'alice' as name)
, accounts AS (SELECT 1 as user_id, 1 as account_id, 'premium' as type)
SELECT * FROM users u INNER JOIN accounts a USING (user_id, account_id);