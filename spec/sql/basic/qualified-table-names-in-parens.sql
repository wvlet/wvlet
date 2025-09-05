-- Test case: Qualified table names with parentheses in FROM clause
-- This should now work with the parser fix

SELECT
  a.table_name,
  a.table_schema,
  _r1.column_name
FROM
  (information_schema.tables a
   LEFT JOIN information_schema.columns _r1 ON (a.table_name = _r1.table_name))
WHERE a.table_name = 'tables'
LIMIT 1;