-- Test UNION queries inside parenthesized subqueries
CREATE TABLE test_table AS SELECT *
FROM
  (
    SELECT 1 as id, 'first' as name
    UNION
    SELECT 2 as id, 'second' as name
    UNION
    SELECT 3 as id, 'third' as name
  )
ORDER BY id ASC