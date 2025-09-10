-- Test ROW types with named fields
-- ROW type with named fields in CAST expression  
-- Use non-keyword field names first
SELECT CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] AS array(ROW(id bigint, name varchar)));

-- ROW type with multiple named fields
-- Provide inline row value as column x
SELECT CAST(x AS ROW(id integer, name varchar, active boolean))
FROM (VALUES (ROW(1, 'a', TRUE))) AS t(x);

-- Test MAP function with variadic syntax
-- Variadic MAP with key-value pairs
SELECT map('a', 1, 'b', 2, 'c', 3);

-- Two-array form (traditional)
SELECT map(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]);

-- Complex example with lambda and ROW types
-- Use DuckDB-style list_transform for lambda mapping over arrays
SELECT list_transform(
  CAST(ARRAY[ROW(1, 'a'), ROW(2, 'b')] AS array(ROW(k bigint, v varchar))),
  (e) -> concat(concat(CAST(e.k AS varchar), ':'), e.v)
);

-- MAP with complex expressions as values
-- Aggregations over inline VALUES to provide input column
SELECT MAP {'count': COUNT(*), 'sum': SUM(v), 'avg': AVG(v)}
FROM (VALUES (10), (20), (30)) AS src(v);

-- Nested ROW types
-- Nested row value via inline VALUES
SELECT CAST(x AS ROW(id integer, details ROW(name varchar, age integer)))
FROM (VALUES (ROW(1, ROW('bob', 42)))) AS t(x);
