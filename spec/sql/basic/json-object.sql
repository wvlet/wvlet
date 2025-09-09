-- Test JSON_OBJECT with simple alternating key-value (DuckDB style)
SELECT JSON_OBJECT('age', 30);
SELECT JSON_OBJECT('name', 'Alice', 'age', 25);
SELECT JSON_OBJECT('item_count', 10, 'ctr', 0.15);

-- Test JSON_OBJECT with KEY...VALUE syntax (Standard SQL style)
SELECT JSON_OBJECT(KEY 'age' VALUE 30);
SELECT JSON_OBJECT(KEY 'name' VALUE 'Alice', KEY 'age' VALUE 25);
SELECT JSON_OBJECT(KEY 'item_count' VALUE 10, KEY 'ctr' VALUE 0.15);

-- Test with column references (DuckDB style)
SELECT JSON_OBJECT('id', user_id, 'name', user_name)
FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS users(user_id, user_name);

-- Test with column references (Standard SQL style)
SELECT JSON_OBJECT(KEY 'id' VALUE user_id, KEY 'name' VALUE user_name)
FROM (VALUES (3, 'Charlie'), (4, 'Diana')) AS users2(user_id, user_name);

-- Complex example with KEY...VALUE syntax
SELECT
  col1,
  JSON_OBJECT(KEY 'age' VALUE age, KEY 'item_count' VALUE item_count, KEY 'ctr' VALUE ctr)
FROM
  (VALUES ('test', 25, 10, 0.15)) AS t(col1, age, item_count, ctr)
WHERE col1 IS NOT NULL;

-- Test with modifiers (Standard SQL)
SELECT JSON_OBJECT(KEY 'age' VALUE age NULL ON NULL)
FROM (VALUES (25), (NULL)) AS t(age);

-- Test ABSENT ON NULL modifier
SELECT JSON_OBJECT(KEY 'data' VALUE data, KEY 'info' VALUE info ABSENT ON NULL)
FROM (VALUES ('test', NULL), ('example', 'value')) AS t(data, info);

-- Test WITHOUT UNIQUE KEYS modifier  
SELECT JSON_OBJECT(KEY 'a' VALUE 1, KEY 'b' VALUE 2 WITHOUT UNIQUE KEYS);

-- Test WITH UNIQUE KEYS modifier
SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20 WITH UNIQUE KEYS);

-- Test multiple modifiers with multiple key-value pairs
SELECT JSON_OBJECT(
  KEY 'name' VALUE name, 
  KEY 'age' VALUE age, 
  KEY 'city' VALUE city 
  NULL ON NULL
)
FROM (VALUES ('Alice', 25, 'NYC'), ('Bob', NULL, 'LA')) AS users(name, age, city);

-- Test complex modifier combinations
SELECT JSON_OBJECT(
  KEY 'status' VALUE status,
  KEY 'count' VALUE count,
  KEY 'metadata' VALUE metadata
  ABSENT ON NULL WITHOUT UNIQUE KEYS
)  
FROM (VALUES ('active', 5, NULL), ('inactive', 0, '{}')) AS data(status, count, metadata);
