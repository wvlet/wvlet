-- Hive data type examples

-- Array constructor
SELECT ARRAY[1, 2, 3] as numbers,
       ARRAY['a', 'b', 'c'] as letters;

-- Map constructor
SELECT MAP(ARRAY['key1', 'key2'], ARRAY['value1', 'value2']) as simple_map;

-- Complex nested structures
SELECT MAP(
    ARRAY['user_info', 'preferences'], 
    ARRAY[
        named_struct('name', 'John', 'age', 30),
        named_struct('theme', 'dark', 'language', 'en')
    ]
) as nested_data;

-- Struct creation
SELECT named_struct(
    'id', user_id,
    'profile', named_struct(
        'name', name,
        'email', email,
        'tags', ARRAY['active', 'premium']
    )
) as user_struct
FROM users;

-- VALUES clause without parentheses
INSERT INTO test_table
VALUES 
    (1, 'first'),
    (2, 'second'),
    (3, 'third');

-- Complex type in CREATE TABLE
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id STRING,
    user_id BIGINT,
    events ARRAY<STRUCT<
        timestamp: TIMESTAMP,
        event_type: STRING,
        properties: MAP<STRING, STRING>
    >>,
    metadata MAP<STRING, STRING>
) STORED AS PARQUET;