-- Test Map data types in SQL
-- Compatible with both Trino and DuckDB

-- Basic map creation
SELECT MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]) AS string_int_map;

-- Map with different value types
SELECT MAP(ARRAY['name', 'age'], ARRAY['Alice', '30']) AS mixed_map;

-- Map access
SELECT MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3])['a'] AS value_a;
SELECT MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3])['b'] AS value_b;

-- Map functions
SELECT cardinality(MAP(ARRAY['a', 'b'], ARRAY[1, 2])) AS map_size;
SELECT map_keys(MAP(ARRAY['x', 'y', 'z'], ARRAY[10, 20, 30])) AS keys;
SELECT map_values(MAP(ARRAY['x', 'y', 'z'], ARRAY[10, 20, 30])) AS values;

-- Map concatenation
SELECT map_concat(
    MAP(ARRAY['a'], ARRAY[1]), 
    MAP(ARRAY['b'], ARRAY[2])
) AS merged_map;

-- Map from entries
SELECT map_from_entries(ARRAY[('a', 1), ('b', 2), ('c', 3)]) AS entries_map;

-- Map aggregation
WITH data AS (
    SELECT 'group1' AS grp, 'key1' AS k, 10 AS v
    UNION ALL
    SELECT 'group1' AS grp, 'key2' AS k, 20 AS v
    UNION ALL
    SELECT 'group2' AS grp, 'key3' AS k, 30 AS v
)
SELECT 
    grp,
    map_agg(k, v) AS key_value_map
FROM data
GROUP BY grp;

-- Nested maps
SELECT MAP(
    ARRAY['user1', 'user2'], 
    ARRAY[
        MAP(ARRAY['name', 'age'], ARRAY['Alice', '25']),
        MAP(ARRAY['name', 'age'], ARRAY['Bob', '30'])
    ]
) AS nested_map;

-- Map transform
SELECT transform_keys(
    MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]),
    (k, v) -> upper(k)
) AS uppercase_keys;

SELECT transform_values(
    MAP(ARRAY['a', 'b', 'c'], ARRAY[1, 2, 3]),
    (k, v) -> v * 10
) AS multiplied_values;