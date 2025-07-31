-- Test combinations of Array, Map, and Struct types
-- Compatible with both Trino and DuckDB

-- Array of Maps
SELECT ARRAY[
    MAP(ARRAY['name', 'score'], ARRAY['Alice', '95']),
    MAP(ARRAY['name', 'score'], ARRAY['Bob', '87']),
    MAP(ARRAY['name', 'score'], ARRAY['Charlie', '92'])
] AS student_scores;

-- Map of Arrays
SELECT MAP(
    ARRAY['math', 'science', 'english'],
    ARRAY[
        ARRAY[90, 85, 88],
        ARRAY[92, 89, 91],
        ARRAY[85, 87, 90]
    ]
) AS subject_scores;

-- Struct with Array and Map fields
SELECT CAST(
    ROW(
        'user123',
        ARRAY['read', 'write', 'execute'],
        MAP(ARRAY['email', 'phone'], ARRAY['user@example.com', '+1234567890'])
    ) AS ROW(
        user_id VARCHAR,
        permissions ARRAY(VARCHAR),
        contacts MAP(VARCHAR, VARCHAR)
    )
) AS user_profile;

-- Complex nested query with aggregation
WITH orders AS (
    SELECT 
        'customer1' AS customer_id,
        ARRAY['item1', 'item2'] AS items,
        MAP(ARRAY['item1', 'item2'], ARRAY[10.5, 20.0]) AS prices
    UNION ALL
    SELECT 
        'customer1' AS customer_id,
        ARRAY['item3'] AS items,
        MAP(ARRAY['item3'], ARRAY[15.5]) AS prices
    UNION ALL
    SELECT 
        'customer2' AS customer_id,
        ARRAY['item1', 'item4'] AS items,
        MAP(ARRAY['item1', 'item4'], ARRAY[10.5, 25.0]) AS prices
)
SELECT 
    customer_id,
    array_agg(items) AS all_items,
    map_agg(
        concat('order', cast(row_number() OVER (PARTITION BY customer_id ORDER BY customer_id) AS varchar)),
        prices
    ) AS order_prices
FROM orders
GROUP BY customer_id;

-- Transform complex types
SELECT 
    transform(
        ARRAY[
            MAP(ARRAY['x', 'y'], ARRAY[1, 2]),
            MAP(ARRAY['x', 'y'], ARRAY[3, 4])
        ],
        m -> transform_values(m, (k, v) -> v * 10)
    ) AS scaled_coordinates;

-- Filter arrays of structs
WITH employees AS (
    SELECT ARRAY[
        ROW('Alice', 'Engineering', 120000),
        ROW('Bob', 'Sales', 80000),
        ROW('Charlie', 'Engineering', 110000),
        ROW('David', 'Sales', 90000)
    ] AS emp_list
)
SELECT 
    filter(emp_list, emp -> emp[3] > 100000) AS high_earners
FROM employees;

-- JSON-like structure using nested types
SELECT MAP(
    ARRAY['users', 'settings'],
    ARRAY[
        CAST(ARRAY[
            MAP(ARRAY['id', 'name', 'active'], ARRAY['1', 'Alice', 'true']),
            MAP(ARRAY['id', 'name', 'active'], ARRAY['2', 'Bob', 'false'])
        ] AS VARCHAR),
        CAST(MAP(
            ARRAY['theme', 'notifications'],
            ARRAY['dark', 'enabled']
        ) AS VARCHAR)
    ]
) AS app_data;