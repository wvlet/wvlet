-- Test Struct/Row data types in SQL
-- Compatible with both Trino and DuckDB

-- Basic struct creation using ROW constructor
SELECT ROW(1, 'Alice', 25) AS person_struct;

-- Named struct fields
SELECT CAST(ROW(1, 'Alice', 25) AS ROW(id INT, name VARCHAR, age INT)) AS named_struct;

-- Struct field access (using field position)
SELECT ROW(1, 'Alice', 25)[1] AS id;
SELECT ROW(1, 'Alice', 25)[2] AS name;
SELECT ROW(1, 'Alice', 25)[3] AS age;

-- Nested structs
SELECT ROW(
    'John',
    ROW('123 Main St', 'New York', 'NY', '10001')
) AS person_with_address;

-- Array of structs
SELECT ARRAY[
    ROW(1, 'Alice', 25),
    ROW(2, 'Bob', 30),
    ROW(3, 'Charlie', 35)
] AS people_array;

-- Map with struct values
SELECT MAP(
    ARRAY['emp1', 'emp2'],
    ARRAY[
        ROW('Alice', 'Engineering', 100000),
        ROW('Bob', 'Sales', 80000)
    ]
) AS employee_map;

-- Complex nested structure
SELECT ROW(
    'company1',
    ARRAY[
        ROW('dept1', ARRAY[ROW('Alice', 100000), ROW('Bob', 90000)]),
        ROW('dept2', ARRAY[ROW('Charlie', 95000), ROW('David', 85000)])
    ]
) AS company_structure;

-- Struct comparison
SELECT 
    ROW(1, 'a') = ROW(1, 'a') AS equal_structs,
    ROW(1, 'a') = ROW(1, 'b') AS unequal_structs;

-- Extract struct fields in SELECT
WITH data AS (
    SELECT ROW(1, 'Alice', 25) AS person
)
SELECT 
    person[1] AS id,
    person[2] AS name,
    person[3] AS age
FROM data;

-- Struct in GROUP BY
WITH sales_data AS (
    SELECT ROW('2024', '01') AS period, 100 AS amount
    UNION ALL
    SELECT ROW('2024', '01') AS period, 200 AS amount
    UNION ALL
    SELECT ROW('2024', '02') AS period, 150 AS amount
)
SELECT 
    period,
    SUM(amount) AS total_amount
FROM sales_data
GROUP BY period;