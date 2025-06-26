-- Basic Hive queries that can be parsed by standard SQL parser

-- Simple aggregation (will be transformed to use collect_list in Hive)
SELECT 
    department,
    COUNT(*) as employee_count
FROM employees
GROUP BY department;

-- Filter with pattern matching
SELECT *
FROM logs
WHERE log_level = 'ERROR';

-- Join query
SELECT 
    o.order_id,
    o.order_date,
    c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01';

-- Subquery
SELECT 
    product_id,
    product_name,
    price
FROM products
WHERE price > (
    SELECT AVG(price) 
    FROM products
);

-- Window function
SELECT 
    employee_id,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank
FROM employees;