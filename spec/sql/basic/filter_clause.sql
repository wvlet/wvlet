-- Test FILTER clause support for aggregate functions

-- Basic FILTER clause with array_agg
SELECT array_agg(value) FILTER (WHERE value > 10) 
FROM (VALUES (5), (15), (8), (20), (3)) AS t(value);

-- FILTER clause with multiple conditions
SELECT 
    count(*) FILTER (WHERE status = 'active') as active_count,
    sum(amount) FILTER (WHERE status = 'completed' AND amount > 0) as completed_sum
FROM (VALUES 
    ('active', 100),
    ('completed', 200),
    ('active', 150),
    ('pending', 50),
    ('completed', -10)
) AS t(status, amount);

-- Complex nested expression in FILTER
SELECT array_agg(concat(concat(col1, ' '), col2)) 
    FILTER (WHERE ((COALESCE(col1, '') <> '') AND (COALESCE(col2, '') <> '')))
FROM (VALUES 
    ('field1', 'type1'),
    (NULL, 'type2'),
    ('field3', NULL),
    ('field4', 'type4')
) AS t(col1, col2);

-- FILTER with GROUP BY
SELECT 
    category,
    count(*) FILTER (WHERE price > 100) as expensive_items,
    avg(price) FILTER (WHERE discount > 0) as avg_discounted_price
FROM (VALUES 
    ('electronics', 150, 10),
    ('electronics', 80, 0),
    ('clothing', 120, 15),
    ('clothing', 50, 5),
    ('food', 20, 0)
) AS t(category, price, discount)
GROUP BY category;

-- Nested function calls with FILTER
SELECT array_agg(concat(concat(field_name, ' '), field_type)) 
    FILTER (WHERE ((COALESCE(field_name, '') <> '') AND (COALESCE(field_type, '') <> '')))
FROM (VALUES 
    ('id', 'bigint'),
    ('name', 'varchar'),
    (NULL, 'timestamp'),
    ('status', NULL),
    ('created_at', 'timestamp')
) AS t(field_name, field_type);