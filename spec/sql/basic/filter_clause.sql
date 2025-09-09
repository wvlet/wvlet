-- Test FILTER clause support for aggregate functions

-- Basic FILTER clause with array_agg
SELECT array_agg(value) FILTER (WHERE value > 10) 
FROM test_table;

-- FILTER clause with multiple conditions
SELECT 
    count(*) FILTER (WHERE status = 'active') as active_count,
    sum(amount) FILTER (WHERE status = 'completed' AND amount > 0) as completed_sum
FROM orders;

-- Complex nested expression in FILTER
SELECT array_agg(concat(concat(f_33448, ' '), f_cfea5)) 
    FILTER (WHERE ((COALESCE(f_33448, '') <> '') AND (COALESCE(f_cfea5, '') <> '')))
FROM test_data;

-- FILTER with GROUP BY
SELECT 
    category,
    count(*) FILTER (WHERE price > 100) as expensive_items,
    avg(price) FILTER (WHERE discount > 0) as avg_discounted_price
FROM products
GROUP BY category;

-- Nested function calls with FILTER
SELECT concat(
    concat(
        concat('create table if not exists test_table (', chr(10)), 
        ARRAY_JOIN(
            concat(
                array_agg(concat(concat(f_33448, ' '), f_cfea5)) 
                    FILTER (WHERE ((COALESCE(f_33448, '') <> '') AND (COALESCE(f_cfea5, '') <> ''))),
                ARRAY['created_by varchar','created_date bigint']
            ), 
            concat(',', chr(10))
        )
    ), 
    ')'
) as table_definition
FROM metadata_table;