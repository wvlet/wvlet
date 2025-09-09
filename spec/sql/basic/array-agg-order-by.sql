-- Test array_agg with ORDER BY syntax
-- https://github.com/wvlet/wvlet/issues/1233

-- Basic array_agg with ORDER BY
SELECT array_agg(x ORDER BY x) FROM (VALUES (3), (1), (2)) AS t(x);

-- array_agg with ORDER BY DESC
SELECT array_agg(x ORDER BY x DESC) FROM (VALUES (3), (1), (2)) AS t(x);

-- array_agg with multiple ORDER BY columns
SELECT array_agg(name ORDER BY score DESC, name ASC) 
FROM (VALUES ('Alice', 85), ('Bob', 92), ('Charlie', 85)) AS t(name, score);

-- array_agg with complex expressions in ORDER BY
SELECT array_agg(name ORDER BY length(name), name)
FROM (VALUES ('Jo'), ('Alice'), ('Bob'), ('Al')) AS t(name);

-- array_agg with ORDER BY and GROUP BY
SELECT 
    category,
    array_agg(product ORDER BY price DESC) as products
FROM (
    VALUES 
        ('Electronics', 'Phone', 999),
        ('Electronics', 'Tablet', 599),
        ('Electronics', 'Laptop', 1299),
        ('Books', 'Novel', 15),
        ('Books', 'Textbook', 85),
        ('Books', 'Magazine', 5)
) AS t(category, product, price)
GROUP BY category
ORDER BY category;

-- array_agg with DISTINCT and ORDER BY
SELECT array_agg(DISTINCT x ORDER BY x) 
FROM (VALUES (1), (2), (1), (3), (2)) AS t(x);

-- Multiple aggregates with ORDER BY
SELECT 
    array_agg(x ORDER BY x) as sorted_asc,
    array_agg(x ORDER BY x DESC) as sorted_desc,
    array_agg(x ORDER BY abs(x - 5)) as sorted_by_distance
FROM (VALUES (1), (3), (5), (7), (9)) AS t(x);

-- array_agg with ORDER BY in subquery
SELECT * FROM (
    SELECT array_agg(x ORDER BY x) as sorted_array
    FROM (VALUES (5), (2), (8), (1)) AS t(x)
) sub;

-- array_agg with NULL handling in ORDER BY
SELECT array_agg(x ORDER BY x NULLS FIRST)
FROM (VALUES (3), (NULL), (1), (NULL), (2)) AS t(x);

SELECT array_agg(x ORDER BY x NULLS LAST)
FROM (VALUES (3), (NULL), (1), (NULL), (2)) AS t(x);