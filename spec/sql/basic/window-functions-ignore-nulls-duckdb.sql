-- Test LAG function with IGNORE NULLS (DuckDB style)
SELECT LAG(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, 'b', '2024-01-02')) AS t(id, col, ts);

-- Test LAG function with RESPECT NULLS (DuckDB style)  
SELECT LAG(col RESPECT NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, NULL, '2024-01-02')) AS t(id, col, ts);

-- Test LEAD function with IGNORE NULLS (DuckDB style)
SELECT LEAD(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, 'b', '2024-01-02')) AS t(id, col, ts);

-- Test LEAD function with RESPECT NULLS (DuckDB style)
SELECT LEAD(col RESPECT NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, NULL, '2024-01-02')) AS t(id, col, ts);

-- Test LAG with offset and default value (DuckDB style)
SELECT LAG(col IGNORE NULLS, 2, 0) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 10, '2024-01-01'), (1, 20, '2024-01-02')) AS t(id, col, ts);

-- Test LEAD with offset and default value (DuckDB style)
SELECT LEAD(col RESPECT NULLS, 1, 'default') OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, 'b', '2024-01-02')) AS t(id, col, ts);

-- Test FIRST_VALUE with IGNORE NULLS (DuckDB style)
SELECT FIRST_VALUE(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, 'b', '2024-01-02')) AS t(id, col, ts);

-- Test LAST_VALUE with IGNORE NULLS (DuckDB style)
SELECT LAST_VALUE(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM (VALUES (1, 'a', '2024-01-01'), (1, 'b', '2024-01-02')) AS t(id, col, ts);