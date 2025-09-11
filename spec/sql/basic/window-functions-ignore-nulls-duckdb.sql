-- Test LAG function with IGNORE NULLS (DuckDB style)
SELECT LAG(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAG function with RESPECT NULLS (DuckDB style)  
SELECT LAG(col RESPECT NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD function with IGNORE NULLS (DuckDB style)
SELECT LEAD(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD function with RESPECT NULLS (DuckDB style)
SELECT LEAD(col RESPECT NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAG with offset and default value (DuckDB style)
SELECT LAG(col, 2, 0 IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LEAD with offset and default value (DuckDB style)
SELECT LEAD(col, 1, 'default' RESPECT NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test FIRST_VALUE with IGNORE NULLS (DuckDB style)
SELECT FIRST_VALUE(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;

-- Test LAST_VALUE with IGNORE NULLS (DuckDB style)
SELECT LAST_VALUE(col IGNORE NULLS) OVER (PARTITION BY id ORDER BY ts)
FROM table1;