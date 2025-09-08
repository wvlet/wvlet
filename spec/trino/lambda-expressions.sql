-- Advanced lambda expressions for Trino (may not work in DuckDB)

-- Test lambda with no parameters  
SELECT transform(ARRAY[1], () -> 10);

-- Test lambda returning a complex type (array)
SELECT transform(ARRAY[1, 2], x -> ARRAY[x, x + 1]);

-- Test lambda returning a complex type (row/struct)
SELECT transform(ARRAY[1, 2], x -> ROW(x, CAST(x AS VARCHAR)));

-- Test lambda with multiple parameters
SELECT zip_with(ARRAY[1, 2, 3], ARRAY[4, 5, 6], (x, y) -> x + y);

-- Test reduce with lambda
SELECT reduce(ARRAY[1, 2, 3, 4], 0, (s, x) -> s + x, s -> s);

-- Test element_at with filter (from original error example)
SELECT element_at(filter(ARRAY[1, 2, NULL, 4], (x) -> (x IS NOT NULL)), 1);