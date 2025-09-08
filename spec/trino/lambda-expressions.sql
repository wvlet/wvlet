-- Trino-only: test multi-parameter lambda inside functions

-- zip_with is a Trino function that accepts a two-argument lambda
SELECT zip_with(ARRAY[1, 2], ARRAY[3, 4], (x, y) -> x + y);

