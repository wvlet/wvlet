-- Test lambda expressions in SQL functions

-- Start with the simplest lambda without parentheses  
SELECT filter(ARRAY[1, 2, 3, NULL], x -> x);

-- Simple filter with lambda (without parentheses around parameter)  
SELECT filter(ARRAY[1, 2, 3, NULL], x -> x IS NOT NULL);

-- Lambda with parentheses around single parameter (original failing case)
SELECT filter(ARRAY[1, 2, 3, NULL], (x) -> x IS NOT NULL);

-- Lambda with parentheses around both parameter and body (fully parenthesized)
SELECT filter(ARRAY[1, 2, 3, NULL], (x) -> (x IS NOT NULL));

-- Test nested function with lambda (from original error pattern) 
SELECT filter(filter(ARRAY[1, 2, NULL, 4], (x) -> (x IS NOT NULL)), (y) -> (y > 1));

-- Test more complex lambda body
SELECT filter(ARRAY[1, 2, 3, 4, 5], (x) -> (x > 2 AND x < 5));
