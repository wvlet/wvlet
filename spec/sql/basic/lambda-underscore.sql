-- Test underscore (_) argument in lambda expressions
-- This tests the fix for issue #1243

-- Simple case with underscore argument without parentheses
SELECT filter(ARRAY[1, 2, 3, NULL], _ -> _ IS NOT NULL);

-- Simple case with underscore argument with parentheses  
SELECT filter(ARRAY[1, 2, 3, NULL], (_) -> _ IS NOT NULL);

-- Complex lambda body with underscore
SELECT filter(ARRAY[1, 2, 3, 4, 5], _ -> _ > 2 AND _ < 5);

-- Nested function case from original issue
SELECT map_values(map_filter(ARRAY[1, 2, 3, 4], (k, _) -> contains(ARRAY[2, 4], k)));

-- Mixed usage of underscore and named parameters
SELECT transform(ARRAY[1, 2, 3], (x, _) -> x + 1);