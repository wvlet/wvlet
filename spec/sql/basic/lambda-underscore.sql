-- Test underscore (_) argument in lambda expressions
-- This tests the fix for issue #1243

-- Simple case with underscore argument without parentheses
SELECT filter(ARRAY[1, 2, 3, NULL], _ -> _ IS NOT NULL);

-- Simple case with underscore argument with parentheses  
SELECT filter(ARRAY[1, 2, 3, NULL], (_) -> _ IS NOT NULL);

-- Complex lambda body with underscore
SELECT filter(ARRAY[1, 2, 3, 4, 5], _ -> _ > 2 AND _ < 5);

-- Test with array of different types
SELECT filter(ARRAY['a', 'b', NULL, 'c'], _ -> _ IS NOT NULL);

-- Test underscore with comparison operators  
SELECT filter(ARRAY[10, 20, 30, 40], _ -> _ >= 25);