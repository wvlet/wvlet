SELECT 
    cardinality(ARRAY[1, 2, 3, 4, 5]) AS array_length,
    array_min(ARRAY[3, 1, 4, 1, 5]) AS min_value,
    array_max(ARRAY[3, 1, 4, 1, 5]) AS max_value