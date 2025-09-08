-- Test basic PREPARE statement with FROM keyword (Trino style)
PREPARE my_select1 FROM SELECT * FROM nation;

-- Test PREPARE statement with AS keyword (DuckDB style)  
PREPARE query_person AS SELECT * FROM person WHERE name = ?;

-- Test PREPARE with complex query and parameters
PREPARE my_select2 FROM SELECT name FROM nation WHERE regionkey = ? and nationkey < ?;

-- Test PREPARE with DuckDB $1 style parameters
PREPARE query_person AS SELECT * FROM person WHERE age >= $1 AND name = $2;

-- Test PREPARE with DuckDB named parameters
PREPARE query_person AS SELECT * FROM person WHERE age >= $minimum_age AND name = $name_start;

-- Test PREPARE with more complex DuckDB syntax
PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, ?) AND age >= ?;

-- Test PREPARE with DuckDB numbered parameters in different order
PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, $2) AND age >= $1;

-- Test PREPARE with DuckDB named parameters with descriptive names
PREPARE query_person AS SELECT * FROM person WHERE starts_with(name, $name_start_letter) AND age >= $minimum_age;

-- Test EXECUTE statement without parameters
EXECUTE my_select1;

-- Test EXECUTE statement with USING parameters (Trino style)
EXECUTE my_select2 USING 1, 3;

-- Test EXECUTE statement with parentheses parameters (DuckDB style)
EXECUTE query_person('B', 40);

-- Test EXECUTE with different parameter order (DuckDB style)
EXECUTE query_person(40, 'B');

-- Test DEALLOCATE statement
DEALLOCATE query_person;

-- Test DEALLOCATE with different statement name
DEALLOCATE my_select1;

-- Test DEALLOCATE with complex name
DEALLOCATE my_complex_query_name;