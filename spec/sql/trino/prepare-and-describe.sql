-- Test sequence of PREPARE and DESCRIBE INPUT statements
PREPARE my_query FROM SELECT ? FROM users WHERE id = ?;

DESCRIBE INPUT my_query;