-- Test SHOW CATALOGS without LIKE clause
SHOW CATALOGS;

-- Test SHOW CATALOGS with LIKE clause  
SHOW CATALOGS LIKE 'td-presto';

-- Test SHOW CATALOGS with LIKE clause and wildcard pattern
SHOW CATALOGS LIKE '%catalog%';

-- Test SHOW CATALOGS with LIKE clause and simple pattern
SHOW CATALOGS LIKE 'system';