-- Test SHOW CURRENT ROLES and SHOW ROLE GRANTS syntax support
-- This tests the parsing of role-related SHOW statements

-- Test SHOW CURRENT ROLES without FROM/IN clause
show current roles;

-- Test SHOW CURRENT ROLES FROM catalog
show current roles from test_catalog;

-- Test SHOW CURRENT ROLES IN catalog
show current roles in test_catalog;

-- Test SHOW ROLE GRANTS without FROM/IN clause
show role grants;

-- Test SHOW ROLE GRANTS FROM catalog
show role grants from test_catalog;

-- Test SHOW ROLE GRANTS IN catalog
show role grants in test_catalog;