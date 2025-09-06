-- Test case for nested parentheses in table references
-- This addresses parsing of ((table alias)) patterns

select * from ((information_schema.tables a1));