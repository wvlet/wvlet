-- Test INSERT with parenthesized SELECT - this was failing with parser error
-- The goal is to verify the parser handles this syntax correctly

-- Clean up from previous runs
drop table if exists test_insert_paren;

-- Create test table using CREATE TABLE AS to populate it
create table test_insert_paren as
select * from (values (1, 'initial')) as t(id, value);

-- This used to fail with "Expected R_PAREN, but found STAR" 
-- Now it should parse and execute correctly
insert into test_insert_paren
(
  select * from (values (2, 'from_paren_select')) as t(id, value)
);

-- Test with specific columns from VALUES
insert into test_insert_paren
(
  select id, value 
  from (values (3, 'test3'), (4, 'test4')) as t(id, value)
  where id > 2
);

-- Verify normal syntax still works
insert into test_insert_paren (id, value)
select * from (values (5, 'normal_syntax')) as t(id, value);

-- Verify all inserts worked
select * from test_insert_paren order by id;

-- Clean up
drop table test_insert_paren;
