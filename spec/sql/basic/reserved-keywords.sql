-- Test case for using reserved keyword "key" as a column name

-- Test with inline VALUES data
select key from (values ('test_key')) as table1(key);

select
  key,
  value
from (values ('k1', 'v1'), ('k2', 'v2')) as table1(key, value);

-- Key used with table alias
select t.key, t.value
from (values ('k1', 'v1'), ('k2', 'v2')) as t(key, value);

-- Key in WHERE clause
select * from (values ('test', 'data1'), ('other', 'data2')) as table1(key, value)
where key = 'test';

-- Key in GROUP BY
select table1.key, count(*) as cnt
from (values ('k1'), ('k1'), ('k2')) as table1(key)
group by table1.key;

-- Key with AS alias
select key as key_column
from (values ('test_key')) as table1(key);

-- Multiple tables with key column
select t1.key, t2.key
from (values ('k1'), ('k2')) as t1(key)
join (values ('k1'), ('k3')) as t2(key) on t1.key = t2.key;

-- Test case for using reserved keyword "system" as part of qualified table names
-- Note: These would require actual system tables to execute, so they're testing parsing only

-- Simple query from system.runtime.nodes (parsing test only)
-- SELECT * FROM system.runtime.nodes;

-- System as catalog with three-part name (parsing test only)  
-- SELECT * FROM system.information_schema.tables;

-- Test system as identifier in VALUES clause
select system from (values ('test_system')) as t(system);

-- Test case for using reserved keyword "table" as part of qualified table names
select table from (values ('test_table')) as t(table);

-- Test case for using reserved keyword "schema" as part of qualified table names
select schema from (values ('test_schema')) as t(schema);