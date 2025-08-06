-- Test case for using reserved keyword "key" as a column name

select key from table1;

select
  key,
  value
from table1;

-- Key used with table alias
select t.key, t.value
from table1 t;

-- Key in WHERE clause
select * from table1
where key = 'test';

-- Key in GROUP BY
select key, count(*)
from table1
group by key;

-- Key with AS alias
select key as key_column
from table1;

-- Multiple tables with key column
select t1.key, t2.key
from table1 t1
join table2 t2 on t1.key = t2.key;


-- Test case for using reserved keyword "system" as part of qualified table names

-- Simple query from system.runtime.nodes
SELECT * FROM system.runtime.nodes;

-- System as catalog with three-part name
SELECT * FROM system.information_schema.tables;
