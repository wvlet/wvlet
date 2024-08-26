# wvlet-ql

The wvlet, pronounced as weave-let, is a new flow-style query language for SQL-based database engines. 

wvlet queries (.wv) are designed to provide a more natural way to describe data processing pipelines, which can be compiled into SQL queries.

- Flow-style: All of wvlet queries starts from a table scan statement `from`, and the result can be streamelined to the next processing steps using `where`, `group by`, `select`, etc.:
```sql
from (table)
where (filtering condition)
group by (grouping keys, ...)
select (columns to output)
order by (ordering columns...)
```
This flow style provides a natual way to describe data processing pipelines, and this style still can leverage the power of existing SQL-based DBMS engines as wvlet queries will be compiled into SQL queries.
- Reusable: wvlet queries can be defined as a data model function, which can be reused for subsequent data processing. For example, you can define a data model function:
```sql
model lookup(person_id: int) =
  from persons
  where id = person_id
end
```

Calling this model, e.g., `from lookup(1)`, will issue this SQL query:
```sql
-- {person_id} will be replaced with the given input 1
select * from persons
where id = 1
```
Model will work as function templates to generate SQL queries depending on the input values.

- Composable. model functions can be reused for subsequent data processing:
```sql
from lookup(1) as p
join address_table on p.id = address_table.person_id
```

- Reproducible. You can build a reproducible data processing pipeline with time-window based incremental processing.

```sql
@config(watermark_column='time', window_size='1h')
model weblog_records =
  from upstream_web_records
  select time, user_id, path,
end

-- Subscribe newly inserted records in weblog_records
-- and save filtered records to downstream_web_records
subscribe weblog_records
where user_id is not null
insert into downstream_web_records
```

This wvlet query will be compiled into the following SQL queries to update the downstream_web_records table:
```sql
create table if not exits as downstream_web_records;

insert into downstream_web_records
select time, user_id, path from upstrem_weblog_records
where
  -- incremental processing
  time > {last_watermark}
  and time <= {last_watermark} + interval '1 hour'
  -- the original filter to the subscribed data
  and user_id is not null;
```

- Extensible. You can define your own functions to the existing data types to specify how to compile these functions int SQL. For example, you can handle null values in string data types as follows:

```sql
type string:
  def or_else(default:string) = sql"coalesce({this}, default)"
end

-- Use or_else function to handle null values:
from person
select id, name.or_else("N/A") as name
```

# Quick Start

