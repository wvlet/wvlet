![wvlet](logos/wvlet-banner-300.png)

Wvlet, pronounced as weave-let, is a new flow-style query language for SQL-based database engines, including [DuckDB](https://duckdb.org/), [Trino](https://trino.io/), etc.

Wvlet queries (.wv files) are designed to be a more natural way to describe data processing pipelines, which will eventually be compiled into SQL queries. While SQL is a powerful language to process data, its syntax often does not match the semantic order of data processing, which has confused users. Here is an illustration of the problem from _[A Critique of Modern SQL And A Proposal Towards A Simple and Expressive Query Language (CIDR '24)](https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf)_:
![semantic-order](docs/img/sql-semantic-order.png)

Wvlet queries aim to provide a more intuitive way to describe data processing pipelines by adopting the top-to-bottom syntax ordering for data processing.

# Features

Wvlet offers the following features to incorporate the best practices of the modern programming languages in the context of querying and managing data processing pipelines:

- [Flow-Style Syntax](#flow-style-syntax)
- [Reusable and Composable Models](#reusable-and-composable-models)
- [Extensible](#extensible)
- [Incremental Processing](#incremental-processing)

## Flow-Style Syntax

All of wvlet queries starts from a table scan statement `from`, and the result can be streamelined to the next processing steps using `where`, `group by`, `select`, etc.:
```sql
from (table)
where (filtering condition)
group by (grouping keys, ...)
select (columns to output)
order by (ordering columns...)
```
This flow style provides a natual way to describe data processing pipelines, and this style still can leverage the power of existing SQL-based DBMS engines as wvlet queries will be compiled into SQL queries. This flow-style query has also been adopted in [Google SQL](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/) too.

## Reusable and Composable Models

wvlet queries can be defined as a data model function, which can be reused for subsequent data processing. For example, you can define a data model function with `model` keyword:
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
Model will work as function templates to generate SQL queries depending on the input values. You can also compose models to build more complex queries:
```sql
from lookup(1) as p
join address_table on p.id = address_table.person_id
```

## Extensible

You can define your own functions to the existing data types to specify how to compile these functions int SQL. For example, you can extend the string type to handle null values:

```sql
type string:
  def or_else(default:string): string = sql"coalesce({this}, '{default}')"
end

-- Use or_else function to handle null values:
from person
select id, name.or_else("N/A") as name
```

You can also define table functions to pipe query results to the next processing steps:
```sql
from persons
-- Use your_table_function to process the input table results
pipe your_table_function(...)
limit 10
```

## Incremental Processing

(This feature will be available soon)

You can build a reproducible data processing pipeline with time-window based incremental processing.

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
create table if not exists as downstream_web_records;

insert into downstream_web_records
select time, user_id, path from upstrem_weblog_records
where
  -- incremental processing
  time > {last_watermark}
  and time <= {last_watermark} + interval '1 hour'
  -- the original filter to the subscribed data
  and user_id is not null;
```

# Quick Start

(To be updated)
