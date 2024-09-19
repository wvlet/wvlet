---
sidebar_label: Introduction
sidebar_position: 1
title: Introduction
---


# Introduction

Wvlet, pronounced as weave-let, is a new flow-style query language for SQL-based database engines, including [DuckDB](https://duckdb.org/), [Trino](https://trino.io/), etc.


## Documentation

- [Installation](./usage/install)
- [Query Syntax](./syntax/)
- [Usage](./usage/repl)


## Why Wvlet?

Wvlet queries (saved as .wv files) provide a natural way to describe data processing pipelines, which will eventually be compiled into a sequence of SQL queries. While SQL is a powerful language for processing data, its syntax often does not match the semantic order of data processing. Lets see the following example: The syntactic order of SQL's SELECT ... statements mismatches with the actual data flow inside the SQL engines (cited from _[A Critique of Modern SQL And A Proposal Towards A Simple and Expressive Query Language (CIDR '24)](https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf)_):

<center>
![semantic-order](./img/sql-semantic-order.png)
</center>

For overcoming this shortcoming of SQL, wvlet start from a table scan statement `from ...`, and the result can be streamlined to the next processing operators like `where`, `group by`, `select`, etc., as if passing table values through [a UNIX pipe](https://en.wikipedia.org/wiki/Pipeline_(Unix)) to the next operator:

```sql
-- Starts from table scan
from 'table'
where column = 'filtering condition'
...
where (more filtering condition can be added)
group by key1, key2, ...
-- HAVING clause can be just where clause after group by
where (conditions for groups)
-- columns to output
select c1, c2, ...
order by (ordering columns...)
```
With this flow style, you can describe data processing pipelines in a natural order to create complex queries. You can also add operators for testing or debugging data in the middle of the query. This flow syntax is gaining traction and has been adopted in Google's SQL to simplify writing SQL queries. For more details, see _[SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL (VLDB 2024)](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/)_.

## Architecture

![wvlet-architecture](./img/wvlet-architecture.svg)


# Features

Wvlet offers the following features to incorporate the best practices of the modern programming languages in the context of querying and managing data processing pipelines:

- [Functional Data Modeling](#functional-data-modeling)
- [Extensible Types](#extensible-types)
- [Chaining Function Calls](#chaining-function-calls)
- [Incremental Processing](#incremental-processing)


## Funcitonal Data Modeling

In Wvlet, you can define queries as data model functions, which can be reused for subsequent data processing. The following example defines a data `model` function:
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
Models in Wvlet will work as template functions to generate SQL queries. You can also compose models to build more complex queries. For example, you can take a join between data modle and other tables:
```sql
from lookup(1) as p
join address_table 
  on p.id = address_table.person_id
```

### Managing Queries As A Reusable Module

:::warning
This feature will be available in later 2024.
:::

These queries saved as `.wv` files can be managed in local folders or GitHub repositories as modules. You can import these queries in other queries to reuse them. If analyzing your datasets requires the knowledge of domain experts or complex data processing, you can leverage such query modules to focus on the core part of your analysis.

```sql
-- import queries from a GitHub repository
import github.com/my_project/my_query_library

-- Call a query from the imported module
from my_query(p1, p2, ...)
```

## Extensible Types

You can define your own functions to the existing data types to specify how to compile these functions int SQL. For example, you can extend the string type to handle null values:

```sql
type string:
  def or_else(default:string): string = sql"coalesce({this}, '{default}')"
end

-- Use or_else function to handle null values:
from person
select id, name.or_else("N/A") as name
```

:::warning
Extending wvlet with table functions will be available in later 2024.
:::

You can also define table functions to pipe query results to the next processing steps:
```sql
from persons
-- Use your_table_function to process the input table results
pipe your_table_function(...)
limit 10
```

## Chaining Function Calls

In the modern programming languages, you can call functions defined in the type using dot-notation, like `lst.sum().round(0)`. Unfortunately however, SQL, which was designed about 50 years ago, relies on global function calls everywhere. To mitigate that, wvlet supports dot-notation based function calls and its chaining:

```sql
wv> from lineitem
  | group by l_shipmode
  | select _1, _.count, l_quantity.sum.round(0);
┌────────────┬──────────────┬───────────────────────────┐
│ l_shipmode │ count_star() │ round(sum(l_quantity), 0) │
│   string   │     long     │       decimal(38,0)       │
├────────────┼──────────────┼───────────────────────────┤
│ FOB        │         8641 │                    219565 │
│ SHIP       │         8482 │                    217969 │
│ TRUCK      │         8710 │                    223909 │
│ AIR        │         8491 │                    216331 │
│ REG AIR    │         8616 │                    219015 │
│ MAIL       │         8669 │                    221528 │
│ RAIL       │         8566 │                    217810 │
├────────────┴──────────────┴───────────────────────────┤
│ 7 rows                                                │
└───────────────────────────────────────────────────────┘
```

## Incremental Processing

:::warning
Incremental processing feature is planned to be available in later 2024.
:::

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

If your database supports modern table formats (e.g., Delta Lake, Iceberg, etc.), which support snapshot reads and time-travelling, the resulting SQL will be optimized to use these features to reduce the data processing time.

