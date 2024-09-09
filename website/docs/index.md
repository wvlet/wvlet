---
sidebar_label: Introduction
sidebar_position: 1
---

<img width='130' src='../img/logo.png' style={{float: "right", padding: 10, margin: 15}}>
</img>

# Introduction

The wvlet, pronounced as weave-let, is a new flow-style query language for SQL-based database engines, including [DuckDB](https://duckdb.org/), [Trino](https://trino.io/), etc.


## Documentation

- [Query Syntax](./syntax/)
- [Installation](./usage/install)


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
This flow style enables describing data processing pipelines with top-to-bottom semantic ordering. It allows various methods to help composing complex queries, such as adding an operator for debuging data in the middle of a query. This type of flow syntax has also been adopted in Google SQL, _[SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL (VLDB 2024)](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/)_, to cope with the complexity of writing SQL queries.

## Architecture

![wvlet-architecture](./img/wvlet-architecture.svg)
