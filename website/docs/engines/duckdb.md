---
description: "Run Wvlet queries on DuckDB with -t duckdb or a duckdb profile: query Parquet files directly by path or URL and generate TPC-H sample data."
---

# DuckDB

`wvlet run` and `wv` run queries on the in-process [wvlet engine](wvlet-engine.md) by default.
Select DuckDB's in-memory database (catalog: memory, schema: main) with `-t duckdb`
(`wv -t duckdb`) or a profile with `"type": "duckdb"`. DuckDB is needed for `sql"..."` and
`execute` statements, `.duckdb` database files, DuckDB extensions, and table functions such as
`read_csv(...)`, and for `view` models of `wvlet build`. `wvlet compile` generates DuckDB SQL by
default; `wvlet build` and `wvlet flow` run on the wvlet engine unless `-t duckdb` (or a DuckDB
profile) is given.

On the JVM CLI, DuckDB is in the full package `wvlet-cli-full-(version).tar.gz`; see [Installation](../usage/install.md#jvm-cli). The npm CLI includes DuckDB.


## Usage Examples

```bash
$ wv -t duckdb
wv> from 'https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'
  | group by c_mktsegment
  | agg _.count
  | limit 5;
┌──────────────┬──────────────┐
│ c_mktsegment │ count_star() │
│    string    │     long     │
├──────────────┼──────────────┤
│ MACHINERY    │          288 │
│ HOUSEHOLD    │          294 │
│ AUTOMOBILE   │          302 │
│ FURNITURE    │          279 │
│ BUILDING     │          337 │
├──────────────┴──────────────┤
│ 5 rows                      │
└─────────────────────────────┘
```


To call some DuckDB internal functions, use raw SQL blocks: 

```bash
# Generate TPC-H dataset with scale factor 0.01
wv> from sql"call dbgen(sf=0.01)";
┌─────────┐
│ Success │
│ boolean │
├─────────┤
├─────────┤
│ 0 rows  │
└─────────┘
wv> show tables;
┌────────────┐
│ table_name │
│   string   │
├────────────┤
│ customer   │
│ lineitem   │
│ nation     │
│ orders     │
│ part       │
│ partsupp   │
│ region     │
│ supplier   │
├────────────┤
│ 8 rows     │
└────────────┘
wv> from orders limit 5;
┌────────────┬───────────┬───────────────┬───────────────┬─────────────┬──────────>
│ o_orderkey │ o_custkey │ o_orderstatus │ o_totalprice  │ o_orderdate │ o_orderpr>
│    long    │   long    │    string     │ decimal(15,2) │    date     │     strin>
├────────────┼───────────┼───────────────┼───────────────┼─────────────┼──────────>
│          1 │       370 │ O             │     172799.49 │ 1996-01-02  │ 5-LOW    >
│          2 │       781 │ O             │      38426.09 │ 1996-12-01  │ 1-URGENT >
│          3 │      1234 │ F             │     205654.30 │ 1993-10-14  │ 5-LOW    >
│          4 │      1369 │ O             │      56000.91 │ 1995-10-11  │ 5-LOW    >
│          5 │       445 │ F             │     105367.67 │ 1994-07-30  │ 5-LOW    >
├────────────┴───────────┴───────────────┴───────────────┴─────────────┴──────────>
│ 5 rows                                                                          >
└─────────────────────────────────────────────────────────────────────────────────>
```
