---
description: "A hands-on Wvlet tutorial: start the wv shell, query the built-in TPC-H tables on the Wvlet engine with no setup, and build flow-style queries step by step."
sidebar_position: 1
---

# Quick Start

## A Walk-Through Example

Let's start with a simple example. If you haven't installed the `wv` command, [install Wvlet](../usage/install.md) first. `wv` starts an [interactive shell](../usage/repl.md) that runs queries on the in-process [Wvlet engine](../usage/wvlet-engine.md), so there is no database to set up.

Every Wvlet engine session can read a generated [TPC-H benchmark](https://www.tpc.org/tpch/) data set as `tpch.<scale>.<table>`. This tutorial uses the smallest one, `tpch.tiny` (scale factor 0.01), whose eight tables are `customer`, `lineitem`, `nation`, `orders`, `part`, `partsupp`, `region`, and `supplier`. Rows are generated in memory while a query reads them, so nothing is written to disk. See [Built-in tpch and tpcds catalogs](../usage/wvlet-engine.md#built-in-tpch-and-tpcds-catalogs) for larger scale factors and the TPC-DS tables.

:::tip
`wv` comes with the JVM CLI (Homebrew or a release package). With the npm package, run each query with `wvlet run` instead, for example `wvlet run "from tpch.tiny.customer limit 3"`.
:::

The simplest form of queries is `from (table name)`:

```wvlet
$ wv
wv> from tpch.tiny.customer;
┌───────────┬────────────────────┬──────────────────────────────────────────┬────────────>
│ c_custkey │       c_name       │                c_address                 │ c_nationkey>
│   long    │       string       │                  string                  │    long    >
├───────────┼────────────────────┼──────────────────────────────────────────┼────────────>
│         1 │ Customer#000000001 │ 6WssTynb387PEmdkrsq3ZfVYAgL2YbZ0         │           6>
│         2 │ Customer#000000002 │ 0YJH3zUJjjA7Wpe0hb7Z2D                   │          21>
│         3 │ Customer#000000003 │ JNGJkuS483CGh7QelzEevplGMT0I77u          │           0>
│         4 │ Customer#000000004 │ U5e440uOu8                               │           4>
│         5 │ Customer#000000005 │ mUNNyEWYOfBICOv6u9E3xAZxl2CBHc2Vi        │          11>
│         6 │ Customer#000000006 │ nWt8Mrij5u                               │           7>
│         7 │ Customer#000000007 │ okyeONh,52YQM1W60wSpFRHbT                │          19>
│         8 │ Customer#000000008 │ lH11rDSC6kcUVNJdbrTgJmsndkrLvb1Mt        │          12>
│         9 │ Customer#000000009 │ P0dmfCY5IrWFQwBP9,CPFCLYNApbksZ          │          18>
```

This query returns all the columns in the `customer` table. If the query result doesn't fit to the screen, the Wvlet shell enters [UNIX `less` command](https://en.wikipedia.org/wiki/Less_(Unix)) mode, which allows you to navigate table data using arrow keys, page up/down keys, and the `q` key to exit the mode. See [Interactive Shell](../usage/repl.md) for the list of the available shortcut keys.

:::tip
The values in the `tpch` tables follow the TPC-H specification but aren't those of the reference `dbgen` generator, so your numbers won't match the official TPC-H answers. To use DuckDB's `dbgen` data instead (this needs the full JVM package, which includes DuckDB), start the shell with `wv -t duckdb` and run `execute sql"call dbgen(sf=0.01)"`; the tables are then named without the `tpch.tiny.` prefix.
:::

To limit the number of rows to display, you can use the `limit` operator:

```wvlet
wv> from tpch.tiny.customer
  │ limit 3;
┌───────────┬────────────────────┬──────────────────────────────────┬─────────────┬──────>
│ c_custkey │       c_name       │            c_address             │ c_nationkey │     c>
│   long    │       string       │              string              │    long     │     s>
├───────────┼────────────────────┼──────────────────────────────────┼─────────────┼──────>
│         1 │ Customer#000000001 │ 6WssTynb387PEmdkrsq3ZfVYAgL2YbZ0 │           6 │ 16-54>
│         2 │ Customer#000000002 │ 0YJH3zUJjjA7Wpe0hb7Z2D           │          21 │ 31-56>
│         3 │ Customer#000000003 │ JNGJkuS483CGh7QelzEevplGMT0I77u  │           0 │ 10-19>
├───────────┴────────────────────┴──────────────────────────────────┴─────────────┴──────>
│ 3 rows                                                                                 >
└────────────────────────────────────────────────────────────────────────────────────────>
```

:::tip
Separators `│` between lines are shown only while editing queries. You don't need to type `│` in the Wvlet shell or in query files.
:::

To select specific columns, you can use the `select` operator:

```wvlet
wv> from tpch.tiny.customer
  │ select c_name, c_nationkey
  │ limit 5;
┌────────────────────┬─────────────┐
│       c_name       │ c_nationkey │
│       string       │    long     │
├────────────────────┼─────────────┤
│ Customer#000000001 │           6 │
│ Customer#000000002 │          21 │
│ Customer#000000003 │           0 │
│ Customer#000000004 │           4 │
│ Customer#000000005 │          11 │
├────────────────────┴─────────────┤
│ 5 rows                           │
└──────────────────────────────────┘
```

To select specific rows from the table, you can use the `where` operator:

```wvlet
wv> from tpch.tiny.customer
  │ where c_nationkey = 1
  │ select c_name, c_address
  │ limit 5;
┌────────────────────┬──────────────────────────────────────────┐
│       c_name       │                c_address                 │
│       string       │                  string                  │
├────────────────────┼──────────────────────────────────────────┤
│ Customer#000000060 │ x1TuMVRHKcnIwGZWWi                       │
│ Customer#000000098 │ 4u7vh3lRgQnOLq0fpjkM5r1nMOhC,SUk5su      │
│ Customer#000000117 │ ubT6riSam5zisD2tL                        │
│ Customer#000000130 │ 4sXPTI6oPZ                               │
│ Customer#000000153 │ jxt43opIMcgNXk251dY5enRmvJNlwn9UscB2J2KZ │
├────────────────────┴──────────────────────────────────────────┤
│ 5 rows                                                        │
└───────────────────────────────────────────────────────────────┘
```

## Writing Queries

### One-Liner Queries

In Wvlet, each query line often matches a single [relational operator](./), which processes a given input table and returns a new table. Newlines, however, are not mandatory. You can fit the whole query within a single line, which is convenient for quick data exploration:

```wvlet
wv> from tpch.tiny.customer where c_mktsegment = 'HOUSEHOLD' limit 5;
┌───────────┬────────────────────┬───────────────────────────────────────┬─────────────┬─>
│ c_custkey │       c_name       │               c_address               │ c_nationkey │ >
│   long    │       string       │                string                 │    long     │ >
├───────────┼────────────────────┼───────────────────────────────────────┼─────────────┼─>
│         2 │ Customer#000000002 │ 0YJH3zUJjjA7Wpe0hb7Z2D                │          21 │ >
│         3 │ Customer#000000003 │ JNGJkuS483CGh7QelzEevplGMT0I77u       │           0 │ >
│         6 │ Customer#000000006 │ nWt8Mrij5u                            │           7 │ >
│        10 │ Customer#000000010 │ kd0gojXB41qhfehP2cmPN0y5VL1iwLIe4     │           7 │ >
│        28 │ Customer#000000028 │ JP1U6jgwEnXiMzaT7BKSF97PaQVfymaQOwMN5 │           8 │ >
├───────────┴────────────────────┴───────────────────────────────────────┴─────────────┴─>
│ 5 rows                                                                                 >
└────────────────────────────────────────────────────────────────────────────────────────>
```

### Adding Comments

The multi-line syntax is convenient for improving the readability of your queries. As Wvlet adopts a flow-style syntax, you can add comments to each line of the query:

```wvlet
wv> from tpch.tiny.customer
  │ -- Select customers for each market segment, e.g., HOUSEHOLD, BUILDING, etc.
  │ group by c_mktsegment
  │ -- Report the number of customers in each market segment
  │ agg _.count as customer_count;
┌──────────────┬────────────────┐
│ c_mktsegment │ customer_count │
│    string    │      long      │
├──────────────┼────────────────┤
│ BUILDING     │            324 │
│ HOUSEHOLD    │            285 │
│ FURNITURE    │            307 │
│ MACHINERY    │            275 │
│ AUTOMOBILE   │            309 │
├──────────────┴────────────────┤
│ 5 rows                        │
└───────────────────────────────┘
```

Comments in Wvlet start with `--` and continue to the end of the line.

## Exploring Data

### Describing Table Schema

To learn about the table schema, the list of columns and types in the table, you can use the `describe` operator:

```wvlet
wv> from tpch.tiny.customer
  │ describe;
┌──────────────┬───────────────┐
│ column_name  │  column_type  │
│    string    │    string     │
├──────────────┼───────────────┤
│ c_custkey    │ long          │
│ c_name       │ string        │
│ c_address    │ string        │
│ c_nationkey  │ long          │
│ c_phone      │ string        │
│ c_acctbal    │ decimal(15,2) │
│ c_mktsegment │ string        │
│ c_comment    │ string        │
├──────────────┴───────────────┤
│ 8 rows                       │
└──────────────────────────────┘
```

`describe` is also a relational operator, which can be filtered by the `where` operator:

```wvlet
wv> from tpch.tiny.customer
  │ describe
  │ where column_name like '%name%';
┌─────────────┬─────────────┐
│ column_name │ column_type │
│   string    │   string    │
├─────────────┼─────────────┤
│ c_name      │ string      │
├─────────────┴─────────────┤
│ 1 rows                    │
└───────────────────────────┘
```

### Quick Schema Check

A more convenient way to see the table schema is to use `ctrl-j ctrl-d` shortcut keys in the Wvlet shell:

```wvlet
describe (line:1): from tpch.tiny.customer
┌──────────────┬───────────────┐
│ column_name  │  column_type  │
│    string    │    string     │
├──────────────┼───────────────┤
│ c_custkey    │ long          │
│ c_name       │ string        │
│ c_address    │ string        │
│ c_nationkey  │ long          │
│ c_phone      │ string        │
│ c_acctbal    │ decimal(15,2) │
│ c_mktsegment │ string        │
│ c_comment    │ string        │
├──────────────┴───────────────┤
│ 8 rows                       │
└──────────────────────────────┘
wv> from tpch.tiny.customer  -- Press ctrl-j ctrl-d sequence here
  │ where c_nationkey = 1
  │ select c_name, c_nationkey
  │ limit 5;
```

`ctrl-j ctrl-d` internally runs `(the query fragment up to the current line) describe` to show the schema of the current query fragment.

You can also check the schema in the middle of a query:

```wvlet
describe (line:3): select c_name, c_nationkey
┌─────────────┬─────────────┐
│ column_name │ column_type │
│   string    │   string    │
├─────────────┼─────────────┤
│ c_name      │ string      │
│ c_nationkey │ long        │
├─────────────┴─────────────┤
│ 2 rows                    │
└───────────────────────────┘
wv> from tpch.tiny.customer
  │ where c_nationkey = 1
  │ select c_name, c_nationkey -- Press ctrl-j ctrl-d here
  │ limit 5;
```

### Test Run to Peek Data

While editing queries, you will often need to look at the actual data. Type `ctrl-j` `ctrl-t` (test run) to see the intermediate query results at the line:

```wvlet
wv> from tpch.tiny.customer -- type ctrl-j ctrl-t here
  │ where c_nationkey = 1
  │ select c_name, c_nationkey
  │ limit 5;
debug (line:1): from tpch.tiny.customer
┌───────────┬────────────────────┬──────────────────────────────────────────┬────────────>
│ c_custkey │       c_name       │                c_address                 │ c_nationkey>
│   long    │       string       │                  string                  │    long    >
├───────────┼────────────────────┼──────────────────────────────────────────┼────────────>
│         1 │ Customer#000000001 │ 6WssTynb387PEmdkrsq3ZfVYAgL2YbZ0         │           6>
│         2 │ Customer#000000002 │ 0YJH3zUJjjA7Wpe0hb7Z2D                   │          21>
│         3 │ Customer#000000003 │ JNGJkuS483CGh7QelzEevplGMT0I77u          │           0>
│         4 │ Customer#000000004 │ U5e440uOu8                               │           4>
│         5 │ Customer#000000005 │ mUNNyEWYOfBICOv6u9E3xAZxl2CBHc2Vi        │          11>
│         6 │ Customer#000000006 │ nWt8Mrij5u                               │           7>
│         7 │ Customer#000000007 │ okyeONh,52YQM1W60wSpFRHbT                │          19>
│         8 │ Customer#000000008 │ lH11rDSC6kcUVNJdbrTgJmsndkrLvb1Mt        │          12>
│         9 │ Customer#000000009 │ P0dmfCY5IrWFQwBP9,CPFCLYNApbksZ          │          18>
```

Test run is useful to refine your query as you add more relational operators:

```wvlet
wv> from tpch.tiny.customer
  │ select c_custkey, c_name, c_nationkey -- type ctrl-j ctrl-t here
debug (line:2): select c_custkey, c_name, c_nationkey
┌───────────┬────────────────────┬─────────────┐
│ c_custkey │       c_name       │ c_nationkey │
│   long    │       string       │    long     │
├───────────┼────────────────────┼─────────────┤
│         1 │ Customer#000000001 │           6 │
│         2 │ Customer#000000002 │          21 │
│         3 │ Customer#000000003 │           0 │
│         4 │ Customer#000000004 │           4 │
│         5 │ Customer#000000005 │          11 │
```

## Reusing Queries

In Wvlet, you can name a query using the `select as` operator, and refer to the named query result in the subsequent queries:

```wvlet
wv> from tpch.tiny.customer
  │ where c_nationkey = 1
  │ -- Name the query as domestic_customer
  │ select as domestic_customer;
```

You can refer to the named query result in the subsequent queries:

```wvlet
wv> from domestic_customer
  │ limit 5;
┌───────────┬────────────────────┬──────────────────────────────────────────┬────────────>
│ c_custkey │       c_name       │                c_address                 │ c_nationkey>
│   long    │       string       │                  string                  │    long    >
├───────────┼────────────────────┼──────────────────────────────────────────┼────────────>
│        60 │ Customer#000000060 │ x1TuMVRHKcnIwGZWWi                       │           1>
│        98 │ Customer#000000098 │ 4u7vh3lRgQnOLq0fpjkM5r1nMOhC,SUk5su      │           1>
│       117 │ Customer#000000117 │ ubT6riSam5zisD2tL                        │           1>
│       130 │ Customer#000000130 │ 4sXPTI6oPZ                               │           1>
│       153 │ Customer#000000153 │ jxt43opIMcgNXk251dY5enRmvJNlwn9UscB2J2KZ │           1>
├───────────┴────────────────────┴──────────────────────────────────────────┴────────────>
│ 5 rows                                                                                 >
└────────────────────────────────────────────────────────────────────────────────────────>
```

Unlike SQL views, which are registered to the system catalog, named queries are available only in the current scope (e.g., the current Wvlet shell session).

## Saving Queries in .wv Files

If you want to reuse the query in other sessions or share it with others, you can save the query to a file with the `.wv` extension:

```wvlet title="my_query.wv"
from tpch.tiny.customer
where c_nationkey = 1
```

Queries in `.wv` files can be loaded in the `from` operator:

```wvlet
-- Load the query written in my_query.wv file
from 'my_query.wv'
```

In the Wvlet shell, .wv files are loaded from the current directory. If you want to load files from other directories, use the `-w (working directory)` option to specify the base directory. To run a query file from the command line, use `wvlet run --file my_query.wv`.

:::tip
You can also define reusable data models and functions that accept parameters. Wvlet finds data models defined in `.wv` files in the working directory. See [Data Models](./data-models.md) for more details.
:::

## Next Steps

- [Query Syntax](./index.md) lists all relational operators.
- [Wvlet Engine](../usage/wvlet-engine.md) shows how to query your own Parquet, CSV, JSON, and Iceberg files.
- [DuckDB](../usage/duckdb.md), [Trino](../usage/trino.md), and [Snowflake](../usage/snowflake.md) show how to run the same queries on a SQL engine.
