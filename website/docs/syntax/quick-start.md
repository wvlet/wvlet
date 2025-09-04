---
sidebar_position: 1
---

# Quick Start

## A Walk-Through Example

Let's start with a simple example. If you haven't installed `wv` command, [install Wvlet command](../usage/install.md) to your machine. `wv` command starts an [interactive shell](../usage/repl.md), which is backed by DuckDB in-memory database by default.

For the ease of learning, let's create a sample [TPC-H benchmark](https://www.tpc.org/tpch/) data set:

```wvlet
$ wv
wv> execute sql"call dbgen(sf=0.01)";
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
```
The `execute sql"call dbgen(sf=0.01)"` command calls DuckDB's [TPC-H extension](https://duckdb.org/docs/extensions/tpch.html) and creates an in-memory TPC-H benchmark database, which will be gone when you exit the Wvlet shell. So you can try this command without worrying about the disk space.

The simplest form of queries is `from (table name)`:
```wvlet
wv> from customer;
┌───────────┬────────────────────┬─────────────────────────────────────────┬───────────>
│ c_custkey │       c_name       │                c_address                │ c_nationke>
│   long    │       string       │                 string                  │     int   >
├───────────┼────────────────────┼─────────────────────────────────────────┼───────────>
│         1 │ Customer#000000001 │ j5JsirBM9PsCy0O1m                       │          1>
│         2 │ Customer#000000002 │ 487LW1dovn6Q4dMVymKwwLE9OKf3QG          │          1>
│         3 │ Customer#000000003 │ fkRGN8nY4pkE                            │           >
│         4 │ Customer#000000004 │ 4u58h fqkyE                             │           >
│         5 │ Customer#000000005 │ hwBtxkoBF qSW4KrIk5U 2B1AU7H            │           >
│         6 │ Customer#000000006 │  g1s,pzDenUEBW3O,2 pxu0f9n2g64rJrt5E    │          2>
│         7 │ Customer#000000007 │ 8OkMVLQ1dK6Mbu6WG9 w4pLGQ n7MQ          │          1>
│         8 │ Customer#000000008 │ j,pZ,Qp,qtFEo0r0c 92qobZtlhSuOqbE4JGV   │          1>
│         9 │ Customer#000000009 │ vgIql8H6zoyuLMFNdAMLyE7 H9              │           >
```

This query returns all the columns in the `customer` table.
If the query result doesn't fit to the screen, Wvlet shell enters [UNIX `less` command](https://en.wikipedia.org/wiki/Less_(Unix)#:~:text=less%20is%20a%20terminal%20pager,backward%20navigation%20through%20the%20file.) mode, which
allows to navigate table data using arrow keys, page up/down keys, and `q` key to exit the mode. See [Interactive Shell](../usage/repl.md) for the list of the available shortcut keys.

To limit the number of rows to display, you can use `limit` operator:
```wvlet
wv> from customer
  │ limit 3;
┌───────────┬────────────────────┬────────────────────────────────┬─────────────┬──────>
│ c_custkey │       c_name       │           c_address            │ c_nationkey │     c>
│   long    │       string       │             string             │     int     │     s>
├───────────┼────────────────────┼────────────────────────────────┼─────────────┼──────>
│         1 │ Customer#000000001 │ j5JsirBM9PsCy0O1m              │          15 │ 25-98>
│         2 │ Customer#000000002 │ 487LW1dovn6Q4dMVymKwwLE9OKf3QG │          13 │ 23-76>
│         3 │ Customer#000000003 │ fkRGN8nY4pkE                   │           1 │ 11-71>
├───────────┴────────────────────┴────────────────────────────────┴─────────────┴──────>
│ 3 rows                                                                               >
└──────────────────────────────────────────────────────────────────────────────────────>
```

:::tip
Separators `|` between expressions are shown only while editing queries. You don't need to type `|` in the Wvlet shell or in query files.
:::


To select specific columns, you can use `select` operator:

```wvlet
from customer
wv> from customer
  │ select c_name, c_nationkey
  │ limit 5;
┌────────────────────┬─────────────┐
│       c_name       │ c_nationkey │
│       string       │     int     │
├────────────────────┼─────────────┤
│ Customer#000000001 │          15 │
│ Customer#000000002 │          13 │
│ Customer#000000003 │           1 │
│ Customer#000000004 │           4 │
│ Customer#000000005 │           3 │
├────────────────────┴─────────────┤
│ 5 rows                           │
└──────────────────────────────────┘
```

To select specific values from the table, you can use `where` operator:

```wvlet 
wv> from customer 
  │ where c_nationkey = 1
  │ select c_name, c_address
  │ limit 5;
┌────────────────────┬─────────────┐
│       c_name       │ c_nationkey │
│       string       │     int     │
├────────────────────┼─────────────┤
│ Customer#000000003 │           1 │
│ Customer#000000014 │           1 │
│ Customer#000000030 │           1 │
│ Customer#000000059 │           1 │
│ Customer#000000106 │           1 │
├────────────────────┴─────────────┤
│ 5 rows                           │
└──────────────────────────────────┘
```

## Writing Queries

### One-Liner Queries

In Wvlet, individual query line often matches with a single [relational operator](./), which processes a given input table data and return a new table data. Inserting newlines, however, is not mandatory. You can fit the whole query within a single line, which is convenient for quick data exploration:

```wvlet
wv> from customer where c_mktsegment = 'HOUSEHOLD' limit 5;
┌───────────┬────────────────────┬────────────────────────────────────────┬────────────>
│ c_custkey │       c_name       │               c_address                │ c_nationkey>
│   long    │       string       │                 string                 │     int    >
├───────────┼────────────────────┼────────────────────────────────────────┼────────────>
│         5 │ Customer#000000005 │ hwBtxkoBF qSW4KrIk5U 2B1AU7H           │           3>
│        10 │ Customer#000000010 │ Vf mQ6Ug9Ucf5OKGYq fsaX AtfsO7,rwY     │           5>
│        12 │ Customer#000000012 │ Sb4gxKs7W1AZa                          │          13>
│        15 │ Customer#000000015 │ 3y4KK4CcfNwNCTP0u0p1Rk6aeghe3Z30mo0VnD │          23>
│        19 │ Customer#000000019 │ yO0XPkiuSWk0vN FfcH5 IA3oBYy           │          18>
├───────────┴────────────────────┴────────────────────────────────────────┴────────────>
│ 5 rows                                                                               >
└──────────────────────────────────────────────────────────────────────────────────────>
```

### Adding Comments

The multi-line syntax is convenient for improving the readability of your queries. As Wvlet adopts a flow-style syntax, you can add comments to each line of the query:

```wvlet
wv> from customer
  │ -- Select customers for each market segment, e.g., HOUSEHOLD, BUILDING, etc.
  │ group by c_mktsegment,
  │ -- Report the number of customers in each market segment
  │ agg _.count as customer_count
┌──────────────┬────────────────┐
│ c_mktsegment │ customer_count │
│    string    │      long      │
├──────────────┼────────────────┤
│ HOUSEHOLD    │            588 │
│ BUILDING     │            674 │
│ MACHINERY    │            576 │
│ FURNITURE    │            558 │
│ AUTOMOBILE   │            604 │
├──────────────┴────────────────┤
│ 5 rows                        │
└───────────────────────────────┘
```

Comments in Wvlet start with `--` and continue to the end of the line.

## Exploring Data

### Describing Table Schema

To learn about the table schema, the list of columns and types in the table, you can use `describe` operator:
```wvlet
wv> from customer
  │ describe;
┌──────────────┬─────────────┐
│ column_name  │ column_type │
│    string    │   string    │
├──────────────┼─────────────┤
│ c_custkey    │ long        │
│ c_name       │ string      │
│ c_address    │ string      │
│ c_nationkey  │ int         │
│ c_phone      │ string      │
│ c_acctbal    │ decimal     │
│ c_mktsegment │ string      │
│ c_comment    │ string      │
├──────────────┴─────────────┤
│ 8 rows                     │
└────────────────────────────┘
```

`describe` is also a relational operator, which can be filtered by `where` operator:

```wvlet
wv> from customer
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
describe (line:1): from customer
┌──────────────┬─────────────┐
│ column_name  │ column_type │
│    string    │   string    │
├──────────────┼─────────────┤
│ c_custkey    │ long        │
│ c_name       │ string      │
│ c_address    │ string      │
│ c_nationkey  │ int         │
│ c_phone      │ string      │
│ c_acctbal    │ decimal     │
│ c_mktsegment │ string      │
│ c_comment    │ string      │
├──────────────┴─────────────┤
│ 8 rows                     │
└────────────────────────────┘
wv> from customer  -- Press ctrl-j ctrl-d sequence here
  │ where c_nationkey = 1
  │ select c_name, c_nationkey
  │ limit 5;
```
`ctrl-j ctrl-d` shortcut key internally calls `(A query fragment up to the current line) describe`  to show the schema of the current query fragment.

You can also check the schema in the middle of a query:
```wvlet
describe (line:3): select c_name, c_nationkey
┌─────────────┬─────────────┐
│ column_name │ column_type │
│   string    │   string    │
├─────────────┼─────────────┤
│ c_name      │ string      │
│ c_nationkey │ int         │
├─────────────┴─────────────┤
│ 2 rows                    │
└───────────────────────────┘
wv> from customer
  │ where c_nationkey = 1
  │ select c_name, c_nationkey -- Press ctrl-j ctrl-d here
  │ limit 5;
```

### Test Run to Peek Data

While editing queries, you will often need to look at the actual data. Type `ctrl-j` `ctrl-t` (test run) to see the intermediate query results at the line:

```wvlet
wv> from customer -- type ctrl-j ctrl-t here
  │ where c_nationkey = 1
  │ select c_name, c_nationkey
  │ limit 5;
debug (line:1): from customer
┌───────────┬────────────────────┬─────────────────────────────────────────┬───────────>
│ c_custkey │       c_name       │                c_address                │ c_nationke>
│   long    │       string       │                 string                  │     int   >
├───────────┼────────────────────┼─────────────────────────────────────────┼───────────>
│         1 │ Customer#000000001 │ j5JsirBM9PsCy0O1m                       │          1>
│         2 │ Customer#000000002 │ 487LW1dovn6Q4dMVymKwwLE9OKf3QG          │          1>
│         3 │ Customer#000000003 │ fkRGN8nY4pkE                            │           >
│         4 │ Customer#000000004 │ 4u58h fqkyE                             │           >
│         5 │ Customer#000000005 │ hwBtxkoBF qSW4KrIk5U 2B1AU7H            │           >
│         6 │ Customer#000000006 │  g1s,pzDenUEBW3O,2 pxu0f9n2g64rJrt5E    │          2>
│         7 │ Customer#000000007 │ 8OkMVLQ1dK6Mbu6WG9 w4pLGQ n7MQ          │          1>
│         8 │ Customer#000000008 │ j,pZ,Qp,qtFEo0r0c 92qobZtlhSuOqbE4JGV   │          1>
│         9 │ Customer#000000009 │ vgIql8H6zoyuLMFNdAMLyE7 H9              │           >
│        10 │ Customer#000000010 │ Vf mQ6Ug9Ucf5OKGYq fsaX AtfsO7,rwY      │           >
```

Test run command is useful to refine your query as you add more relational operators:
```wvlet
wv> from customer
  │ select c_custkey, c_name, c_nationkey -- type ctrl-j ctrl-t here
debug (line:2): select c_custkey, c_name, c_nationkey
┌───────────┬────────────────────┬─────────────┐
│ c_custkey │       c_name       │ c_nationkey │
│   long    │       string       │     int     │
├───────────┼────────────────────┼─────────────┤
│         1 │ Customer#000000001 │          15 │
│         2 │ Customer#000000002 │          13 │
│         3 │ Customer#000000003 │           1 │
│         4 │ Customer#000000004 │           4 │
│         5 │ Customer#000000005 │           3 │
│         6 │ Customer#000000006 │          20 │
```

## Reusing Queries

In Wvlet, you can name a query using `select as` operator, and refer to the named query result in the subsequent queries:

```wvlet
wv> from customer
  │ where c_nationkey = 1
  │ -- Name the query as domestic_customer
  │ select as domestic_customer;
```

You can refer to the named query result in the subsequent queries:
```wvlet
wv> from domestic_customer
  │ limit 5;
┌───────────┬────────────────────┬─────────────────────────────────────────┬─────────────┬─>
│ c_custkey │       c_name       │                c_address                │ c_nationkey │ >
│   long    │       string       │                 string                  │     int     │ >
├───────────┼────────────────────┼─────────────────────────────────────────┼─────────────┼─>
│         3 │ Customer#000000003 │ fkRGN8nY4pkE                            │           1 │ >
│        14 │ Customer#000000014 │ h3GFMzeFfYiamqr                         │           1 │ >
│        30 │ Customer#000000030 │ EhnzmgkqQw7UXhF0PVdg gLfSAihaaHaD2fZah2 │           1 │ >
│        59 │ Customer#000000059 │ tfcob0wJRYdypIJLzBckGW                  │           1 │ >
│       106 │ Customer#000000106 │ vkocmr6H6dl                             │           1 │ >
├───────────┴────────────────────┴─────────────────────────────────────────┴─────────────┴─>
│ 5 rows                                                                                   >
└──────────────────────────────────────────────────────────────────────────────────────────>
```

Unlike SQL views, which will be registered to the system catalog, named queries are available only in the current scope (e.g., the current Wvlet shell session).

## Saving Queries in .wv Files

If you want to reuse the query in other sessions or share it with others, you can save the query to a file with `.wv` extension:

```wvlet title="my_query.wv"
from customer
where c_nationkey = 1
```

Queries in `.wv` files can be loaded in `from` operator:

```wvlet
-- Load the query written in my_query.wv file
from 'my_query.wv'
```

In the wvlet shell, .wv files will be loaded from the current directory. If you want to load files from other directories, use `-w (working directory)` option to specify the base directory.


:::tip
For advanced users, you can define reusable data models, which can accept some input parameters. Wvlet will find data models defined in `.wv` files in the working directory. See [Data Models](./data-models.md) for more details.
:::

