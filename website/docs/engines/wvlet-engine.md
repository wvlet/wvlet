---
sidebar_label: Overview
description: "Wvlet's own in-process columnar engine runs Wvlet plans directly on Parquet, Iceberg, JSON, and CSV data, on the JVM, the native CLI, Node.js, and in the browser, with no database or DuckDB required."
---

# Wvlet Engine

The Wvlet engine is an in-process columnar query engine that executes compiled Wvlet plans directly, with no SQL generation and no external database. It is the default engine of `wvlet run`, the REPL, `wvlet build`, and `wvlet flow run`.

- **Complete on the standard benchmarks**: it runs all 22 TPC-H and 99 TPC-DS queries.
- **On par with DuckDB at scale factor 10**: vectorized, multi-threaded operators planned by a cost-based optimizer run TPC-DS in 0.90x of DuckDB's time and TPC-H in 1.04x (geometric means). These are steady-state numbers, with the engine reusing per-file statistics and declared TPC keys; see [Performance](wvlet-engine-performance.md) for the setup.
- **One engine everywhere**: the same Scala codebase runs on the JVM, as a native binary, in Node.js, and in the browser.

## Where it runs

The engine reads Parquet and Iceberg with its own Scala readers, so no platform requires a database or a native library. On the JVM it decodes Snappy-compressed Parquet pages with [snappy-java](https://github.com/xerial/snappy-java) when a native build fits the platform, and uses the pure-Scala decoder otherwise (`-Dwvlet.uni.snappy.useSnappyJava=false` forces it); the other platforms use the pure-Scala decoder.

- **JVM**: the `wvlet` command (and its `wv` alias) from Homebrew or a release package ([Installation](../usage/install.md)).
- **Node.js**: `wvlet run` from the npm package `@wvlet/cli`.
- **Native binary**: the Scala Native build of the CLI.
- **Browser**: the [Playground](../usage/playground.md) runs the engine compiled to JavaScript.
- **GPU (experimental)**: an example module offloads joins and aggregations through cudf; see [GPU Executor](wvlet-engine-gpu.md).

## What it supports

- Queries over files and registered tables: filters, projections, joins (including ASOF joins), aggregations, window functions, subqueries, `pivot`, `unnest`, lambdas, and `show` commands.
- All 22 TPC-H and 99 TPC-DS queries, and built-in [TPC-H and TPC-DS catalogs](wvlet-engine-tables.md#built-in-tpch-and-tpcds-catalogs) to try them.
- Reading Parquet, Iceberg, JSON, and CSV data, including Hive-partitioned Parquet folders.
- `save to`, `append to`, and `delete` on tables, and `save to` / `append to` Parquet, CSV, JSON files and Iceberg tables.
- `create table`, `drop table`, `truncate`, `create` / `drop schema`, `reshape` (or SQL `alter table`), and `rename table`.
- Persistent tables as Iceberg tables in a warehouse folder; see [Tables and Files](wvlet-engine-tables.md).

Not supported yet, and reported as errors: views, `update`, `execute`, table functions such as `read_csv(...)`, and some engine-specific functions. Map values run, but some map results can differ from DuckDB's. For these, use `-t duckdb` or a DuckDB profile.

## Usage

The engine is the default of `wvlet run` and the REPL when neither `-t` nor `--profile` is given (`-t wvlet` selects it explicitly):

```bash
# Query a local file
wvlet run "from 'data/orders.parquet' where o_totalprice > 1000 limit 10"

# Run a query file
wvlet run -f report.wv
```

`wvlet build` and `wvlet flow run <flow>` run model builds and flow stages on the engine by default. Their tables persist in the project's `target/warehouse` unless the profile's `wvlet` connector sets a `warehouse`; see [Engine Support](../syntax/flow.md#engine-support).

With a Trino or Snowflake profile, a query that reads only local files also runs on the engine.

Relative file paths resolve against the working folder (`-w`).

`-t wvlet` *without* `--host` runs the in-process engine; with `--host` it connects to a [remote wvlet server](../usage/remote-server.md) instead.


## Memory and spilling

On the JVM, an aggregation whose input would take the query's large in-memory structures (join tables, aggregation inputs, sort buffers) past half of the JVM's max heap (`-Xmx`) writes that input to temporary files and aggregates it in parts, instead of running out of memory. The files go in a folder of their own under the system temp folder (`java.io.tmpdir`), readable only by you, and are deleted when the query ends. Joins, sorts, and window functions don't spill yet. The browser and Node.js builds never spill, and the Native binary spills only when an embedding application sets a threshold. To spill earlier or later, pass a threshold in bytes to the JVM, e.g. `JAVA_OPTS=-Dwvlet.engine.spillThreshold=4000000000`; `-Djava.io.tmpdir=<folder>` moves the files. Applications that [embed the engine](wvlet-engine-embedding.md) set both with `WvletEngine.withSpillThreshold` and `withSpillDirectory`.

## Functions

Queries on the engine can call the [standard library](../syntax/stdlib.md) functions and the functions the engine implements. The engine has its own `wvlet` dialect context: it runs the DuckDB variants of the standard library (`def ... in duckdb`), and an engine-only variant can be written as `def ... in wvlet`. A function it lacks, such as DuckDB's `array_unique`, fails at compile time instead of at run time:

```
[FUNCTION_NOT_FOUND] Function array_unique is not supported by the wvlet engine. Use a standard library function, or run the query on the DuckDB connector (a profile with type: duckdb)
```

When the name is another engine's function that a standard library function covers, the message names that function instead (for example, `count_approx_distinct` for Trino's `approx_distinct`). Calls inside `sql"..."` fragments are not checked until the engine runs them.

## Mixing with other connectors

A profile can hold a `wvlet` connector next to other engines; switch to it with `use wvlet` (see [Working with Multiple Connectors](../usage/connectors.md)).

## Next

- [Tables and Files](wvlet-engine-tables.md): register tables, use the built-in TPC-H and TPC-DS catalogs, save results, and persist tables as Iceberg.
- [Performance](wvlet-engine-performance.md): TPC-H and TPC-DS at scale factor 10, against DuckDB, DataFusion, and Trino.
- [GPU Executor](wvlet-engine-gpu.md): the experimental cudf offload and its DGX Spark results.
- [Embedding](wvlet-engine-embedding.md): running the engine inside your application (planned).
