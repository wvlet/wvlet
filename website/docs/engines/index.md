---
sidebar_label: Choosing an Engine
description: "Wvlet runs queries on its own in-process engine by default, or compiles them to SQL for DuckDB, Trino, and Snowflake. Pick the engine that fits where your data lives."
---

# Choosing an Engine

The same Wvlet query runs on Wvlet's own engine or, compiled to SQL, on a database. `wvlet run` and `wv` use the Wvlet engine unless you pass `-t` or `--profile`.

| Engine | Pick it when | Select it with |
| --- | --- | --- |
| [Wvlet engine](wvlet-engine.md) | Your data is in local Parquet, Iceberg, JSON, or CSV files, or you want no database to install | Default; `-t wvlet` |
| Wvlet engine in the browser or Node.js | You want to try Wvlet with no install, or run it from the npm CLI | The [Playground](../usage/playground.md); `wvlet run` from `@wvlet/cli` |
| [Wvlet engine with GPU](wvlet-engine-gpu.md) (experimental) | You have an NVIDIA GPU and want to try offloading joins and aggregations | sbt only, from the repository |
| [DuckDB](duckdb.md) | You need `sql"..."` / `execute` statements, `.duckdb` files, DuckDB extensions, or table functions such as `read_csv(...)` | `-t duckdb` or a `"type": "duckdb"` profile (npm CLI, or the JVM full package) |
| [Trino](trino.md) | Your tables live in a Trino cluster | A Trino profile, `--profile <name>` |
| [Snowflake](snowflake.md) | Your tables live in Snowflake | A Snowflake profile, `--profile <name>` (JVM full package) |

- `wvlet compile` generates DuckDB SQL by default; see [Generate SQL Queries](../usage/cli.md).
- One profile can hold several connectors, and `use <connector>` switches between them; see [Working with Multiple Connectors](../usage/connectors.md).
- With `--host`, `-t wvlet` sends queries to a [remote Wvlet server](../usage/remote-server.md) instead of running them in-process.
