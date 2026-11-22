---
description: "Run Wvlet queries on the in-process wvlet engine, a columnar engine that executes Wvlet plans directly on Parquet, JSON, CSV, and Iceberg data with no database or DuckDB required."
---

# Connector - Wvlet Engine

The wvlet engine is an in-process columnar query engine that executes compiled Wvlet plans
directly — no SQL generation and no external database. It runs on every platform the CLI
supports (JVM, Node.js, and the native binary), reading Parquet, Iceberg, JSON, and CSV data
through pure-Scala readers, so it also works where DuckDB is unavailable.

:::warning Preview
The engine covers queries over files and registered tables (filters, projections, joins,
aggregations, window functions, subqueries, and TPC-H). Features it doesn't support yet report an
error: SQL DDL (`create table`, `drop`), `execute`, `append to`, `delete`, `save to` a file,
`pivot` / `unpivot`, `unnest`, ASOF joins, lambdas, `show` commands, and some engine-specific
functions.
:::

## Usage

Select the engine with `-t wvlet`:

```bash
# Query a local file
wvlet run -t wvlet "from 'data/orders.parquet' where o_totalprice > 1000 limit 10"

# Run a query file
wvlet run -t wvlet -f report.wv
```

Relative file paths resolve against the working folder (`-w`).

`-t wvlet` *without* `--host` runs the in-process engine; with `--host` it connects to a
[remote wvlet server](remote-server.md) instead.

## Registering tables

Define a profile with a `wvlet` connector to reference tables by name. Its properties register
tables:

- `dataDir`: a folder whose data files (`.parquet`, `.json`, `.jsonl`, `.csv`, `.tsv`, and their
  `.gz` variants) and sub-folders (Iceberg tables or folders of Parquet files) become tables named
  after the file name without its extension
- `tables`: an object mapping table names to paths

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "local-engine",
      "connectors": [
        {
          "name": "wvlet",
          "type": "wvlet",
          "properties": {
            // customer.parquet, orders.parquet, ... become tables customer, orders, ...
            "dataDir": "data",
            "tables": {
              "events": "logs/events.jsonl",
              "sales": "warehouse/sales"
            }
          }
        }
      ]
    }
  ]
}
```

```bash
wvlet run -p local-engine "from orders join customer on o_custkey = c_custkey agg _.count"
```

Paths relative to the working folder are resolved there. The schemas of registered tables are
visible to the compiler, so column names are checked and completed as for other engines.

## Saving results

`save to <table>` keeps a query result as an in-memory table for the rest of the session:

```wvlet
from 'data/orders.parquet'
where o_orderstatus = 'F'
save to finished_orders

from finished_orders
agg _.count
```

## Mixing with other connectors

A profile can hold a `wvlet` connector next to other engines; switch to it with `use wvlet`
(see [Working with Multiple Connectors](connectors.md)).
