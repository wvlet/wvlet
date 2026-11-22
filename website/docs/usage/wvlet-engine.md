---
description: "Run Wvlet queries on the in-process wvlet engine, a columnar engine that executes Wvlet plans directly on Parquet, JSON, CSV, and Iceberg data with no database or DuckDB required."
---

# Connector - Wvlet Engine

The wvlet engine is an in-process columnar query engine that executes compiled Wvlet plans
directly — no SQL generation and no external database. It runs on every platform the CLI
supports (JVM, Node.js, and the native binary), reading Parquet, Iceberg, JSON, and CSV data
through pure-Scala readers, so it also works where DuckDB is unavailable.

:::warning Preview
The engine covers queries over files and registered tables (filters, projections, joins
including ASOF joins, aggregations, window functions, subqueries, `pivot`, `unnest`, lambdas,
`show` commands, and TPC-H), plus `save to`, `append to`, and `delete` on in-memory tables.
Features it doesn't support yet report an error: SQL DDL (`create table`, `drop`), `execute`,
`update`, writes to files (`save to` / `append to` a file path), flows, table functions such as
`read_csv(...)`, and some engine-specific functions. `unpivot` and map values run but can return
results that differ from DuckDB.
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

Relative paths resolve against the working folder. The compiler sees the schemas of Parquet and
Iceberg tables (read from file footers), so their column names are checked as for other engines.
JSON and CSV tables are read only when a query scans them.

## Saving results

`save to <table>` keeps a query result as an in-memory table for the rest of the session, and
`append to` and `delete` change it:

```wvlet
from 'data/orders.parquet'
where o_orderstatus = 'F'
save to finished_orders

-- Add rows; `append to` a table that doesn't exist creates it
from 'data/orders_today.parquet'
where o_orderstatus = 'F'
append to finished_orders

-- Insert-or-update by key: rows with a matching o_orderkey update the existing rows
append to finished_orders on o_orderkey {
  from 'data/order_fixes.parquet'
}

-- Remove rows
from finished_orders
where o_totalprice < 100
delete
```

Appended rows map to the table's columns by position, or to the columns listed in
`append to t (c1, c2)` (the others become NULL), and are cast to the column types. A keyed
`append to t on k` matches columns by name, as SQL `MERGE` does: matched rows get the new values
of the query's columns, and rows with a NULL key always insert. Tables
registered from files (`dataDir`, `tables`) are read-only: writing to them reports an error
instead of changing the files.

## Mixing with other connectors

A profile can hold a `wvlet` connector next to other engines; switch to it with `use wvlet`
(see [Working with Multiple Connectors](connectors.md)).
