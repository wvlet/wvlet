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
`show` commands, and TPC-H), plus `save to`, `append to`, and `delete` on in-memory tables and
`save to` / `append to` Parquet, CSV, and JSON files, and `create table`, `drop table`,
`truncate`, and `create` / `drop schema`. Features it doesn't support yet report an error:
`alter table`, views, `update`, `execute`, flows, table functions such as `read_csv(...)`, and some
engine-specific functions. Map values run but can return results that
differ from DuckDB.
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
`append to t (c1, c2)` (the others get their declared default, or NULL), and are cast to the
column types. A keyed
`append to t on k` matches columns by name, as SQL `MERGE` does: matched rows get the new values
of the query's columns, and rows with a NULL key always insert. Tables
registered from files (`dataDir`, `tables`) are read-only: writing to them reports an error
instead of changing the files.

## Creating tables

`create table` makes an empty in-memory table from a `table` declaration, including column
defaults and `not null`, `primary key`, and `unique` constraints, which every write to the table
checks:

```wvlet
table accounts = {
  id: int primary key
  email: string unique not null
  status: string = 'active'
}

create table accounts
-- Fills status with its default
append to accounts (id, email) {
  from [[1, 'alice@example.com']] as t(id, email)
}

truncate accounts
drop table accounts
```

- `create table t if not exists` keeps an existing table, and `create or replace table t` starts
  over; a plain `create table` of an existing table is an error
- `append to` a declared table that doesn't exist yet creates it from the declaration first
- `create schema s` and `save to s.t` group tables under a schema; `drop schema s with cascade:
  true` drops them, and `drop schema s` of a schema that still holds tables is an error
- `drop table` of a table registered from files only unregisters it; the files stay as they are

## Writing files

`save to '<path>'` writes a query result to a file, and `append to '<path>'` adds rows to it
(creating the file when it doesn't exist). The extension selects the format:

| Extension | Format |
|-----------|--------|
| `.parquet` | Parquet |
| `.csv`, `.tsv` | CSV / TSV with a header row |
| `.json`, `.jsonl`, `.ndjson` | Newline-delimited JSON |

```wvlet
from 'data/orders.parquet'
where o_orderstatus = 'F'
save to 'out/finished_orders.parquet' with compression: 'zstd'

from 'data/orders_today.parquet'
where o_orderstatus = 'F'
append to 'out/finished_orders.parquet'
```

Options for `save to ... with`:

- `format`: `parquet`, `csv`, `tsv`, or `json`, overriding the extension
- `compression` (Parquet): `snappy`, `zstd`, `gzip`, `brotli`, `lz4`, or `uncompressed`
- `header` (CSV, default `true`) and `delimiter` (CSV)

Appended rows map to the file's columns by position. CSV, TSV, and JSON lines files get the new
rows added at the end, leaving the existing text as is; Parquet files are rewritten with the new
rows cast to the file's column types, through a temporary file so a failed append keeps the
original. Files are written to local paths only; writing to `s3://` or other remote paths isn't
supported yet.

## Mixing with other connectors

A profile can hold a `wvlet` connector next to other engines; switch to it with `use wvlet`
(see [Working with Multiple Connectors](connectors.md)).
