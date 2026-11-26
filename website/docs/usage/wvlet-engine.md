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
`save to` / `append to` Parquet, CSV, and JSON files and Iceberg tables, and `create table`, `drop table`,
`truncate`, and `create` / `drop schema`. Features it doesn't support yet report an error:
`alter table`, views, `update`, `execute`, table functions such as `read_csv(...)`, and some
engine-specific functions. Map values run but can return results that
differ from DuckDB.
:::

## Functions

Queries on the engine can call the [standard library](../syntax/stdlib.md) functions and the functions the engine implements. The engine has its own `wvlet` dialect context: it runs the DuckDB variants of the standard library (`def ... in duckdb`), and an engine-only variant can be written as `def ... in wvlet`. A function it lacks, such as DuckDB's `array_unique`, fails at compile time instead of at run time:

```
[FUNCTION_NOT_FOUND] Function array_unique is not supported by the wvlet engine. Use a standard library function, or run the query on the DuckDB connector (a profile with type: duckdb)
```

When the name is another engine's function that a standard library function covers, the message names that function instead (for example, `count_approx_distinct` for Trino's `approx_distinct`). Calls inside `sql"..."` fragments are not checked until the engine runs them.

## Usage

The engine is the default of `wvlet run` and `wv` when neither `-t` nor `--profile` is given
(`-t wvlet` selects it explicitly):

```bash
# Query a local file
wvlet run "from 'data/orders.parquet' where o_totalprice > 1000 limit 10"

# Run a query file
wvlet run -f report.wv
```

`wvlet build` and `wvlet flow run <flow>` run model builds and flow stages on the engine by default.
Their tables persist in the project's `target/warehouse` unless the profile's `wvlet` connector
sets a `warehouse`; see [Engine Support](../syntax/flow.md#engine-support).

Use `-t duckdb` (or a DuckDB profile) for the features listed above as unsupported. With a
Trino or Snowflake profile, a query that reads only local files also runs on the engine.

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
wvlet run --profile local-engine "from orders join customer on o_custkey = c_custkey agg _.count"
```

Relative paths resolve against the working folder. The compiler sees the schemas of Parquet and
Iceberg tables (read from file footers), so their column names are checked as for other engines.
JSON and CSV tables are read only when a query scans them.

### Declaring table keys

Parquet files carry no key information, so the engine can't tell a join on a composite primary key from a join on columns that merely share values. Declare keys in a sidecar JSON file and the join planner uses them to estimate multi-column joins: a join that covers a declared key of one side is estimated as a foreign key to that primary key, and a join on columns that are no key of either table as independent columns.

- `<name>.keys.json` next to `<name>.parquet` declares the keys of that file. `<name>.parquet.keys.json` works too.
- `_keys.json` inside a folder of Parquet files declares the keys of that folder table. A `_keys.json` in a `dataDir` may instead hold `{"tables": {"<table>": {...}}}`, the keys of each Parquet file in the folder by table name.

```json title='data/lineitem.keys.json'
{
  "primaryKey": ["l_orderkey", "l_linenumber"],
  "uniqueKeys": [["l_comment"]],
  "foreignKeys": [
    {"columns": ["l_partkey", "l_suppkey"], "references": "partsupp", "referencedColumns": ["ps_partkey", "ps_suppkey"]}
  ]
}
```

- `primaryKey` is one column list; `uniqueKeys` is a list of column lists. Both mean that no two rows share all the listed columns' values.
- `foreignKeys` are recorded but not yet used in planning.
- Declare every unique key of a table once you declare one: a join on columns outside every declared key is then taken to be a non-key join.
- Keys only steer join order and build sides, never results. A wrong declaration can make a plan slower but can't change its answer.
- A sidecar that can't be parsed, or a key naming a column the table doesn't have, is ignored with a warning. Tables made by `create table` keep their declared `primary key` / `unique` columns themselves.

### Built-in tpch and tpcds catalogs

Every session can read generated TPC-H and TPC-DS tables as `tpch.<schema>.<table>` and `tpcds.<schema>.<table>`, with no profile setting. The schema names the scale factor, as in Trino's tpch and tpcds connectors: `tiny` is 0.01, and `sf<n>` is n (`sf1`, `sf10`, `sf100`). Rows are generated only while a query scans them:

```bash
wvlet run "from tpch.sf1.lineitem group by l_returnflag agg _.count"
wvlet run "from tpcds.tiny.store_sales join tpcds.tiny.item on ss_item_sk = i_item_sk group by i_category agg _.count"
```

- TPC-H goes up to `sf1000` and TPC-DS up to `sf10`; values differ from `dbgen` / `dsdgen`'s, as described below.
- Columns keep their TPC prefixes (`l_orderkey`), as in DuckDB's tables; Trino's tpch connector drops them unless `tpch.column-naming=STANDARD`.
- A table you register under the same qualified name takes precedence. To name the tables without a qualifier, use the profile properties below.

### TPC-H tables

`prepareTPCH: true` registers the eight TPC-H tables (`lineitem`, `orders`, `customer`, ...), generated while queries scan them, with no data files and no DuckDB. `tpchScaleFactor` sets the size (default 0.01, 60K line items; up to 1000). These are the same properties as the DuckDB connector's, so one profile works on both engines:

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "tpch",
      "connectors": [
        {
          "name": "wvlet",
          "type": "wvlet",
          "properties": { "prepareTPCH": true, "tpchScaleFactor": 1 }
        }
      ]
    }
  ]
}
```

```bash
wvlet run --profile tpch "from lineitem group by l_returnflag agg _.count"
```

- Rows, keys, and value domains follow the TPC-H specification, but the values aren't those of the reference `dbgen`, so query answers differ from the official ones. For benchmarks, generate Parquet files with DuckDB's `dbgen` and register them with `dataDir`.
- The tables are read-only, and a name that `dataDir` / `tables` also registers is an error.

### TPC-DS tables

`prepareTPCDS: true` registers the 24 TPC-DS tables (`store_sales`, `item`, `date_dim`, ...) the same way, sized by `tpcdsScaleFactor` (default 0.01, 29K store sales lines; up to 10):

```jsonc title='~/.wvlet/profiles.json'
{
  "profiles": [
    {
      "name": "tpcds",
      "connectors": [
        {
          "name": "wvlet",
          "type": "wvlet",
          "properties": { "prepareTPCDS": true, "tpcdsScaleFactor": 1 }
        }
      ]
    }
  ]
}
```

```bash
wvlet run --profile tpcds "from store_sales join item on ss_item_sk = i_item_sk group by i_category agg _.count"
```

- `date_dim`, `time_dim`, `customer_demographics`, `household_demographics`, and `income_band` equal those of DuckDB's `dsdgen`. The other tables have `dsdgen`'s columns, row counts, keys, NULL shares, and order structure (ticket lines sharing a customer, returns matching their sale), but random values drawn from the literals the TPC-DS queries filter on, so answers differ from the official ones.
- 96 of the 99 TPC-DS queries return rows where they do on `dsdgen`'s tables at scale factor 0.1.
- TPC-H and TPC-DS both have a `customer` table, so a connector can register only one of them.

## Saving results

`save to <table>` keeps a query result as a table, and `append to` and `delete` change it.
Without a [warehouse](#persistent-tables), tables live in memory for the rest of the session:

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

`create table` makes an empty table from a `table` declaration, including column
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

## Changing tables

`reshape` (or SQL `alter table`) changes a table's columns, keeping its rows:

```wvlet
reshape accounts {
  add signup_date: date      -- NULL in existing rows
  rename status as state
  cast id as long            -- change a column's type
  exclude email              -- drop a column
  set state not null         -- existing rows must have a state
  set state = 'active'       -- default for rows appended without it
  drop state not null
  drop state default
}

rename table accounts to members
```

Tables registered from files can't be changed.

## Persistent tables

Give the connector a `warehouse` folder to keep tables across sessions, as a DuckDB database file
does. Every table written by name (`create table`, `save to t`, `append to t`) goes there and is
read back by the next session:

```jsonc title='~/.wvlet/profiles.json'
{
  "name": "wvlet",
  "type": "wvlet",
  "properties": {
    "warehouse": "warehouse"
  }
}
```

- Each table is an [Iceberg](#iceberg-tables) table at `<warehouse>/<schema>/<table>`: tables of
  the default schema `main` are under `warehouse/main/`, and `create schema s` makes the
  `warehouse/s/` folder. DuckDB, Spark, Trino, and pyiceberg read the tables too
- Every write is one atomic Iceberg commit: `save to t` and `truncate t` replace the rows in a new
  snapshot (`save to` may change the columns), `append to t` adds a snapshot, and `delete` and
  `append to t on k` rewrite only the data files holding the rows they change (copy-on-write).
  Column statistics in the manifests let reads, deletes, and keyed appends skip data files that
  can't hold a matching row
- `reshape` / `alter table` change the table's metadata only when Iceberg allows it (adding a
  column without a default, dropping or renaming a column, widening `int` to `long` or a
  decimal's precision, defaults, dropping `not null`); other changes rewrite the table in one
  snapshot
- `rename table t to u` moves the table's folder and rewrites its metadata for the new location,
  so other engines read it there. It keeps only the current snapshot, so time travel to older
  snapshots ends; a table with Iceberg branches or tags other than `main` can't be renamed
- Every commit adds an Iceberg snapshot, and old snapshots keep their data files. Set
  `keepSnapshots` to keep only the newest snapshots: after each commit, older ones are expired and
  the files only they reference are deleted. Unset, every snapshot is kept

  ```jsonc
  "properties": { "warehouse": "warehouse", "keepSnapshots": 10 }
  ```
- Declared defaults and `not null` / `primary key` / `unique` constraints are kept in the table's
  properties and checked by later sessions too
- `drop table` deletes the table's folder, and `drop schema s with cascade: true` the schema's
- A name that is both a warehouse table and a `dataDir` / `tables` entry is an error when the
  session starts. Tables registered from files stay read-only
- Without `warehouse`, tables are in memory only (like DuckDB's `:memory:`). The browser
  playground always keeps tables in memory

## Writing files

`save to '<path>'` writes a query result to a file, and `append to '<path>'` adds rows to it
(creating the file when it doesn't exist). The extension selects the format:

| Extension | Format |
|-----------|--------|
| `.parquet` | Parquet |
| `.csv`, `.tsv` | CSV / TSV with a header row |
| `.json`, `.jsonl`, `.ndjson` | Newline-delimited JSON |

The same writers serve `save to '<path>'` on engines that can't write local files (Trino,
Snowflake): the result streams from the engine page by page into the file, in any format above.

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

## Iceberg tables

The engine reads and writes [Apache Iceberg](https://iceberg.apache.org/) tables in local folders,
on every platform, with no Spark or DuckDB. A folder is an Iceberg table when it has a `metadata/`
folder holding `.metadata.json` files.

```wvlet
-- Create a table (snapshot 1), partitioned by country
from 'data/events.parquet'
save to 'warehouse/events' with format: 'iceberg', partition_by: 'country'

-- Add a snapshot
from 'data/events_today.parquet'
append to 'warehouse/events'

-- Read the current snapshot, or the one a metadata file records
from 'warehouse/events'
from 'warehouse/events/metadata/v1.metadata.json'
```

- `save to '<dir>' with format: 'iceberg'` creates a table in a new or empty folder. Options:
  `partition_by` (comma-separated identity partition columns) and `compression`. On an existing
  Iceberg table, `save to` commits a snapshot that replaces every row: the schema follows the
  result, and the table keeps its partition spec unless `partition_by` is given or the result drops a partition column. Older snapshots
  stay readable from their `metadata/vN.metadata.json` files. A non-empty folder that isn't an
  Iceberg table is an error
- `append to '<dir>'` commits the rows as a new snapshot, mapped to the table's columns by position
  and cast to their types. It works on tables committed as `metadata/vN.metadata.json` (as the
  wvlet engine and Hadoop-catalog writers create them); a table that a catalog owns, such as one
  written by pyiceberg or a REST catalog, is read-only. `append to` a folder that isn't an Iceberg
  table is an error, since `append to` can't name the format
- Reads cover partitioned tables, format versions 1 to 3, and schema evolution (promoted, added,
  dropped, and renamed columns, matched by Iceberg field ID). Position deletes, deletion vectors,
  and equality deletes are applied, so rows deleted merge-on-read don't appear. Tables with ORC
  data files or encryption aren't supported and report an error
- The engine never deletes table files; a failed commit removes only the files it wrote

## Mixing with other connectors

A profile can hold a `wvlet` connector next to other engines; switch to it with `use wvlet`
(see [Working with Multiple Connectors](connectors.md)).
