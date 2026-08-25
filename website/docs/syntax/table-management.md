# Managing Tables and Schemas

Wvlet provides native statements for creating and managing database schemas and tables, so
pipeline scripts no longer need `execute sql"..."` escapes for everyday catalog work.

## Schema Management

Create or drop a database schema (namespace). Modifiers like `if not exists` trail the
name, following Wvlet's left-to-right style:

```wvlet
create schema staging
create schema staging if not exists

drop schema staging
drop schema staging if exists
```

A schema can carry a self-describing storage location with `in '<uri>'`, and engine
properties with `with key: value` options. Both render as schema properties on engines that
support them (e.g. Trino's `WITH (location = ...)`) and are rejected with a clear error on
engines that don't (e.g. DuckDB):

```wvlet
create schema sales in 's3://bucket/sales/' if not exists
create schema sales in 's3://bucket/sales/' with owner: 'etl'
```

To drop a schema together with the objects it contains, pass `cascade: true` — an engine
modifier riding in an option, keeping the keyword surface flat:

```wvlet
drop schema staging if exists with cascade: true
```

## Declaring a Table Shape

A `table` declaration describes a table's columns using the same `name: type` notation as
`type` definitions. The declaration itself is side-effect free — it only registers the
schema for type checking, so declarations can live in shared files that other scripts
`import` without touching the database:

```wvlet
table users = {
  id: int
  name: string
  created_at: timestamp
}
```

### Column Defaults

A column can declare a default value with `= expr`. The default renders as a `DEFAULT`
clause in the generated `CREATE TABLE` — both for explicit `create table` actions and for
tables auto-created by `append to` — so the engine fills omitted columns on insert:

```wvlet
table users = {
  id: int
  name: string
  active: boolean = true
  created_at: timestamp = now()
}

create table users

-- Appending only a subset of columns: active and created_at get their defaults
append to users (id, name) {
  from new_signups select id, name
}
```

Defaults compose with `like` and `extends`: a reused or mixed-in column keeps its declared
default. Engines whose `CREATE TABLE` grammar has no `DEFAULT` clause (e.g. Trino) reject a
create with column defaults at compile time instead of silently dropping them.

### Reusing a Shape with `like`

`table <name> like <source>` declares a second table with the same columns as an existing
declaration, so a shape is written exactly once. Everything that works for a normal
declaration — type checking, `create table`, automatic creation on write — reads its columns
through the `like` reference:

```wvlet
table users_backup like users

from users
append to users_backup   -- auto-creates users_backup with users' columns if missing
```

### Composing Shapes with `extends`

Where `like` copies one declaration exactly, `extends a, b, c` composes partial shapes: each
parent in the comma-separated list contributes its columns (structural `type` shapes and
`table` declarations) or its methods (`trait` parents), followed by the declaration's own
body columns:

```wvlet
type timestamped = {
  created_at: timestamp
  updated_at: timestamp
}

trait auditable = {
  def is_recent: boolean = created_at > now() - interval '7 days'
}

table events extends timestamped, auditable = {
  id: int
  label: string
}

-- events has columns (created_at, updated_at, id, label), and auditable's methods
from events
where _.is_recent
```

Mixed-in columns come before own body columns, in parent-list order. A column arriving
through two parents with the same type (a diamond) appears once; the same name with
conflicting types is a compile-time error. `create table` and automatic creation on write
materialize the full composed shape. `extends` and `like` cannot combine on one declaration —
`like` is the exact-copy form.

### Binding a Declaration to a Location

`in <catalog>.<schema>` binds a declaration to the table's location, so references type-check
and compile to qualified scans without a live catalog connection, and writes land at the bound
location (see [Data Models](data-models.md) for the resolution rules):

```wvlet
table events in mydb.analytics = {
  id: int
  label: string
}

-- Reads and writes both resolve to mydb.analytics.events
from analytics.events
create table analytics.events if not exists
```

This is the form [`wvlet catalog import`](../usage/catalog-import.md) generates for every table
of your database.

### Row Methods

A declaration body can also carry `def` members — reusable expressions over the table's
columns — so behavior travels with the table declaration. Methods resolve on any relation
that carries the declared type: a direct scan of the table, or a model annotated with the
type (`model all_users: users = { ... }`):

```wvlet
table users = {
  id: int
  name: string
  deleted_at: timestamp

  def is_active: boolean = deleted_at is null
}

from users
where _.is_active
```

## Creating and Dropping Tables

The `create table` action materializes a declared shape in the target database. It takes
nothing after the table name — the columns come from the `table` declaration — and it
follows SQL semantics exactly, so there is nothing new to learn:

```wvlet
create table users                  -- fresh create; error if the table already exists
create table users if not exists   -- create if missing; silently skip otherwise
create or replace table users      -- drop and recreate (empties the table)

drop table users
drop table users if exists

truncate users         -- delete all rows, keep the table
```

Running `create table` without a matching `table` declaration in scope is a compile-time
error.

In most pipelines an explicit `create table` is unnecessary: writing to a declared table
with `save to` or `append to` creates it automatically (see below). `create table` exists
for tables that only external systems write to.

## Writing Query Results to Tables

The `save to` and `append to` operators work in two spellings that compile identically.
The *flow* form ends a query, which is convenient while exploring interactively:

```wvlet
from orders
where amount > 0
save to cleansed_orders
```

The *block* form leads with the effect, which makes committed pipeline scripts easier to
scan — each statement announces what it writes before the query details:

```wvlet
save to cleansed_orders {
  from orders
  where amount > 0
}

append to events(id, name) {
  from staged_events
}
```

### Seeding a Table Once

`save to` replaces the target table by default. Adding `if not exists` makes it a
seed-once operation: the whole statement is a no-op when the table already exists, so
re-running the script never overwrites or duplicates data:

```wvlet
save to calendar if not exists {
  from date_dimension_source
}
```

### Automatic Table Creation

Writing to a table that does not exist yet creates it — no `create table` step is needed.
`save to` always creates (or replaces) the target. `append to` creates the missing table
before inserting; when a `table` declaration is in scope, the table is created with the
*declared* column types, so the declaration — not the first query's inferred types — decides
the shape:

```wvlet
table events = {
  id: long
  name: string
}

-- Runs in a completely fresh database: creates `events` with (id: long, name: string),
-- then inserts the rows
append to events {
  from [[1, 'click'], [2, 'view']] as t(id, name)
}
```

| Statement | Table missing | Table exists |
|-----------|---------------|--------------|
| `save to t` | Created from the query | Replaced with the new query result |
| `save to t if not exists` | Created from the query | No-op (seed once) |
| `append to t` | Created (declared shape if available), then rows inserted | Rows are appended |
| `create table t` | Created from the declaration | Error (SQL semantics) |
| `create table t if not exists` | Created from the declaration | No-op |
| `create or replace table t` | Created from the declaration | Dropped and recreated empty |

## Updating Rows

The `update` operator follows the same pattern as `delete`: select the rows first with
`where`, then say what changes. The input must be filters over a single table.

```wvlet
from users
where last_login < current_date() - '1 year':interval
update status = 'dormant', updated_at = now()
```

## Keyed Append (Insert or Update)

`append to ... on <keys>` is insert-or-update: rows whose key columns match an existing
row replace its non-key columns; the rest insert. Plain `append to` stays pure insert.
It works in both flow and block forms and compiles to `MERGE INTO`:

```wvlet
from staged_users
append to users on user_id

append to metrics on date, metric_name {
  from daily_metrics
}
```

## Changing a Table's Columns

The `reshape` statement evolves a table's schema using the same column operators you already
use in queries — `add`, `rename ... as`, `exclude`, and `cast ... as` — enclosed in a block.
Operations run in order, one `ALTER TABLE` statement each:

```wvlet
reshape users {
  add email: string
  rename name as full_name
  exclude age
  cast user_id as long
}
```

Each operation is retry-safe: `add` is a no-op when the column already exists with that type,
`exclude` when it is already gone, and `cast` when the column already has the target type.
When a table declaration and the database schema have drifted apart,
[`wvlet catalog diff`](../usage/catalog-import.md#detecting-schema-drift) generates a
ready-to-run `reshape` block for the migration.

## Renaming Tables and Schemas

Renaming changes an object's identity rather than its shape, so it is a direct statement:

```wvlet
rename table users to customers
rename schema staging to archive
```

## Views

A view exports a query to engine-side consumers (BI tools, other SQL users). Like `save to`,
view creation works in both spellings and is create-or-replace:

```wvlet
-- Flow form: develop the query, then persist it
from users
where active = true
save as view active_users

-- Block form: lead with the effect in pipeline scripts
save as view active_users {
  from users
  where active = true
}

drop view active_users
drop view active_users if exists
```

## Attaching External Databases

`use '<path>' as <name>` attaches an external database under an alias, making its tables
addressable as `<name>.<table>`. The path is self-describing: a file path attaches a database
file, and a URI scheme such as `postgres://` selects the matching engine automatically.

```wvlet
-- Attach a local DuckDB file
use 'archive.duckdb' as archive

from archive.events
where year = 2025

-- Attach a remote PostgreSQL database (engine inferred from the scheme)
use 'postgres://host/mydb' as pg

-- Engine-specific modifiers ride in trailing options
use 'archive.duckdb' as archive with read_only: true
```

Attachment is currently supported on DuckDB. Options are passed through to the engine
(`read_only: true` becomes `READ_ONLY`); `engine: '<name>'` overrides the engine type
inferred from the path.
