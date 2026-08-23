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

## Creating and Dropping Tables

The `create table` action materializes a declared shape in the target database. It takes
nothing after the table name — the columns come from the `table` declaration — and it is
idempotent: if the table already exists, the statement is a no-op.

```wvlet
create table users     -- creates the table from its declaration if missing

drop table users
drop table users if exists

truncate users         -- delete all rows, keep the table
```

Running `create table` without a matching `table` declaration in scope is a compile-time
error.

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

| Statement | Behavior when the table exists |
|-----------|-------------------------------|
| `save to t` | Replaced with the new query result |
| `save to t if not exists` | No-op (seed once) |
| `append to t` | Rows are appended |
| `create table t` | No-op (idempotent) |

## Changing a Table's Columns

The `reshape` statement evolves a table's schema using the same column operators you already
use in queries — `add`, `rename ... as`, and `exclude` — enclosed in a block. Operations run
in order, one `ALTER TABLE` statement each:

```wvlet
reshape users {
  add email: string
  rename name as full_name
  exclude age
}
```

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
