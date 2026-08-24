# Data Models

:::warning
This page is still work in progress.
:::

### Defining Data Models 

In Wvlet, you can define reusable data models, which wraps an Wvlet query with `model (model name) = { ... }` block:

```wvlet
model my_model = {
  -- Write your query here
  from ...
  ... 
}
```

Models can be used in other queries in the same manner with scanning a table:

```wvlet
from my_model
limit 10
```

Data models are often the units to __materialize query results into the target database tables__. If your data model needs to be accessed by multiple queries, materializing (or persisting) data models will reduce the cost of data processing and often accelerates the query processing.   

### Declaring Table Schemas

A `table` declaration describes a stored relation — its column names and types — so queries
referencing the table can be type-checked without connecting to the database:

```wvlet
table orders = {
  order_id: bigint
  status: string
}

-- Type-checks against the declaration above, and compiles to `select * from orders`
from orders
```

To describe a table that lives in a specific schema of your database, bind the declaration to
its location with `in <catalog>.<schema>`:

```wvlet
table orders in mydb.sales = {
  order_id: bigint
  status: string
}

-- Both resolve through the bound declaration and compile to a scan of mydb.sales.orders
from sales.orders
from mydb.sales.orders
```

Declarations are contracts, not actions: nothing is created when a declaration is read. Writes
materialize them — appending to a declared table that does not exist yet creates it from the
declared columns, and `create table <name>` reads its columns from the declaration (see
[Table Management](table-management.md)). Writes matching a bound declaration land at the bound
location, so a declared name never reads from one place and writes to another.

Notes on how bound declarations resolve:

- Catalog and schema names are matched case-insensitively, following SQL identifier semantics.
- A bare reference like `from orders` resolves through a bound declaration only when the binding
  matches the current catalog and schema of the compilation context, mirroring the search-path
  behavior of SQL engines.
- Connector names take precedence: a reference like `from myconnector.sales.orders` resolves
  through the connector configured in your profile, not through a bound declaration.
- Table declarations take precedence over the live database catalog, so committed declaration
  files act like a lockfile: the compile-time schema stays deterministic even when the database
  changes, while queries still execute against the real tables.

Instead of writing these declarations by hand, you can generate them from a live database with
[`wvlet catalog import`](../usage/catalog-import.md).

#### `table` vs `type`

Use `table` for anything that denotes a stored relation, and `type` for reusable types that make
no claim about storage: scalar domains with methods (e.g. `type ip_address extends string =
{ def country_name: string = ... }`), engine-dialect method extensions (`type any in duckdb =
{ ... }`), and abstract row shapes used only as column or parameter types. On a `type` or `def`,
`in <name>` always means an engine dialect; on a `table` declaration it always means a
`<catalog>.<schema>` storage location.

Declaring a table's shape with `type` (the pre-2026 spelling `type orders in mydb.sales = ...`)
still resolves, but reports a deprecation warning steering to the `table` spelling, and will be
removed in a future release.
