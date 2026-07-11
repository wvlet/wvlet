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

### Describing Table Schemas with Types

A `type` definition describes the schema (column names and types) of a table, so queries
referencing the table can be type-checked without connecting to the database:

```wvlet
type orders = {
  order_id: bigint
  status: string
}

-- Type-checks against the type definition above, and compiles to `select * from orders`
from orders
```

To describe a table that lives in a specific schema of your database, bind the type to its
location with `in <catalog>.<schema>`:

```wvlet
type orders in mydb.sales = {
  order_id: bigint
  status: string
}

-- Both resolve through the bound type and compile to a scan of mydb.sales.orders
from sales.orders
from mydb.sales.orders
```

Notes on how bound types resolve:

- Catalog and schema names are matched case-insensitively, following SQL identifier semantics.
- A bare reference like `from orders` resolves through a bound type only when the binding
  matches the current catalog and schema of the compilation context, mirroring the search-path
  behavior of SQL engines.
- The binding must be the two-part `<catalog>.<schema>` form. A single name after `in` keeps its
  existing meaning as a dialect scope (e.g. `type string in duckdb`).
- Connector names take precedence: a reference like `from myconnector.sales.orders` resolves
  through the connector configured in your profile, not through a bound type.
- Type definitions take precedence over the live database catalog, so committed type files act
  like a lockfile: the compile-time schema stays deterministic even when the database changes,
  while queries still execute against the real tables.
