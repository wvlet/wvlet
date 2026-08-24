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

### Traits: Method Interfaces

A `trait` attaches reusable `def` members to a type — a method interface in the Scala/Rust
sense. Declare a new value domain by extending a base type, then use it as a column type; the
columns get the trait's methods:

```wvlet
trait ip_address extends string = {
  def country_name: string = sql"ip_to_country(${this})"
}

table access_logs = {
  time: timestamp
  client_ip: ip_address

  -- Row methods can call trait methods of the declared column types
  def client_country: string = client_ip.country_name
}

-- Both trait and row methods resolve directly on a scan of the declared table
from access_logs
select time, client_ip.country_name, _.client_country
```

A trait re-opening an existing type with a *dialect scope* provides engine-specific
implementations — the standard library defines its methods this way:

```wvlet
trait ip_address in duckdb extends string = {
  def country_name: string = sql"'N/A'"   -- DuckDB has no IP database
}
```

Trait bodies carry `def` members only — column fields are a compile-time error, because a trait
never describes storage. Likewise `in` on a trait always names an engine dialect; binding a
trait to a `<catalog>.<schema>` location is an error (declare a `table` instead).

#### `table` vs `trait` vs `type`

Everything the family declares is a type — as in Scala, where traits and classes all introduce
types while the `type` keyword itself covers the general forms. `table` and `trait` are
specialized kinds of type that add a commitment on top; `type` remains for declarations that
make neither:

- **`table`** — a type committed to storage: a stored relation with columns, an optional
  `in <catalog>.<schema>` location, and optional row methods.
- **`trait`** — a type committed to being a method interface: new value domains
  (`trait ip_address extends string`) and engine-dialect method packages
  (`trait any in duckdb`). Never storage.
- **`type`** — the general form, making no commitment: aliases and marker types
  (`type td_trino extends trino`), and structural row or value shapes used as column or
  parameter types (`type point = { x: long, y: long }` with `start: point`).

Only the commitment-claiming `type` spellings are deprecated, each warning toward its
specialized keyword: a columns-carrying `type` that resolves a table reference is a storage
claim and warns toward `table`, and a def-only `type` is a method interface and warns toward
`trait`. Structural types, aliases, and markers are the permanent `type` usage and never warn.

The `in` clause is unambiguous across the family: on a `trait` or `def` it always names an
engine dialect; on a `table` declaration, `create schema`, or `use` it always names a storage
location.

#### Composing Declarations with Mixins

Since `table` and `trait` are kinds of type, a declaration can compose others with a
comma-separated `extends` list: structural `type` shapes and `table` declarations contribute
their columns, and `trait` parents contribute their `def` members (including dialect-scoped
variants):

```wvlet
type timestamped = { created_at: timestamp, updated_at: timestamp }
trait auditable = { def is_recent: boolean = created_at > now() - interval '7 days' }

table events extends timestamped, auditable = {
  id: int
  label: string
}

from events
select created_at, id, _.is_recent
```

The composed shape places mixed-in columns before own body columns, in parent-list order.
A column reached through multiple parents with the same type appears once, so diamond mixins
are fine; the same name with conflicting types is a compile-time error. Method precedence
follows Scala 3's intuition: own body defs shadow mixed-in ones, and among parents the later
one wins (`extends a, b` means b refines a).

Because a trait never describes storage, a `trait` cannot extend a column-carrying
declaration — that is a compile-time error pointing toward `table`. A `table` extending a
trait gains its methods; the single-parent forms (`trait ip_address extends string`,
`type td_trino extends trino`) are unchanged as the one-element case of the same list.
