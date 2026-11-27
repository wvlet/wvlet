---
sidebar_position: 3
---

# Model Pipelines

A folder of `.wv` files can work like a [dbt](https://www.getdbt.com/) project. You define models,
mark the ones to persist, and `wvlet build` builds them in dependency order. Wvlet works out the
dependency graph from model references, so you don't need `ref()` calls or a separate DAG file.

## Materializing Models

Add a `with { }` config to a [model](data-models.md) to choose how `wvlet build` persists it:

```wvlet
-- models/staging.wv
model stg_orders with { materialized: 'view' } = {
  from raw.orders
  where status != 'cancelled'
}

-- models/marts.wv
model customer_totals with { materialized: 'table', schema: 'marts' } = {
  from stg_orders
  group by customer_id
  agg amount.sum as total
}
```

| `materialized:` | `wvlet build` writes | Referencing the model |
|-----------------|-------------------------|-----------------------|
| `ephemeral` (default) | Nothing | Inlines the model's query, as plain models always do |
| `view` | `create or replace view` (like `save as view`) | Reads the view during a build |
| `table` | `create or replace table ... as` (like `save to`) | Reads the table during a build |

Other config keys:

| Key | Description |
|-----|-------------|
| `schema` | Target schema. Defaults to the profile's current schema |
| `alias` | Target relation name. Defaults to the model name |
| `engine` | Profile connector to build the model on. Defaults to the default engine |
| `retries`, `timeout`, `retry_delay`, `backoff`, `max_retry_delay` | Retry and timeout settings for the model's build step, with the same meaning as [stage configuration](flow.md#stage-configuration) |

Configs are checked at compile time. An unknown key or materialization is an error. So is
materializing a model with parameters, because such a model has no single result to persist.

### Static types, inlined outside a build

A model reference keeps the model's static type wherever it appears. Wvlet never looks the
persisted relation up in the live database to type-check it.

- During `wvlet build`, a model that reads a materialized model reads its persisted table or view,
  as dbt's `ref()` does. This also applies when the read goes through ephemeral models without parameters.
- Everywhere else (`wvlet run`, the REPL, spec tests), models are inlined as before. Queries
  therefore never fail on a table that has not been built yet, and never read stale results.

## Building Models

```bash
# Build every materialized model in dependency order
wvlet build

# Print the build order and generated SQL without running anything
wvlet build --dry-run

# Build one model, its downstream models, or its upstream models
wvlet build -s customer_totals
wvlet build -s stg_orders+
wvlet build -s +customer_totals

# Several selectors, comma-separated
wvlet build -s stg_orders,customer_totals
```

| Selector | Selects |
|----------|---------|
| `m` | Model `m` only. The materialized models it reads must already be built |
| `m+` | `m` and every model downstream of it |
| `+m` | `m` and every model upstream of it |
| `+m+` | Both directions |

A model builds only after every selected model it reads. The build order lists models by
dependency level, in definition order within a level. A selector that names no model (for example
`-s ""`) is an error.

Materialized models must not form a dependency cycle. That includes a model that reads itself,
directly or through ephemeral models. `wvlet build` reports such a cycle as a path (for example
`Cyclic model dependency: a -> b -> a`) and builds nothing, even when the selection avoids the
cycle.

`wvlet build` takes `-w <folder>`, `-t <engine>`, `-p, --profile`, `--catalog`, `--schema`, and, on the JVM CLI, `--run-store`, as [`wvlet flow`](flow.md#wvlet-flow-cli) does. Builds run on the
in-process [wvlet engine](../engines/wvlet-engine.md) by default. The built tables persist as Iceberg
tables in the project's `target/warehouse` folder, so later builds of a selection find their
upstream models. A `wvlet` connector of a profile can set its own `warehouse` folder instead. When
the work folder has no `.wv` file directly in it, the folder is `~/.cache/wvlet/target/warehouse`,
which all such work folders share: give a project whose files are only in subfolders a `warehouse`
of its own. `target/` is disposable:
`append to` models whose history can't be rebuilt need an explicit `warehouse`. The engine can't
build `view` models yet; build them on DuckDB with `wvlet build -t duckdb` or a DuckDB profile.

On the JVM CLI, a build runs on the flow executor as a flow named `build`, with one stage per model:

- Models that don't depend on each other build in parallel.
- A failed model is retried as its config says. Models downstream of a failure are skipped, and the
  command exits with a non-zero code.
- Runs are recorded, so `wvlet flow session list` and `wvlet flow session show <run id>` show past
  builds.
- If a selection reads a materialized model that has never been built, the build stops before
  running anything and names the models to build first.

The npm and Native `wvlet` build the same models into the same `target/warehouse`, and print the same result, with these differences:

- Models build one at a time, in build order.
- `retries`, `retry_delay`, `backoff`, and `max_retry_delay` apply, but `timeout` is not enforced (a warning says so), so a slow attempt isn't stopped or retried.
- Runs aren't recorded, and `--run-store` is an error.
- A model can't read a model built on another `engine:`; the JVM CLI copies such tables between engines.
- Engines are the wvlet engine, DuckDB, and Trino; Snowflake and other JDBC connectors need the JVM CLI.

The built tables are read by later builds. To query them with `wvlet run` or the REPL, use a profile whose `wvlet` connector sets `warehouse` to the build's folder; `-t duckdb` builds into an in-memory database that is gone when the command ends, so persist DuckDB builds with a profile that sets a database `path`.

To recover from a failure, fix the model and run `wvlet build` again, or rerun only the failed part
with a selector such as `wvlet build -s failed_model+`.

## Feature Parity with dbt

| dbt | Wvlet |
|-----|-------|
| Models and `ref()` | `model` definitions referenced by name, type-checked at compile time |
| Materializations: view, table, ephemeral | `with { materialized: ... }` |
| Incremental models | Keyed upserts with [`append to t on keys`](table-management.md#keyed-append-insert-or-update). An `incremental` materialization is planned |
| `dbt build --select` | `wvlet build -s` |
| Sources | [`table` declarations](data-models.md#declaring-table-schemas) bound to `catalog.schema`. Freshness checks are planned |
| Seeds | `from 'file.csv'` with [`save to t if not exists`](table-management.md#seeding-a-table-once) |
| Snapshots (SCD2) | Planned |
| Singular data tests | [`test` assertions](test-syntax.md) |
| Generic tests (unique, not_null, relationships) | Column constraints on `table` declarations. Running them as data tests is planned |
| Jinja macros | Type-checked building blocks: reusable query fragments (`def is_adult = where age >= 18`) and scalar functions (`def double(x: int): int = x * 2`), parameterized models, [`for` loops](index.md#for-loops), interpolated table names (``s`name_${x}` ``), and `import` |
| `adapter.dispatch` | Engine-specific standard library and `trait ... in <dialect>` |
| `var()`, `env_var()` | Flow parameters. Model variables are planned |
| Pre- and post-hooks | `execute` statements in flows. Model hooks are planned |
| Docs and lineage | `---` doc comments. Generated docs and lineage are planned |
| Targets and profiles | [Profiles](../usage/connectors.md) |

Wvlet doesn't add a text-template layer like Jinja. Templates would lose compile-time type
checking, and with it the ability to compile one model to several SQL engines.
