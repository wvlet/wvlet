# Qualify columns by a model's name

## Goals

- `from stg_orders join stg_customers on stg_orders.customer_id = stg_customers.customer_id` works like the same query over tables, on DuckDB/Trino SQL and on the wvlet engine.
- Users stop needing `as o` / `as c` aliases only to disambiguate joined models.

## Background

- `GenSQL.expandRelation` inlines a model body in place of its `ModelScan`, and both SQL generation and the wvlet engine (`WvletEngine` calls `GenSQL.expand`) consume that expansion.
- The inlined body carries no name: DuckDB sees `join (select ...)` as `unnamed_subquery` ("Referenced table mq_orders not found"), and the engine's `Binder` finds no field qualified by `mq_orders` (COLUMN_NOT_FOUND).
- A user alias (`from stg_orders as o`) works because the `AliasedRelation` survives the expansion.
- Name resolution in the typer (the Lean `NameLookup` model's scope) is not involved: the typer already accepts the query.

## Design

- Lane: **Express**: one-layer fix with a clear diagnosis, no user-facing surface beyond making a documented-looking query work, no always-gate area.
- When a model reference is expanded, wrap the inlined body in an alias named after the model, the way a table is qualified by its own name.
- When the reference already has a user alias (`stg_orders as o`), keep only the user's alias, matching SQL where an aliased table is no longer reachable by its table name.
- Generated SQL for every model reference gains `as <model_name>` after the subquery; that is valid in all target dialects.
- Tests: a `spec/basic/model` case run on DuckDB and the engine, plus a GenSQL test on the generated SQL; then switch the playground's layered-models example back to model-name qualifiers.

## Alternatives and Why Not?

- Fix in the engine's Binder only: DuckDB/Trino SQL would still fail.
- Wrap only when the query uses the model name as a qualifier: saves a few SQL tokens but needs a usage scan and makes SQL shape depend on distant references.
