# ADR: Materialized models build on the flow executor and stay statically typed

Date: 2026-11-18 · PR #2179

## Context

Wvlet models were always inlined, so a dbt-style project had to order its `save to` statements by hand. Adding dbt-like materializations raised three questions: where the config lives, what `from <materialized model>` means, and what runs the dependency graph. dbt answers these with YAML config, Jinja `ref()` that always reads the persisted relation, and its own scheduler.

## Decision

- Model config uses the same `with { key: value }` block as flows and stages (`materialized: 'table' | 'view' | 'ephemeral'`, plus `schema`, `alias`, `engine`, and the stage retry/timeout keys). It is checked at compile time. There is no YAML project file and no text-template layer: Jinja-style templating would lose type checking and compilation to several engines.
- A reference to a model always keeps the model's static type, taken from the model definition. The live catalog is never consulted.
- Only inside `wvlet build` does a reference to a materialized model read its persisted relation. This applies to direct reads and to reads through ephemeral models without parameters. Everywhere else (`wvlet run`, the REPL, specs), models stay inlined, so a query never fails on an unbuilt table or reads a stale one.
- `wvlet build` derives the graph from model references and compiles it into a flow named `build`, with one stage per model. The body of each stage is a `Save` statement, which `FlowExecutor` writes to the save target. Builds therefore reuse the existing parallelism, retries, skipping of downstream models, and run records instead of adding a second scheduler.
- A dependency cycle among materialized models is an error for the whole project, whatever the selection, and is reported as a path (`a -> b -> a`). This includes a model that reads itself, directly or through ephemeral models. dbt fails the same way. Rejecting only cycles inside the selection would let `wvlet build -s a` read a stale relation of a model that cannot be built from scratch (found while proving the build order in Lean).
- A target follows a `table ... in <catalog>.<schema>` binding that governs the model's name, so writes, downstream reads, and build checks all use one relation. A `schema:`/`alias:` that conflicts with such a binding is an error.

## Consequences

- Behavior is the same inside and outside a build, but the cost differs: outside a build, downstream queries recompute their upstream models.
- An ephemeral model with parameters is still inlined during SQL generation, so a materialized model it reads is recomputed instead of read from storage.
- SQL is generated for the default engine of the profile. A model with `engine:` set to a connector of another dialect relies on that engine accepting the generated DDL.
- Incremental models, snapshots, data tests, variables, and hooks are expected to be added as further materializations or config keys on the same block (see `website/docs/syntax/model-pipelines.md`).
