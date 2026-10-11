# ADR: Lowering un-inlined keyword-named member calls in the SQL generator

Date: 2026-09-12 · PR #2064

## Context

Wvlet spells SQL operators as stdlib member calls: `o_comment.like('%x%')`, `o_orderkey.in(1, 2)`,
`p_size.between(1, 5)`, `l_shipdate.extract('year')`. `FunctionInliner` resolves them through
the qualifier's type and inlines the stdlib `sql"..."` template, so `spec/tpch` (which declares
the tables in `schema.wv`) already generated the operator form. From a work folder without a
schema the qualifier's type is unknown, `resolveFunctionApply` leaves the
`FunctionApply(DotRef(qual, like), ...)` node untouched, and `SqlGenerator` printed it as a
function call whose name is a SQL keyword: `o_comment."like"(...)`. That is not valid SQL on any
engine, and 9 of 22 TPC-H queries failed only at execution time.

## Decision

- The fallback lives in `SqlGenerator`'s `FunctionApply` branch, next to the existing
  `RLike → regexp_matches` dialect fallback, not in `FunctionInliner` or the stdlib. The generator
  is the last place that still sees the un-inlined node, and keeping the language surface
  unchanged avoids widening the `any` type (adding `like` to `any` would make `1.like('x')`
  type-check and would churn the freshness-gated stdlib docs and engine catalogs).
- The lowering table is exactly the operator-shaped stdlib defs: `like`, `in`, `not_in`,
  `between`, `extract` (`SqlGenerator.operatorMethods`). The generator constructs the existing
  `Like` / `In` / `NotIn` / `Between` / `Extract` nodes and prints them through `expr`, so the text
  cannot drift from the parser-produced operators.
- The match mirrors typed resolution: exact (case-sensitive) method name on an
  `UnquotedIdentifier`, only plain arguments (no name, `DISTINCT` or `ORDER BY`), no window and no
  filter. Anything else falls through to the regular function-call printer as before. `in()` with
  no arguments is a `SYNTAX_ERROR`.
- There is deliberately **no fail-fast** for other keyword-named member calls. A schema-qualified
  function call such as `main.left(s, 1)` parses to the same `FunctionApply(DotRef(main, left))`
  shape and is valid SQL as `main."left"(s, 1)`; the generator has no type information to tell the
  two apart, so rejecting the shape would break qualified calls.

## Consequences

- Schema-less `wvlet compile` and the typed path produce the same SQL for these five methods;
  `TPCHSchemalessTest` runs all 22 queries compiled without `schema.wv` on DuckDB to guard it.
- `x.extract('year')` lowers to `extract (YEAR from x)` (the `Extract` node's spelling), while the
  stdlib inline form is `extract('year' from x)`; both run on DuckDB, the former is also Trino-safe.
- New operator-shaped stdlib defs must be added to `operatorMethods` to get the same fallback;
  non-keyword member calls on an untyped qualifier (`x.upper()`) still pass through as function
  calls and fail at execution if the engine has no such function.
- Measured while investigating the related "first-query latency" task: the first DuckDB JDBC
  connection costs ~1.5 s (native library load), the TPC-H extension plus `dbgen(sf=0.01)` under
  80 ms; the latency is not addressable from the TPC-H side.
