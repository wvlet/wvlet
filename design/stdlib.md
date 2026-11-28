# Standard library and function catalogs

This page owns the stdlib (`wvlet-stdlib/module/standard/*.wv`), the per-engine function catalogs generated from it, and the docs page that lists it. Function naming follows [the stdlib naming ADR](../adr/2026-11-23-stdlib-function-naming.md): the stdlib is Wvlet's engine-neutral function contract, so new functions get stdlib names, not engine-specific ones.

## Freshness gates

The stdlib is freshness-gated in CI. After changing `wvlet-stdlib/module/standard/*.wv`, or bumping an engine dependency, regenerate:

- The DuckDB catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator"`
- The Trino catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibTrinoFunctionCatalogGenerator"`
- The docs reference page, with the results of the `-- @example` lines run on the engine: `./sbt "engineJVM/Test/runMain wvlet.lang.engine.StdLibDocGenerator"`
- The engine's function names (`WvletEngineFunctionNames`, after adding or removing an engine function): `./sbt "engineJVM/Test/runMain wvlet.lang.engine.EngineFunctionNamesGenerator"`
- The Snowflake SQL of the stdlib specs (`spec/snowflake-sql/`, after a change to a stdlib function or a stdlib spec): `./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.SnowflakeSqlGolden"`

`StdLibCatalogFreshnessTest`, `StdLibDocFreshnessTest`, `EngineFunctionNamesTest`, and `SnowflakeSqlGoldenTest` fail on drift and print the exact command to run.

## Reference page and examples

- The [reference page](../website/docs/syntax/stdlib-reference.md) and the playground's Reference tab are built from one model, `StdLibReference` (wvlet-lang): entries by category, one entry per function name and arity, merged across receiver types (`T.abs: T` with T: int, long, ...) and dialect variants.
- `StdLibCategories` assigns categories by receiver type, with name overrides (`regexp_*` → Pattern Matching, `sum` on arrays → Aggregate). A new function always gets a category; add a name override when the type default is wrong.
- A `-- @example <expr or query>` line in a definition's doc comment adds an example. `StdLibExamples` (wvlet-engine) runs it on the engine; the page shows that result and the playground computes it live, so results are never written by hand. Generating the page fails when an example fails, so `StdLibDocFreshnessTest` gates both. Keep examples deterministic (no `uuid`, `current_date`), on inline values, and without backslashes (the stdlib is embedded as a Scala string literal).

## Snowflake variants

CI has no Snowflake account ([ADR](../adr/2026-11-25-snowflake-local-verification.md)), so Snowflake SQL is checked in two steps:

- In CI, `SnowflakeSqlGoldenTest` pins the Snowflake SQL generated for `spec/basic/stdlib_*.wv` in `spec/snowflake-sql/`, so every change to it shows in the PR diff. A function with no `in snowflake` variant falls back to another engine's variant on Snowflake (`dialectCompatible`), and the golden SQL shows which one.
- Before adding or changing an `in snowflake` variant, run the stdlib specs on Snowflake with your own account and note the result in the PR: `SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_DATABASE=... SNOWFLAKE_PRIVATE_KEY_FILE=... ./sbt "runnerJVM/testOnly *SnowflakeStdLibSpec"`. Snowflake no longer accepts a password alone (MFA for people, no passwords for service users), so use a key pair (`SNOWFLAKE_PRIVATE_KEY_FILE`) or a programmatic access token (`SNOWFLAKE_TOKEN`); the variables are listed in `SnowflakeTestEnv`. Without them it is skipped. The specs query literal tables only.

## Dialect contexts

- `def ... in <context>` variants are selected by `DBType.dialectContextChain`, most specific first. Each engine has one context, except the wvlet engine: `type wvlet extends duckdb`, so `in wvlet` wins over `in duckdb`, which wins over a dialect-neutral def.
- The wvlet engine inherits the hand-written `in duckdb` variants, but not the generated DuckDB catalog (`standard/duckdb/functions.wv`). A catalog def is kept only when the engine binds its name (`WvletEngineFunctionNames`), which keeps its typed signature.
- On the wvlet target, `EngineFunctionValidator` rejects a call that resolves to no def for the dialect and is not an engine function, with `FUNCTION_NOT_FOUND`. When the name is what another engine's stdlib variant expands to (e.g. Trino's `approx_distinct`), the message names that stdlib function. Other targets pass unknown names through as SQL. Raw `sql"..."` is not checked.

## Engine support

Engine coverage of the stdlib is measured by `EngineStdlibCoverageCheck`; see [the engine page](engine.md#function-coverage-ratchets).
