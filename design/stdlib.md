# Standard library and function catalogs

This page owns the stdlib (`wvlet-stdlib/module/standard/*.wv`), the per-engine function catalogs generated from it, and the docs page that lists it. Function naming follows [the stdlib naming ADR](../adr/2026-11-23-stdlib-function-naming.md): the stdlib is Wvlet's engine-neutral function contract, so new functions get stdlib names, not engine-specific ones.

## Freshness gates

The stdlib is freshness-gated in CI. After changing `wvlet-stdlib/module/standard/*.wv`, or bumping an engine dependency, regenerate:

- The DuckDB catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator"`
- The Trino catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibTrinoFunctionCatalogGenerator"`
- The docs reference page: `./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.StdLibDocGenerator"`
- The engine's function names (`WvletEngineFunctionNames`, after adding or removing an engine function): `./sbt "engineJVM/Test/runMain wvlet.lang.engine.EngineFunctionNamesGenerator"`

`StdLibCatalogFreshnessTest`, `StdLibDocFreshnessTest`, and `EngineFunctionNamesTest` fail on drift and print the exact command to run.

## Dialect contexts

- `def ... in <context>` variants are selected by `DBType.dialectContextChain`, most specific first. Each engine has one context, except the wvlet engine: `type wvlet extends duckdb`, so `in wvlet` wins over `in duckdb`, which wins over a dialect-neutral def.
- The wvlet engine inherits the hand-written `in duckdb` variants, but not the generated DuckDB catalog (`standard/duckdb/functions.wv`). A catalog def is kept only when the engine binds its name (`WvletEngineFunctionNames`), which keeps its typed signature.
- On the wvlet target, `EngineFunctionValidator` rejects a call that resolves to no def for the dialect and is not an engine function, with `FUNCTION_NOT_FOUND` and the stdlib name when one expands to it on DuckDB. Other targets pass unknown names through as SQL. Raw `sql"..."` is not checked.

## Engine support

Engine coverage of the stdlib is measured by `EngineStdlibCoverageCheck`; see [the engine page](engine.md#function-coverage-ratchets).
