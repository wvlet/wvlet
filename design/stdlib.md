# Standard library and function catalogs

This page owns the stdlib (`wvlet-stdlib/module/standard/*.wv`), the per-engine function catalogs generated from it, and the docs page that lists it. Function naming follows [the stdlib naming ADR](../adr/2026-11-23-stdlib-function-naming.md): the stdlib is Wvlet's engine-neutral function contract, so new functions get stdlib names, not engine-specific ones.

## Freshness gates

The stdlib is freshness-gated in CI. After changing `wvlet-stdlib/module/standard/*.wv`, or bumping an engine dependency, regenerate:

- The DuckDB catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibFunctionCatalogGenerator"`
- The Trino catalog: `./sbt "runnerJVM/Test/runMain wvlet.lang.runner.StdLibTrinoFunctionCatalogGenerator"`
- The docs reference page: `./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.StdLibDocGenerator"`

`StdLibCatalogFreshnessTest` and `StdLibDocFreshnessTest` fail on drift and print the exact command to run.

## Engine support

Engine coverage of the stdlib is measured by `EngineStdlibCoverageCheck`; see [the engine page](engine.md#function-coverage-ratchets).
