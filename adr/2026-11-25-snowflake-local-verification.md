# ADR: Snowflake variants are verified locally, with golden SQL in CI

Date: 2026-11-25 · PR #TBD

## Context

- The stdlib's `in trino` variants run on an embedded Trino server in CI (`TrinoStdLibSpec`), and the `in duckdb` variants on DuckDB. Snowflake has no local server, so the `in snowflake` variants have never been executed by a test.
- Running Snowflake in CI needs an account and credentials in the repository's secrets. The project has no Snowflake account of its own, and a company account is not appropriate for an open-source project.
- Emulators don't fill the gap: fakesnow translates Snowflake SQL to DuckDB, so it can't show whether Snowflake accepts the SQL, and LocalStack's emulator is a paid product.
- A function with no `in snowflake` variant does not fail on Snowflake: non-checking targets fall back to another engine's variant (`FunctionInliner.dialectCompatible`), e.g. Trino's `reduce(slice(...), ...)` for `xs.reduce(f)`. Nothing showed which Snowflake SQL each stdlib function produces.

## Decision

- **CI pins the SQL.** `SnowflakeSqlGoldenTest` compiles each `spec/basic/stdlib_*.wv` for Snowflake, one file at a time, and compares the SQL with `spec/snowflake-sql/<spec>.sql`. `SnowflakeSqlGolden` regenerates the files; a spec that does not compile for Snowflake records its error message instead. This catches unintended changes to Snowflake SQL and makes the fallbacks visible in review, but it does not prove the SQL runs.
- **Execution is checked locally.** `SnowflakeStdLibSpec` runs the same specs on Snowflake through the Snowflake connector, with credentials from `SNOWFLAKE_*` environment variables, and skips itself without them. It shares the compile-and-run loop with the Trino specs (`ConnectorSpecRunner`).
- **The gate.** An `in snowflake` variant is added or changed only after someone has run `SnowflakeStdLibSpec` with their own account (a personal or trial account works) and noted the result in the PR. Until then, a function keeps its current behavior on Snowflake.

## Consequences

- No credentials live in the repository, and CI stays free of external services.
- Snowflake regressions in the generated SQL show up in PR diffs; regressions in Snowflake's own behavior are found only when someone runs the local spec.
- The array lambda functions (`transform`, `filter`, `reduce`, `exists`, `forall`, `zip_with`) still have no Snowflake variants. They can be added through the gate above.
- The connector authenticates with a password only. Snowflake is phasing out single-factor password sign-ins for users, so key-pair or token authentication in the connector is a follow-up for running the local spec with a service user.
