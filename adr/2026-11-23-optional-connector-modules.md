# ADR: DuckDB and Snowflake are optional connector modules, shipped in a full CLI

Date: 2026-11-23

## Context

The JVM CLI pack was 228 MB in 45 jars. The two largest jars are JDBC drivers that most local use never touches: `snowflake-jdbc` (94 MB) and `duckdb_jdbc` (78 MB, with native libraries for every platform). Since [the engine became the default](2026-11-23-wvlet-engine-default.md), `wvlet run`, `wv`, file exports, and source staging run on the in-process wvlet engine. DuckDB stays available as an opt-in engine (`-t duckdb`, `type: duckdb`), and tests use it as the conformance oracle.

DuckDB reached the CLI classpath in two ways: `wvlet-connector` bundled the DuckDB connector, and `wvlet-lang` (JVM) depended on `duckdb_jdbc` to infer the schemas of Parquet and CSV files at compile time. `ConnectorProvider` built its registry from a list that imported every factory.

## Decision

- `wvlet-connector-duckdb` holds the DuckDB connector and the `generic` fallback, which runs on DuckDB. `wvlet-connector-snowflake` holds the Snowflake connector. Each new module owns its JDBC driver. `wvlet-connector` keeps the connector interfaces, Trino (over REST), and Slack. Package names are unchanged.
- Connector modules register their factories through `ServiceLoader`. Each module lists a `ConnectorFactoryProvider` class in `META-INF/services`, because the factories are Scala objects and ServiceLoader cannot instantiate those. `ConnectorProvider` names no connector class, and an installed module joins the registry just by being on the classpath. Tests can still pass an explicit factory list.
- An optional connector type (`duckdb`, `generic`, `snowflake`) whose module is missing fails with `CONNECTOR_NOT_INSTALLED`. The message names the full CLI and the module jars to copy into the CLI's `lib/` folder (the pack launcher loads `lib/*`). An unknown type (e.g. `hive`) still falls back to the generic DuckDB connector when that module is installed. Without it, the unknown type fails and the message lists the available connectors.
- `wvlet-lang` no longer depends on `duckdb_jdbc`. On the JVM, `DuckDB.isAvailable` reports whether the DuckDB driver class loads, as Native reports whether libduckdb loads. Without the driver, `from 'file.parquet'` stays unresolved at compile time, and the engine reads the file's schema when it runs.
- Behaviour that does not need DuckDB is routed so that it works without it:
  - `wvlet compile` keeps the DuckDB SQL dialect but skips the catalog when the connector is missing.
  - `wvlet ui` defaults to the wvlet engine, like `run` and `wv`.
  - `wvlet catalog` asks the connector for its default catalog (`defaultCatalogName`) and its function and type dialect (`functionDialect`), instead of checking for DuckDB and Generic by name.
- Distribution: a lite CLI by default, plus a full CLI with the optional connectors, attached to the same release. Versions always match, and `brew upgrade` keeps the full CLI whole. Until the full pack exists, the CLI keeps depending on both modules, so its contents do not change.

## Consequences

- Without DuckDB and Snowflake, the pack is about 56 MB, down from 228 MB.
- `wvlet catalog diff` without `--profile` still defaults to DuckDB, because the wvlet engine is not a catalog-backed SQL connector. On the lite CLI it reports how to install DuckDB.
- Flow stages that read another connector's table stage into the wvlet engine or DuckDB (#2337), so the lite CLI runs them on the engine. Choosing a DuckDB engine there fails at connector creation with the install message.
- Downstream users of the `wvlet-lang` and `wvlet-connector` artifacts who relied on the transitive `duckdb_jdbc` or `snowflake-jdbc` must add `wvlet-connector-duckdb` or `wvlet-connector-snowflake`.

## Alternatives

- Ship a connector add-on tarball and unpack it into `lib/`. Users would match versions by hand, and `brew upgrade` replaces `lib/`.
- Download connectors on first use (`wvlet connector install duckdb`). This adds a Maven resolver, a cache, and network access to the CLI. It is worth revisiting only if more optional connectors appear.
- Move only the connector and keep `duckdb_jdbc` in `wvlet-lang`. The 78 MB driver would still reach the CLI transitively.
