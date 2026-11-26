# Connectors

This page owns how query execution reaches a database. The module split is recorded in [the optional connector modules ADR](../adr/2026-11-23-optional-connector-modules.md); the `wvlet` connector type is in [the engine connector ADR](../adr/2026-11-21-wvlet-engine-connector.md).

## Modules

- `wvlet-connector` holds the `Connector` interface, the ServiceLoader factory registry, and the Trino and Slack connectors.
- `wvlet-connector-duckdb` and `wvlet-connector-snowflake` are optional modules that own their JDBC drivers.
- The JVM CLI ships as two packs: `cli` (`wvlet-cli-<version>.tar.gz`, the default and the Homebrew formula) without the optional modules, and `cliFull` (`wvlet-cli-full-<version>.tar.gz`) with them. Both are attached to every release.
- `type: wvlet` without a host runs the in-process engine (`wvlet-engine`); with a host it is a remote Wvlet server. It is the default for `wvlet run` and `wv`; see [the engine default ADR](../adr/2026-11-23-wvlet-engine-default.md).

## Rules

- A new connector module registers a `ConnectorFactoryProvider` class in `META-INF/services/wvlet.lang.connector.ConnectorFactoryProvider`.
- Main code outside a connector module must not reference its classes.
- `cli` depends on optional connector modules only in Test scope; bundle a new optional module in `cliFull`, not `cli`.
- Tests that need DuckDB as an oracle depend on `wvlet-connector-duckdb` (or `duckdb_jdbc`) in Test scope.
- Trino connector tests need JDK 25+.
- The `wvlet` connector's `prepareTPCH` / `tpchScaleFactor` properties match the DuckDB connector's names and default (0.01), so a profile runs on either engine. Its TPC-H tables are generated while scanned (`TpchSource`), read-only, and not `dbgen`-identical: tests that compare answers with DuckDB keep reading `dbgen` Parquet. `prepareTPCDS` is DuckDB-only and an error on the engine.
