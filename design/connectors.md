# Connectors

This page owns how query execution reaches a database. The module split is recorded in [the optional connector modules ADR](../adr/2026-11-23-optional-connector-modules.md); the `wvlet` connector type is in [the engine connector ADR](../adr/2026-11-21-wvlet-engine-connector.md).

## Modules

- `wvlet-connector` holds the `Connector` interface, the ServiceLoader factory registry, and the Trino and Slack connectors.
- `wvlet-connector-duckdb` and `wvlet-connector-snowflake` are optional modules that own their JDBC drivers.
- `type: wvlet` without a host runs the in-process engine (`wvlet-engine`); with a host it is a remote Wvlet server. It is the default for `wvlet run` and `wv`; see [the engine default ADR](../adr/2026-11-23-wvlet-engine-default.md).

## Rules

- A new connector module registers a `ConnectorFactoryProvider` class in `META-INF/services/wvlet.lang.connector.ConnectorFactoryProvider`.
- Main code outside a connector module must not reference its classes.
- Tests that need DuckDB as an oracle depend on `wvlet-connector-duckdb` (or `duckdb_jdbc`) in Test scope.
- Trino connector tests need JDK 25+.
