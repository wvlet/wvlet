# Phase 1 of #1861 — Multi-connector profiles: module extraction + Profile schema

## Context

Issue: https://github.com/wvlet/wvlet/issues/1861 (phases 1–3 in scope for this effort; MCP phase 4 out of scope).

Today `Profile` ≡ one connection: `Profile(name, type, host, port, user, password, catalog, schema, useHttps, properties)` in `wvlet-lang/src/main/scala/wvlet/lang/catalog/Profile.scala`, and `DBConnectorProvider` (wvlet-runner) dispatches on `profile.dbType` with a `ConcurrentHashMap[Profile, DBConnector]` cache. This cannot express an environment with several simultaneous connections (Trino + DuckDB + future Slack/Confluence sources). Phase 1 is the mechanical foundation: profile = environment holding a list of named connectors, connector implementations extracted into a `wvlet-connector/*` module family with a capability-based trait + explicit factory registry. Breaking change targeted at 2026.2.0 — no back-compat shims except an error-path config converter.

Exploration established:
- Trino connector is HTTP-based (uses cross-platform `TrinoSqlConnector` in wvlet-lang); `io.trino:*` deps in runner are Test-only (embedded server). Only DuckDB JDBC (build.sbt:403) and Snowflake JDBC (build.sbt:409) move out of runner.
- `wvlet-lang` itself keeps its own `duckdb_jdbc` dep (build.sbt:190, parquet schema resolution) — untouched.
- `DBConnector.scala` is engine-agnostic (java.sql only); `GenericConnector` delegates to DuckDB; `ConnectorCatalog` (caffeine Catalog impl) depends only on the DBConnector trait.
- Profile consumers: DBConnectorProvider, QueryExecutor:54/68, WvletScriptRunner:38/89, WvletServer:171-186 (DI `bindInstance[Profile]`), WvletCompiler:51-102, WvletFlowCommand:108/203/387/443, WvletREPLMain:67-112, cliCore WvletCli:47-96 (cross-platform; sole reader of `port`/`useHttps`).
- Tests: ProfileTest (12 tests, flat JSON fixtures), RunnerSpec:36, TrinoSpec:34, TrinoFlowExecutorTest:29, FlowExecutorTest:34, FlowRunRetentionTest:13, ExecutionPlanTest:29, DuckDBConnectorTest:24, wvlet-spec TDTrinoSpec:32.
- Docs with flat profiles.json samples: website/docs/usage/trino.md, snowflake.md; cli-reference.md:135-154 documents stale YAML config (fix while here); syntax/flow.md:542.

## Design decisions (settled in issue discussion)

1. **Profile = environment**: `Profile(name, connectors: Seq[ConnectorConfig])`; `ConnectorConfig` carries name, `type`, `default` flag + the current per-connection fields. `profile.defaultEngine` = connector marked `default: true`, else first engine-capable connector.
2. **Module**: a single `wvlet-connector` module (`connector`), JVM-only, packages kept per-engine (`wvlet.lang.connector{,.duckdb,.trino,.snowflake}`) so future service connectors with heavy deps can split out mechanically. [Revised from the api+3 submodule plan on user request: duckdb_jdbc already reaches everything via wvlet-lang, Trino has no JDBC dep, so a split only isolates snowflake-jdbc — not worth 4 artifacts yet. Bonus: ConnectorProvider.defaultFactories can include ALL engines, eliminating the DI-forgot-to-register risk.]
3. **Capability trait** (api module): `Connector { name, connectorType, catalog: Option[CatalogProvider], engine: Option[SqlEngine], tools: Seq[ToolSpec], invoke(...) }`. Phase 1 keeps CatalogProvider/ToolSpec minimal; compiler wiring is Phase 2.
4. **Explicit factory registry** — no ServiceLoader. `ConnectorFactory { connectorType; create(config, workEnv) }`; `ConnectorProvider` (replaces DBConnectorProvider) takes the factory list; runner defaults to DuckDB+Generic, cli/server add Trino+Snowflake.
5. **Old flat profiles.json fails with an error printing the exact converted JSON** (converter only in the error path).
6. `--catalog`/`--schema` CLI overrides apply to the default engine's ConnectorConfig.

## Additional design decisions (validated against code)

- **D1: `DBConnector` extends `Connector with SqlEngine` directly** (no adapter): `final override def engine = Some(this)`. Every consumer holds a `DBConnector` and calls its JDBC-shaped surface; a wrapper would force `.engine.get` at ~20 sites for zero Phase-1 benefit. `SqlEngine` is a thin supertrait (`dbType`, `execute`, `executeUpdate`, `getCatalog`, `listFunctions`) whose signatures already match DBConnector's.
- **D2: `ConnectorCatalog` moves to connector-api too** — `DBConnector.getCatalog` (DBConnector.scala:87-95, verified) constructs it. Costs a caffeine dep on api. Drop its only-ever-defaulted `ThreadManager` param; use `ThreadUtil.runBackgroundTask`.
- **D3: supporting moves**: `codec/JDBCCodec.scala` → api (`wvlet.lang.connector.codec`); `object ThreadUtil` → api; `class ThreadManager` stays in runner (keeps arrow import out of api).
- **D4: decouple QueryExecutor from TrinoConnector**: QueryExecutor matches `case trino: TrinoConnector => …asSqlConnector…` at ~397 and ~531 (verified). Add `def sqlConnector: Option[SqlConnector] = None` to `DBConnector` (cross-platform `SqlConnector` lives in wvlet-lang, so api can reference it); `TrinoConnector` overrides with `Some(asSqlConnector)`; QueryExecutor matches on the capability. Keep `asSqlConnector` too.
- **D5: cache key = `ConnectorConfig` value equality** (`ConcurrentHashMap[ConnectorConfig, Connector]`). `(profileName, connectorName)` is unsound: `defaultDuckDBProfile` and `defaultGenericProfile` are both named "local". Whole-Profile key is over-broad.
- **D6: visibility**: `private[runner]` members (`newSession`, `withSession`, `executeCancellable`, `queryJsonRows` — DBConnector.scala:109-141) become `private[lang]`; `private[connector] def newConnection` keeps working from `wvlet.lang.connector.*` subpackages.
- **D7: connector `name`**: `private var` on DBConnector defaulting to dbType string + `def withName(n: String): this.type` called by factories with `config.name` — avoids churning ~10 direct-construction test sites. Phase 2 may promote to ctor param.
- **D8: legacy-shape detection runs BEFORE env-var expansion** so the converted JSON printed in the error preserves `${TD_API_KEY}` placeholders (no secret leak to terminal).

## Implementation steps

### 1. Profile schema (wvlet-lang) — standalone compilable
Rewrite the case-class layer of `wvlet-lang/src/main/scala/wvlet/lang/catalog/Profile.scala` (JSONC reading, env interpolation, YAML rejection unchanged):
- `ConnectorConfig(name, type, default: Boolean = false, user, password, host, port, catalog, schema, useHttps, properties)` + `dbType`, `withProperty`.
- `Profile(name, connectors: Seq[ConnectorConfig])` with `defaultEngine` (explicit `default:true`, else first; throws INVALID_ARGUMENT if empty), `withEngineOverrides(catalog, schema)` (replaces the old `.copy` in `getProfile`), `withProperty` (delegates to default engine — keeps RunnerSpec compiling).
- `defaultDuckDBProfile`/`defaultGenericProfile`/`defaultProfileFor` become single-connector profiles (connector named after its type).
- `failOnLegacyProfiles(parsed, configPath)` called between `JSON.parse` and env expansion: a profile object with `type` and no `connectors` → INVALID_ARGUMENT whose message embeds the exact converted JSON (`convertLegacyProfile`: wrap all fields except `name` into a single connector named after `type`). Verify uni JSON API accessor names.
- Weaver risk: if `default: Boolean = false` doesn't decode when omitted, fall back to `default: Option[Boolean]` + `.contains(true)`.

### 2. build.sbt
- `val CAFFEINE_VERSION = "3.2.4"` next to other version vals.
- New projects after `testUtil` (~line 388): `connectorApi` (`wvlet-connector/api`, deps: caffeine, dependsOn `lang.jvm`), `connectorDuckDB` (duckdb_jdbc), `connectorTrino` (no extra deps — HTTP client comes from lang), `connectorSnowflake` (snowflake-jdbc); each dependsOn `connectorApi`. Match file style.
- `jvmProjects` (lines 62-73): add all four (picked up by `projectJVM`/publishing).
- `runner`: remove caffeine/duckdb_jdbc/snowflake-jdbc deps (keep jline, arrow-vector, sqlite-jdbc, `io.trino:* % Test`); `.dependsOn(lang.jvm, connectorDuckDB, connectorTrino % Test, connectorSnowflake % Test, testUtil % Test)` — runner tests (TrinoSpec, TrinoFlowExecutorTest, SnowflakeConnectorTest, …) construct those connectors and can't move (they exercise FlowExecutor/QueryExecutor).
- `server` (line 516): add `connectorTrino, connectorSnowflake` (cli reaches them via server).
- `spec` (line 434): add `connectorTrino % Test` (TDTrinoSpec; runner's Test deps don't propagate).
- wvlet-lang's own duckdb_jdbc (line 190, parquet schema) stays.

### 3. connector-api contents (`wvlet-connector/api/src/main/scala/wvlet/lang/connector/`)
- `Connector.scala`: trait Connector (name, connectorType, `catalog: Option[CatalogProvider] = None`, `engine: Option[SqlEngine] = None`, `tools: Seq[ToolSpec] = Nil`, `invoke` default NOT_IMPLEMENTED); minimal `CatalogProvider` (listTables, schemaOf — scan lands in Phase 3); MCP-shaped `ToolSpec(name, description, inputSchema)`, `ToolResult`.
- `SqlEngine.scala`: dbType, execute, executeUpdate, getCatalog, listFunctions (default `using QueryProgressMonitor` args declared here so call sites keep compiling).
- `ConnectorFactory.scala`: `connectorType: String`, `create(config: ConnectorConfig, workEnv: WorkEnv): Connector`.
- `DBConnector.scala` moved from runner, package `wvlet.lang.connector`, with D1/D4/D6/D7 modifications. `DBConnection`, `CancellableStatement`, `QueryScope` move along.
- `ConnectorCatalog.scala` moved (D2), `ThreadUtil.scala` (object only, D3), `codec/JDBCCodec.scala` moved verbatim.

### 4. Engine modules
- `wvlet-connector/duckdb/…/connector/duckdb/`: `DuckDBConnector.scala` + `GenericConnector.scala` (delegates to DuckDB — hence this module) moved; `DuckDBConnectorFactory` (prepareTPCH/TPCDS from config.properties) + `GenericConnectorFactory`; `.withName(config.name)`.
- `wvlet-connector/trino/…/connector/trino/`: `TrinoConnector.scala` moved; `override def sqlConnector = Some(asSqlConnector)`; `TrinoConnectorFactory` hosts the `parseBoolean`/useSSL logic lifted from DBConnectorProvider.scala:30-51 and TrinoConfig construction.
- `wvlet-connector/snowflake/…/connector/snowflake/`: `SnowflakeConnector.scala` moved; `SnowflakeConnectorFactory` with required-field errors from DBConnectorProvider.scala:63-88.

### 5. Runner rewiring
- Delete moved files + `DBConnectorProvider.scala`; `ThreadUtil.scala` → `ThreadManager.scala` (class only).
- New `connector/ConnectorProvider.scala` (package `wvlet.lang.runner.connector`): `ConnectorProvider(workEnv, factories = ConnectorProvider.defaultFactories)`; `getConnector(config: ConnectorConfig): Connector` cached by config; unknown type → warn + generic fallback (preserves `-t hive` compile-only flows), no generic factory → INVALID_ARGUMENT listing registered types; `getConnector(profile): DBConnector` resolves `profile.defaultEngine` and errors "not a SQL engine" otherwise; `getConnector(dbType, profileName)` kept. `defaultFactories = Seq(DuckDBConnectorFactory, GenericConnectorFactory)`. Drop the unused `properties` param of the old getConnector.
- `QueryExecutor.scala`: provider type swap; replace both TrinoConnector matches with `connector.sqlConnector match`.
- `FlowExecutor.scala`, `ActivationSink.scala`, `FlowRunRetention.scala`, `WvletScriptRunner.scala`: import updates only.

### 6. Server / CLI / cliCore
- `WvletServer.scala`: drop DuckDBConnector import; explicit `bindProvider[WorkEnv, ConnectorProvider]` with the FULL factory list (DI can't auto-wire the factories param — HIGHEST RISK if forgotten: Trino profiles would silently fall back to generic); `WvletScriptRunnerConfig` provider reads `profile.defaultEngine.catalog/schema`. `QueryService.scala`: import update.
- `wvlet-cli`: new `WvletConnectors.newProvider(workEnv)` helper (full factory list) used by `WvletMain`, `WvletFlowCommand` (4 sites), `WvletCompiler`; `WvletCompiler` reads `currentProfile.defaultEngine.{dbType,catalog,schema}`; `WvletREPLMain` same + explicit bindProvider in its design.
- `wvlet-cli-core/WvletCli.scala:47-96` (cross-platform): `val engine = profile.map(_.defaultEngine)`; swap field reads (sole reader of port/useHttps).
- `wvlet-spec/TDTrinoSpec.scala`: imports + `defaultEngine.*` + provider with Trino factory.

### 7. Tests
- Rewrite `ProfileTest.scala` fixtures to nested shape; new tests: legacy-shape error (message contains converted JSON AND preserves `${VAR}` placeholder — D8), defaultEngine selection (explicit flag beats first; first wins without flag), `default` omitted decodes false, getProfile catalog/schema overrides touch only default engine, multi-connector round-trip with properties.
- `TrinoSpec` / `TrinoFlowExecutorTest`: nested Profile fixture + provider with Trino factory. `FlowExecutorTest`/`FlowRunRetentionTest`: provider swap. Others: import path updates only (test packages can stay `wvlet.lang.runner.connector.*`).
- New `ConnectorProviderTest`: type dispatch, unknown-type→generic fallback, no-generic→error, cache hit on equal configs / miss on different configs, "local"-named duckdb vs generic profiles get distinct connectors (D5 regression), default:true beats first, withName.

### 8. Docs
- `website/docs/usage/trino.md`, `snowflake.md`: samples → `connectors` array shape.
- `cli-reference.md:133-154`: replace stale YAML config docs with profiles.json JSONC shape (env interpolation, `default: true`).
- `syntax/flow.md:542-546`: "profile's catalog/schema" → "default engine's".

## Risks
1. DI auto-wiring (server/REPL bindProvider with full factory list) — smoke-test `wvlet ui --quit-immediately`; assert factory set in a server test; confirm provider close() runs on shutdown.
2. Weaver decoding of `default: Boolean = false` in nested Seq — explicit test; Option fallback ready.
3. Cache sharing across equal configs in different profiles — per-test providers keep exposure minimal.
4. Legacy error must throw (not log) so server's eager `bindInstance[Profile]` fails loudly.
5. Four new published artifacts via projectJVM — confirm release automation uses the aggregate, no pinned module list.
6. `prepareTPCH` server option is currently unused — keep it unused (no behavior fix in this PR).

## Verification

- `./sbt "projectJVM/Test/compile"` and `./sbt "projectJS/Test/compile"` / `"projectNative/Test/compile"` (cliCore WvletCli.scala is cross-platform)
- `./sbt "langJVM/testOnly *ProfileTest*"` — new-schema fixtures + flat-shape error converter test
- `./sbt runner/test` (RunnerSpec suites on DuckDB), `./sbt cli/test`
- `./sbt "langJVM/testOnly *TyperCoverageCheck"` (CI ratchet, must not regress)
- `./sbt scalafmtAll` before commit
- Smoke: `cli/packInstall` then `wv -c 'from [1] as t(a) select a'` with a new-format profiles.json and with an old flat one (expect the converter error message)
