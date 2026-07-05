# Phase 2 of #1861 — Connector namespaces, `use` statement, staging

Builds on Phase 1 (#1863, merged): multi-connector `Profile`, `Connector` capability trait, `ConnectorProvider`.

Split into two PRs:

- **PR-A (this branch): connector namespaces in the compiler + extended `use`.** Queries can reference `from <connector>.<table>` and switch the active engine with `use <connector>`. Cross-connector references inside one query are type-checked but rejected at execution with a clear "staging lands in a follow-up" error unless the referenced connector is the active engine.
- **PR-B: runtime staging + per-stage `on <connector>` in flows.** Materialize connector B's tables into engine A (JSONL → CTAS, reusing the QueryExecutor.executeSaveToLocalFileViaDuckDB pattern), per-stage engine selection in FlowExecutor.

## Exploration facts (file:line verified)

- `from a.b.c` parses to `TableRef(QualifiedName)` (WvletParser.scala:2431-2442); resolution in `RelationRefResolver.resolveTableRef` (RelationRefResolver.scala:60): symbol `lookup` returns None for any DotRef (":54 TODO"), then catalog fallback `context.catalog.findTable(schema, table)` (:108-112) via `TableName.parse` (2-part = schema.table, 3-part = catalog.schema.table, catalog part dropped at this call site). Unresolved refs pass through.
- `Context.catalog` = single `global.defaultCatalog` var (Context.scala:78, ":178 TODO support multiple catalogs"); **`Context.dbType = catalog.dbType`** (:180) — the SQL dialect follows the active catalog, so switching the active connector's catalog switches dialect for free.
- `use` parses to `UseSchema` (WvletParser.scala:924, optional soft keyword "schema"); no compiler-phase handling; runtime handler QueryExecutor.scala:265-284 mutates `context.global.defaultSchema` (2-part catalog ignored, 3-part rejected).
- CLI registers only the default engine's catalog: WvletCompiler.scala:102 `setDefaultCatalog(getDBConnector.getCatalog(...))`; same in WvletScriptRunner.scala:87-96.
- `ConnectorCatalog` construction triggers an async `init()` that queries the DB → registering all profile connectors eagerly would open connections to every engine at startup. Needs a lazy wrapper.
- Flow seams for PR-B: `FlowExecutor` single `connector` field (:236); `materializeStage` (:1084, engine-aware DDL :1114); `rewriteStageRefs` (:1144) is where staging-table rewrites inject; `StageDef` (flow.scala:132) gains `engine: Option[NameExpr]`; parser slot in `stageDef()` between config block (:473) and `=` (:475); `FlowLowering.LoweredStage` waitUntil-extraction (FlowLowering.scala:155-234) is the pattern for the new per-stage directive; `ConnectorProvider` needs `getConnector(profile, name)`.

## PR-A design

1. **Registry**: `GlobalContext` gets `connectorCatalogs: mutable.Map[String, Catalog]` + `Compiler.addConnectorCatalog(name, catalog)`. New `LazyCatalog(name, () => Catalog)` in wvlet-lang catalog package delegating all `Catalog` methods to a lazily-built target (so non-default engines connect only on first reference).
2. **CLI/runner wiring**: WvletCompiler.createCompiler + WvletScriptRunner register every profile connector that has `catalog`+`schema` config: default engine eagerly (as today), others as `LazyCatalog` via `ConnectorProvider.getConnector(config).getCatalog(...)`.
3. **Resolution**: in `RelationRefResolver.resolveTableRef`, after symbol lookup fails and the name is a DotRef whose leading identifier matches a registered connector name (connector names shadow schema names; CTEs/aliases/models still win via the earlier symbol path): resolve the remaining 1-2 parts against that connector's catalog (2-part `td.orders` → td's default schema; 3-part `td.s.t` → schema s). 4-part deferred to PR-B. Record the connector name in `CompilationUnit` (e.g. `referencedConnectors`) so the executor can enforce single-engine execution; the emitted `TableScan.name` is the fully-qualified `catalog.schema.table` on that engine.
4. **`use` extension**: parser accepts `use connector <name>` (soft keyword, like `schema`); bare `use <name>` resolution at runtime checks the active profile's connector names first, then falls back to today's schema behavior. Switching connector = set `global.defaultCatalog` to that connector's catalog (dialect follows), `defaultSchema` to its config schema, and the executor's active engine config (new `private var activeEngine: ConnectorConfig` in QueryExecutor; `getDBConnector` uses it). `use prod.td` across profiles: not supported (profile stays a CLI boundary).
5. **Execution guard**: QueryExecutor errors with NOT_IMPLEMENTED "cross-connector query staging arrives in a follow-up; run `use <name>` to execute on that connector" when a compiled unit references a connector other than the active engine.
6. **Specs/tests**: ProfileTest-style unit tests for LazyCatalog; RelationRefResolver test with a registered fake connector catalog; runner spec: multi-connector profile (duckdb + second duckdb alias), `use <name>` switch, `from <name>.tbl` resolution; `use connector` parse test in spec/basic; TyperCoverageCheck must not regress.
7. **Docs**: website/docs — connector namespace + `use` section in the profiles doc.

## PR-B design (follow-up branch)

1. `StageDef.engine: Option[NameExpr]`; parse `on <name>` in stageDef() (lookahead to disambiguate from `depends on`); thread per-stage connector through FlowExecutor (resolve via `ConnectorProvider` + `Profile.connectorByName`); engine-aware DDL already per-connector.
2. `CatalogProvider.scan` + JSONL→CTAS staging: materialize non-local `TableRef`s in `rewriteStageRefs`/`materializeStage` into run-scoped `__wv_flow_*_stg_*` tables on the stage's engine.
3. Lift the PR-A execution guard for the staged paths.
4. Trino flow test with a DuckDB-sourced staged input; docs update (flow.md `on <connector>`).

## Verification (each PR)

- `./sbt "projectJVM/Test/compile" "projectJS/Test/compile" "projectNative/Test/compile"`
- `./sbt "langJVM/test"` (TyperCoverageCheck ratchet), `./sbt runner/test`, `./sbt cli/test`
- Smoke: packInstall; multi-connector profiles.json with two duckdb connectors; `wv -c 'use second; from second.t'`
- `./sbt scalafmtAll`
