# Wvlet Table Store — portable PlazmaDB successor (substrate v0)

Date: 2026-08-21
Design source: `/Users/leo/tdx/work/local/default/notes/2026-08-21-wvlet-table-store-portable-plazmadb-successor.md` (rev 2, design-reviewed — treat as pre-approved).
Worktree: `.worktree/feature-table-store`, branch `feature/table-store`.

## Problem statement

Wvlet lacks its own analytical table store. Today's workloads (LLM Proxy cost tracking, event logs, metering) share one shape: high-frequency structured append → rollups → interactive queries → retention. The design note specifies a portable catalog+format substrate: immutable data files on any object store, a transactional SQL catalog, leased background merge, and lazy schema escalation — re-implementing PlazmaDB's best ideas without its operational lock-in, going where DuckLake doesn't (lease/fencing, raw-ingest tier, hierarchical metadata).

## Scope of THIS PR (agreed with user)

- **Inside wvlet**: new JVM-only sbt module `wvlet-table-store` (id `tableStore`). Package boundaries mirror the four Uni modules so extraction into `org.wvlet.uni` artifacts later is mechanical.
- **Core substrate + protocols**, no language/compiler changes yet (`CREATE TABLE … USING wvlet` wiring is a fast-follow PR).
- Deferred (open items in the note): fileset manifests (`kind='fileset'`), predicate rewrite (GDPR erasure) runner, retention runner scheduling, DuckLake projection views, catalog export/import tool, S3/GCS/Azure drivers (interface + FS driver only in v0), struct/array widening rules.

## Architecture (from the note)

Three cleanly separated layers:

1. **Catalog** — transactional SQL DBMS holding snapshots + file_entries (interval-encoded liveness), schema_versions, leases, snapshot_pins, retention policies. Drivers: SQLite (dev/tests), DuckDB (embedded), Postgres (service). Portable contract: TEXT-JSON columns, no arrays/JSONB, UTC-micro timestamps, monotonic per-table sequence emulation.
2. **Data** — immutable files: `raw/…/<file_id>.jsonl` (pre-issued id + SHA-256 checksum) and `merged/…/<sha256>.parquet` (content-addressed). v0 driver: local filesystem; `ObjectStore` trait leaves S3 open.
3. **Compute** — engine-agnostic readers; v0 uses DuckDB JDBC for Parquet writing during merge.

## Package layout (`wvlet-table-store/src/main/scala/wvlet/lang/tablestore/`)

- `objectstore/ObjectStore.scala` — checksummed put, streaming get, inventory list; `LocalObjectStore`.
- `catalog/model.scala` — row types (snapshots, file_entries, file_column_stats, leases, snapshot_pins, schema_versions…).
- `catalog/CatalogStore.scala` — driver trait: DDL bootstrap, sequence allocation, ingest registration txn, lease acquire/renew/release, retiring-commit txn primitives (conditional retirement + token check), live-entry resolution at pinned snapshot, pruning query joining `file_column_stats`.
- `catalog/JdbcCatalogStore.scala` — shared JDBC base; `SQLiteCatalogStore`, `DuckDBCatalogStore`, `PostgresCatalogStore`.
- `format/DataFile.scala` — JSONL reader/writer; Parquet writer via DuckDB COPY; content-addressing.
- `schema/PromotionLattice.scala` — monotonic lattice `null → long → double → string`; order-independent fold of observed schemas; outlier guardrail (quarantine widenings forced by < 0.1% of rows).
- `TableStore.scala` — protocol orchestration: `IngestSession` (writer session: pre-issued file-id range, rotating segments, register-on-rotate), `TableReader` (pin snapshot, resolve live entries + cast plan, prune by predicate), `Merger` (leased: select → read once → write Parquet + infer schema → fenced commit), orphan detection helper.

## Protocol invariants to encode (from rev 2 of the note)

- Registered ⇒ immediately readable; there is NO separate live/streaming tier.
- Visibility: file visible at snapshot S iff `begin_snapshot <= S AND (end_snapshot IS NULL OR end_snapshot > S)`. Snapshot publication is O(files touched).
- Snapshot ids come from a catalog-side per-table counter incremented inside the registration transaction (sequence emulation, portable).
- Merge commit txn MUST re-read the lease row under lock and abort unless `fencing_token` equals the merger's token ("stamping is not checking").
- Retirement is conditional: `UPDATE … SET end_snapshot = :new WHERE id IN (…) AND end_snapshot IS NULL` + assert affected-row count == expected. No entry retires twice; no two mergers fold the same rows.
- Only the lease holder retires entries / publishes schema versions. Ingest only adds. No optimistic retry paths.
- Escalation runs only under the merge lease inside the commit txn; lattice fold must be order-independent; outlier guardrail quarantines instead of escalating.
- Stats are advisory: missing stats ⇒ must-scan. min/max canonical-encoded, tagged with schema_version.
- Readers pin snapshots via expiring `snapshot_pins` rows; GC never retires files visible at a pinned snapshot.
- Embedded profile = single-process serialized writes (honest limitation; correctness confidence comes from fault-injection tests).

## Build integration

- `build.sbt`: new `lazy val tableStore = project.in(file("wvlet-table-store"))` JVM project; depends on `api.jvm`; libs: sqlite-jdbc (already in repo), duckdb_jdbc (present), postgresql (add); aggregated into `jvmProjects`; publishable (not noPublish) so future modules can consume it.
- Tests: uni-test (`UniTest`) under `wvlet-table-store/src/test/scala`.

## Test plan (deterministic, no sleeps where avoidable)

1. Lattice: order-independence (fold permutations), quarantine guardrail thresholds.
2. Interval visibility: begin/end semantics incl. AS OF reads before/during/after merge.
3. Ingest: pre-issued ids, crash idempotency (duplicate registration is a no-op), checksum mismatch rejection, immediate readability after registration.
4. Fencing: zombie merger holding an expired token cannot commit (token asserted in-txn); fresh lease wins; retired-entry double-fold attempt changes 0 rows and fails the assertion.
5. Pruning: predicate over stats eliminates files; missing stats ⇒ included.
6. Pins: expired pin does not block retirement; active pinned snapshot blocks it.
7. Cross-driver conformance: same suite against SQLite and DuckDB catalogs (+ Postgres if available locally; otherwise skipped-by-env).
8. End-to-end: append N JSONL batches → query (reader) → merge → same results, fewer live entries; schema escalated lazily; new columns appear only post-merge.

## Open questions carried forward (from the note)

Exact struct/array widening rules, predicate-rewrite spec, fileset manifest schema, DuckLake superset-schema decision, export/import tool format, quantified SLO targets.

## Definition of done

- `./sbt tableStore/test` green (plus `scalafmtAll`, `compile`).
- All protocol invariants above covered by named failing-if-broken tests.
- Module documented (package README or scaladoc) including honest embedded-profile caveats.

## Outcome (2026-08-21 — shipped as wvlet#1959)

All of the above landed; 31/31 tests green locally and in CI (Scala 3 / format / JS / Native jobs).

Deviations and learnings from the original plan:

- **Isolation levels not load-bearing.** Retiring transactions enforce fencing and no-double-retire
  with predicate-based conditional updates asserted by row count (`UPDATE … WHERE fencing_token = ?
  AND expires_at > now`, `UPDATE … SET end_snapshot = ? WHERE … AND end_snapshot IS NULL`), so the
  same statements are correct on SQLite, DuckDB, and Postgres regardless of isolation level.
- **Sequence emulation detail.** `counters(scope, next_value)` upsert allocates ranges inside the
  caller's transaction; fresh rows insert `amount + 1` so ids start at 1 (0 would blur "no snapshot
  yet" sentinels such as `schema_version_head = 0`).
- **Parquet via DuckDB COPY** with explicit `columns={…}` on `read_json` and aliased CAST
  projections — DuckDB names projection columns by expression text unless aliased, which initially
  produced columns literally named `CAST(user_id AS BIGINT)`.
- **JSONL is compact JSON per line**: uni's `JSON.format` pretty-prints, so encoders use `.toJSON`.
- **Escalation guardrail semantics settled**: quarantine decisions are per column but applied at
  file granularity (pass 1 flags outliers, pass 2 refolds over survivors); a brand-new column seen
  by < threshold of batch rows quarantines its introducing files; escalation never narrows an
  already-published type.
- **Embedded concurrency**: one JDBC connection + monitor lock, matching the single-process
  single-writer contract; correctness confidence comes from the deterministic fault-injection suite
  run against both embedded backends.
- Follow-ups unchanged: `CREATE TABLE … USING wvlet` wiring, filesets, retention/erasure runner,
  DuckLake projection, export/import tool, S3 drivers.

## ADRs

- `adr/2026-08-21-table-store-catalog-portability.md` — why conditional-update assertions instead
  of isolation levels / FOR UPDATE, and the counter-based sequence emulation.

