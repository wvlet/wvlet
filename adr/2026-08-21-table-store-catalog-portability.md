# ADR: Table store catalog portability — assertions over isolation levels

Date: 2026-08-21
PR: wvlet#1959 (`feature/table-store`)

## Context

The table store design note requires the catalog contract to be implementable on Postgres,
DuckDB, and SQLite with plain SQL (TEXT-JSON only, no arrays/JSONB), and requires that a
zombie merger can never publish stale state and no file entry ever retires twice. The obvious
tools for these guarantees — `SELECT … FOR UPDATE` plus `SERIALIZABLE` transactions — are not
uniformly available or uniformly meaningful across the three drivers (DuckDB's MVCC conflicts,
SQLite's database-level locking, Postgres's row locks), and leaning on isolation levels would
make correctness depend on per-backend tuning that tests cannot exercise deterministically.

## Decision

1. **Predicate-based conditional updates with row-count assertions** carry the protocol
   invariants instead of lock syntax:
   - Lease fencing inside the commit transaction:
     `UPDATE leases SET expires_at = ? WHERE name = ? AND fencing_token = ? AND expires_at > now`
     — commit aborts unless exactly 1 row changes. Tokens come from a monotonic counter, so a
     zombie's stale token matches zero rows.
   - Entry retirement:
     `UPDATE file_entries SET end_snapshot = ? WHERE table_id = ? AND id IN (…) AND end_snapshot IS NULL`
     — abort via `RetireConflictException` when the affected-row count differs from the source
     count, so two mergers can never double-fold rows.
2. **Monotonic sequences are counter-table upserts** executed inside the caller's transaction
   (`INSERT … ON CONFLICT(scope) DO UPDATE SET next_value = next_value + amount`), returning
   `[next - amount, next)`. Fresh counters insert `amount + 1` so allocated ids start at 1;
   0 stays reserved as the "nothing published" sentinel (e.g. `schema_version_head = 0`).
3. **Embedded drivers serialize through one JDBC connection + monitor**, matching the design's
   single-process single-writer tradeoff; confidence comes from deterministic fault-injection
   tests (stale-token commit, double-retire abort, crash-idempotent re-registration) run against
   both SQLite and DuckDB conformance suites rather than from timing-based concurrency tests.

Worked examples: `JdbcCatalogStore.commitMerge` / `acquireLease` /
`registerIngest` (wvlet#1959, commit 154901d6); fault-injection expectations in
`CatalogProtocolTest.scala`.

## Consequences

- Every statement is byte-identical across drivers; adding a backend is a URL + driver class.
- Correctness does not regress if a deployment runs a weaker isolation level; the conditional
  updates remain the last line of defense.
- Cost: wasted fencing tokens when acquisition loses a held lease (harmless — tokens merely
  increase); retirement of large batches binds one parameter per id (fine at v0 batch sizes; a
  temp-table join is the future optimization if needed).
- The Postgres production driver still wants connection pooling; every statement already works
  unchanged across pooled connections because each transaction is self-contained.
