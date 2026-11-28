# Release shared CTE results once their last reader finishes

## Goals

- Return a shared CTE result's memory (rows and reservation) once its last reader finishes, not when `execute` returns, so a query with several CTEs (TPC-DS q64, q23, q14) holds only the results still being read.
- Builds on the early-release PR (sleepy-tracing-noether, `feature/early-memory-release`, stacked on #2577). That PR adds `MemoryContext.child()` / `release()` / `peakBytes` and releases join builds, sorts, windows, and aggregations early. This PR covers only the remaining holder, shared CTE results.

## Background

- A `SharedResult` drains its operator once into `Seq[Batch]` through the query's context and keeps the rows until the operator tree is dropped.
- Readers are `CTETable` / `SharedTable` scans. A `MorselOp` reads one through `scanSplits` / `sliceSplits` and closes the splits after its last part; a plain `ScanOp` reads it through `scan(spec)`. Plan-time subqueries and stats (`rows`, `distinct`) read the result too, before the query runs.
- `reader()` calls also include readers built at bind time and later thrown away (memory note on finalizeCTEs), so readers are counted from the final operator tree, which `finalizeCTEs` already relies on to find every run-time reader.

## Design

- Lane: **Express**. It's an internal engine change with nothing user-facing and no always-gate area, and the launch prompt already authorizes auto-merge.
- Each `SharedResult` reserves through its own `memory.child()` and releases it with its batches.
- At the end of planning (`planRoot`, on the final root after the CTE re-planning passes), walk the operator tree and arm each shared result with its reader scans, identified by their `ScanSpec`. A reader is done when its splits close or its scan iterator runs out. When every armed reader is done, the result drops its batches and releases its child context.
- Reads made outside the armed set (plan-time reads, before arming) don't count. A result is not armed when it has a reader of another shape (the duplicate-name `SharedReader` / `CTEIteratorReader`) or two readers that share a spec. A reader that stops early (a LIMIT) never finishes, so its result is kept, as today.
- Stats keep working after the release: `rows` is cached, and `distinct` returns its cached value or None. A read after the release fails with UNEXPECTED_STATE rather than returning wrong rows.
- Behind a toggle, `release_cte_results`. It has no plan change, so it goes in the `noPlanChange` entries. It's a no-op while `memory_accounting` is off, like the sibling PR.
- Measure: SF10 TPC-DS per-query `peakBytes` with the toggle on and off for the CTE-heavy queries, plus `WVLET_ENGINE_BENCH_AB=release_cte_results` for timing.
- Tests: a CTE read twice releases after its second reader; a reader under LIMIT keeps the result; spec/tpcds and spec/tpch conformance runs with the toggle on catch any early release.

## Alternatives and Why Not?

- Count `reader()` calls: discarded bind-time readers would keep the count above zero for good.
- Release on reader `close()`: operators close only at the end of the query, which is today's behaviour.
- Spill CTE results to disk: that's item 8 of the goal. Releasing them comes first.
