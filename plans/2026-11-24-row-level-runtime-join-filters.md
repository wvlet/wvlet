# Row-level runtime join filters in probe scans

## Goals

- Drop probe rows that can't match a hash join's build inside the probe-side Parquet scan, per row, before the other probe columns are gathered and before the operators between the scan and the join (other joins' probes, filters, projections) see them.
- Close part of the SF10 gap on the FK joins where DuckDB's scan outputs are small (TPC-DS q87, q73, q80, q17; TPC-H q7, q10, q12). Target 0.8–0.9x of today on the affected queries.

## Background

- `runtime_join_filters` gives a probe scan the build's [min, max] key range, and the range only skips whole row groups (`exec/RuntimeFilter.scala`, `runtime/Lowering.scala:231`). Every row of a surviving row group flows up the pipeline.
- The rules for which scans may get a filter already exist (`Lowering.runtimeFilters` / `scanColumns`): joins that drop unmatched probe rows, through filters, plain projections, UNION ALL inputs, and the probe sides of row-preserving joins. The new filter reuses them unchanged.
- Builds already finish before their probe scans start: a morsel pipeline runs every build in `prepare` before it splits the scan, and a sequential join builds on its first `next()`. So no new scheduling is needed if the scan reads the filter per batch rather than once at open.
- Batches defer columns (`Batch.take`): narrowing a batch by a selection gathers no column until something reads it, which is the `late_gather` effect the item asks for. Operators defer only while `late_gather` is on (`HashJoinOp.rowsOf`, `FilterOp`).
- DuckDB 2.0.1 uses an IN list (≤ 50 keys), a bitmap over a dense integer range, or a blocked Bloom filter, applied per row and paused when it passes most rows. DataFusion applies its dynamic filters per row only with `pushdown_filters`, off by default.
- H q18 gains little from this alone: its selective key (`o_orderkey` from the IN subquery) is on the build side of the lower join, so no filter reaches lineitem until the semi-join placement item (#3 in the gap matrix) lands.

## Design

Lane: Standard — a new optimization in the engine's join path, with a Lean model change.

- New toggle `row_join_filters` (on by default, listed in `OptimizerOptionsTest`), applying only where `runtime_join_filters` already gives a scan a filter: integer keys (Int32, Int64, Date, Timestamp, TimestampTz, as `rangeType` accepts today) whose probe key is a scan column.
- The build publishes, next to today's range, an exact or approximate membership test over its non-NULL keys:
  - a bitmap over [min, max] when the range is dense enough (a few bits per build row), exact;
  - a register-blocked Bloom filter otherwise (one 64-bit word per key, ~1–3% false positives), combined with the range check.
  - No separate IN list: for small builds the bitmap or a small Bloom filter is as cheap per row, and exact enough.
- Skip the membership filter when the build has more keys than a cap (a few million), where building it costs more than it can save and the join is rarely selective.
- The scan applies every set filter to each batch right after the source returns it, on both the sequential path (`ScanOp.next`) and each morsel part, and narrows the batch with a deferred `take` when `late_gather` is on (a gather when it is off). NULL probe keys are dropped (they never match in these joins).
- Pass-rate pausing: each filter counts rows checked and passed across all scan copies. After a sample window, a filter passing ≥ 50% of rows stops being applied for a while, then samples again; one that keeps passing most rows stays off.
- Explain text is unchanged (`join_filter=` already names the filtered columns), so golden plans don't move; tests check the scan's output row counts instead.
- Lean: add to the `JoinKeyFilter` model a theorem that filtering the probe input by any predicate true on every non-NULL build key keeps the join's rows, for the probe sides the engine uses (left input of inner / semi / right joins; right input of inner / left / semi / anti joins), plus a counterexample for the preserved input of an outer join. Traces export the sound (kind, probe side) pairs, and a conformance test checks `Lowering`'s choice against them. The path rule from a join down to the scan (`scanColumns`: filters, projections, UNION ALL, probe sides) is unchanged from today's range filter and stays unmodeled; the note in the model says so.
- Docs: extend the `JoinKeyFilter` row of design/formal-verification.md and the model header to cover `Lowering.runtimeFilters`; amend adr/2026-11-23-engine-runtime-join-filters.md ("only ranges are used", "a scan that started earlier reads without the filter") with the membership filters, per-batch reads, and pausing.
- Tests cover each filter kind, NULL keys, duplicates, pausing, and results equal with the toggle off on joins across inner / semi / right / left / anti kinds. A/B with `WVLET_ENGINE_BENCH_AB=row_join_filters` on selected SF10 queries.

## Alternatives and Why Not?

- **Probe the join's own hash table from the scan (DataFusion).** Exact, but the table is often much larger than cache and the probe repeats later anyway; a bitmap or Bloom filter is smaller and branch-free.
- **Push the filter into uni's Parquet decoder so payload columns decode only for surviving rows.** Larger payoff on wide scans but needs uni changes; a follow-up once the filter exists (related to the filter-first decode item).
- **Explicit build-before-probe scheduling.** Not needed today: builds already run before their probe scans start; reading the filter per batch also covers a scan that starts early.
