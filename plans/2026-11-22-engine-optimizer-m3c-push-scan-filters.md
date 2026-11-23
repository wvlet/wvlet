# Engine optimizer M3c, PR 8: PushScanFilters on the runtime plan

## Goals

- Decide Parquet row-group filters (`ScanFilters.toParquet`) on the runtime plan instead of in the physical `ColumnPruner`, so that the scan's filter and its columns are decided by id on the IR (framework pass 7).
- Let the logical `PruneColumns` narrow scans, which it can't do today because the physical pass narrows a scan together with the filter over it (notes from #2273).
- Keep golden plans unchanged: lower to the same `ScanOp`s the physical pass builds today.

## Background

- Today `ColumnPruner` attaches a row-group filter when a `FilterOp` sits directly over a `ScanOp`, translating its condition by position, then narrows the scan; both happen on operators, after lowering (toggle `push_scan_filters`).
- With the runtime planner, table reads bind to `Scan` leaves, and after PushDownFilters every single-table conjunct sits in a `Filter` directly over its `Scan` (through projections, aliases, outer join sides, CTE bodies). So the IR already has the filter where the physical pass looks for it.
- Scans inside `Legacy` leaves (subqueries, unnest, pivot, ...) are operators by the time the runtime plan exists, so they still need the physical pass until Decorrelate/M4.
- A shared CTE body gets a filter pushed into it and is pruned after the passes ran (#2273, #2277), so its scan filters are decided after that step.

## Design

Lane: Express. Internal planner change with no user-facing behavior change; direction and gates came with the launch prompt.

- **Scan carries its read.** A `Scan` keeps the source columns it reads (by source position) and an optional row-group filter, and lowers to `ScanOp(source, schema, columns, filter)` directly.
- **`PushScanFilters` pass**, after `PruneColumns` (last logical pass): for a `Filter` directly over a `Scan` of a source that supports filters, it sets the scan's filter to the translation of the whole condition (recomputed, not added, so running it again is harmless). Toggle `push_scan_filters`, as today.
- **`PruneColumns` narrows scans** to the columns needed above (filter columns included), keeping one column when none is needed, as `ColumnPruner` does; the physical pass then finds nothing left to narrow.
- **Physical pass stays for operators it still owns.** `ColumnPruner` attaches a filter only to a scan that has none yet, so `Legacy` subtrees and the legacy planner are unchanged and IR scans aren't filtered twice.
- **Shared CTE bodies** run PushScanFilters again after the reader filter is pushed and the body is pruned, so the pushed conjuncts reach the row groups as they do today.
- **Tests.** `Optimizer.only(PushScanFilters)` / `PruneColumns` cases on `Scan`; golden plans unchanged on both planners; conformance and the toggle test cover the rest.

## Alternatives and Why Not?

- **Defer until M4.** The IR already places filters over scans; only `Legacy` scans need the physical pass, and keeping it for them costs one guard.
- **Translate in Lowering instead of a pass.** Works, but leaves the decision outside the named-pass pipeline (no toggle per pass, no fired-rule count).
