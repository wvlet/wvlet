# ADR: Shared CTE bodies are lowered after the whole query is planned

Date: 2026-11-22 · PR #2273

## Context

A CTE read more than once is computed once and read from memory. Its body used to be planned and physically pruned when its first reader was planned, keeping every output column, so the readers' column needs never reached it (framework pain point 7). The framework plan asks for `PruneColumns` by column id across CTE boundaries, using the union of the readers' needs. While the runtime plan still has `Legacy` leaves, many readers are not on the IR: the legacy planner plans readers inside set operations, IN/EXISTS subqueries, and windows, and nested runtime plans are lowered before the outer plan is optimized. So no single logical plan holds every reader.

## Decision

- With the runtime planner, a shared CTE is a `SharedCTE` object: its body is bound and optimized at the first read, and every reader (a `CTERef` on the IR, or a legacy-planned scan) reads the same object.
- The body is lowered only at the end of the outermost `Planner.plan`, after the root plan is physically pruned. The kept columns are the union of the columns every reader scan reads, from the root and from the bodies of CTEs that read it, which are finalized first. `PruneColumns` then prunes the body by id.
- The walk over operators reaches inputs that are not children through `Operator.sharedInputs` (shared results, the parallel cross join's right input). An operator that holds an input outside its children must expose it there.
- A reader that can't say what it reads keeps every column: a read before finalization (plan-time subqueries), an iterator reader over duplicate names, or pruning turned off. A scan asking for a column the result doesn't keep fails with UNEXPECTED_STATE: the body can't be recomputed, because its `Legacy` operators run only once.

## Consequences

- Shared CTE bodies compute fewer columns (8 TPC-DS golden plans changed); the legacy planner keeps the eager plan until M4.
- When every reader is on the IR (after M4), the union can come from the `CTERef`s of the logical plan instead of from physical scans, and the walk over operators goes away.
- A future operator that hides an input in a closure without `sharedInputs` makes a query reading a shared CTE there fail loudly rather than return wrong rows.
