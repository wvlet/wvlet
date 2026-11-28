# ADR: Sorts spill sorted runs and merge them; windows spill by their shared PARTITION BY keys

Date: 2026-11-28 · PR #2605

## Context

- `SortOp` and `WindowOp` hold their whole input (`MemoryContext.materialize`), so a sort or window past the query's spill threshold runs out of heap. Aggregations ([ADR](2026-11-27-engine-aggregation-spill.md)) and join builds ([ADR](2026-11-27-engine-grace-hash-join.md)) already spill through `tryReserve`, per-query spill files, and `SpillBlocks`.
- TPC-DS sorts are almost all `limit 100`, and the planner already keeps a top-N per morsel part, so the final sort rarely holds much; windows over large inputs (q47, q51, q89, q67's rank over its rollup) are the holders that matter. DuckDB and DataFusion sort out of core; DuckDB also spills windows.

## Decision

- **Sort: sorted runs and a k-way merge (`spill_sort`).** A spillable sort buffers its input with `tryReserve`, counting each batch twice (it and its concatenated copy) plus 8 bytes per row of indexes. Past the threshold it sorts the buffer with the existing parallel sort, encodes the run's blocks in the range tasks, writes them to one file, releases the buffer, and starts the next run; runs are at least a sixteenth of the threshold even when other holders took the rest. The last buffer sorts in memory as the newest run. A heap merge of the runs' head rows streams the output, ties going to the earlier run, so the result equals the stable in-memory sort. More than 64 runs merge in passes over consecutive groups, which keeps that order.
- **Top-N stays in memory.** With a limit, a full buffer is cut to its top `limit` rows and kept as long as that halves it; only a limit too large for that writes runs, each cut to the limit, and the merge stops at the limit.
- **Window: hash split by the shared PARTITION BY keys (`spill_window`).** A window's rows depend only on their window partition, so the input can be split by the PARTITION BY expressions every call shares (the intersection, so q47's two call groups still split). Past the threshold the window routes its buffered and remaining rows with `AggSpillRouter` into 64 files, reseeded so a file's rows still spread over the in-memory window's parallel parts, then runs the same window over one file at a time and deletes each file once read. The window over a file splits once more when it doesn't fit; deeper, it holds its input and meets the hard limit. A spillable window counts its batch plus 8 bytes per call and 16 bytes of indexes per row.
- Not spilling: windows whose calls share no PARTITION BY key, the streamed window top-N (it keeps only its top rows), and inputs with `AnyT` columns; these reserve as before. Both toggles are on by default and change no plan.

## Consequences

- When nothing spills, only the reservation of a spillable sort or window changes (closer to what it holds while sorting or evaluating).
- The final merge of a spilled sort runs on the thread that drains the sort; run sorting and block encoding stay parallel. A parallel merge (cut points sampled across run files) is a follow-up.
- A window whose calls share no PARTITION BY key (a global `row_number() over (order by …)`) still holds its input; spilling it would need an external sort of the input and a streaming evaluator.
- A spilled window emits its rows partition file by partition file; its output order was already unspecified.
