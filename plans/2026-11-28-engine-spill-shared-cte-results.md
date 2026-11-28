# Spill shared CTE results to disk

## Goals

- A shared CTE result (or grouping-set input) that would take its query past the spill threshold goes to one spill file instead of the heap, so CTE-heavy TPC-DS queries (q23, q95, q14, q04) fit an 8 GB heap at SF100 (goal item 9).
- No change when the result fits: same plans, same timing.

## Background

- `SharedResult` drains its operator into memory under its own child context (`holder.drain(op)`) and readers scan the batches: per-batch splits for `MorselOp`, one-batch slices, iterator readers for duplicate column names, and plan-time stats (`distinct`, `filteredRows`, row counts for replanning).
- #2585 drops the batches once every counted reader finished; #2584 gave `tryReserve`, per-query spill folders, and `SpillBlocks`; #2600 set the default threshold with headroom.
- Readers' filters are already pushed into the body before it runs (`PushDownCTEFilters`), so the file holds only the rows readers keep.

## Design

- Lane: Standard, pre-approved by the work item and launch prompt (direction transcribed from the item).
- Toggle `spill_cte_results` (execution-time group, on by default, changes no plan).
- The result drains with `tryReserve` per batch. On the first refusal it opens one spill file, writes the buffered batches and every later batch to it, and releases its reservation. Results with `AnyT` columns, or queries that can't spill, reserve as before.
- Readers of a spilled result read blocks back from the file: one split per block (the same part shape as in memory), each read by position, so parallel readers and several reader scans share one file without a shared stream.
- Plan-time stats over a spilled result return None (as after a release); the row count still comes from the file.
- The file is deleted when the last counted reader finishes (the same point the in-memory batches drop), or with the query's spill folder at query end, on success and failure.
- Raw `SpillBlocks`, not Parquet: readers' filters are already in the body, so per-block pruning has little to skip (follow-up if a reader-only filter shows up).

## Alternatives and Why Not?

- Parquet temp file: reuses scan pruning, but costs an encode pass and a writer dependency; the item allowed either.
- Keep a prefix in memory and spill only the tail: two read paths for one result; the whole result is read by every reader anyway.
