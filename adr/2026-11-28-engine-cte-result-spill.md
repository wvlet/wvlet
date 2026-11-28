# ADR: Shared CTE results past the spill threshold go to one spill file read by block

Date: 2026-11-28 · PR #2604

## Context

- A shared CTE result (and a grouping-set input) is computed once and held in memory for all its readers: scans split per batch for `MorselOp`, iterator readers over duplicate names, and plan-time stats. TPC-DS q23, q95, q14, and q04 hold large CTE results at SF100, and the goal is SF100 on an 8 GB heap.
- The spill framework ([ADR](2026-11-27-engine-spill-framework.md)) gives `tryReserve`, per-query spill folders, and `SpillBlocks`; #2585 drops a result once every counted reader finished. Readers' filters are pushed into the body before it runs, so the result already holds only the rows some reader keeps.

## Decision

- **All or nothing, one file.** The result drains with `tryReserve` per batch (`SharedRows.drain`, `spill_cte_results`). On the first refusal, the batches held so far and every later batch go to one spill file, and the result's reservation is released. Every reader reads the whole result anyway, so keeping a prefix in memory would save little and add a second read path.
- **Read by block position.** Small batches are appended together into blocks of about 4 MB estimated; the file records each block's offset and rows (`SpillFile.blockRows`, `readBlocks`). A reader scan's parts are the blocks, each read with its own `RandomAccessFile`, so parallel parts and several reader scans share the file without a shared stream. Readers decode blocks as they go; decoded batches are in-flight pipeline memory, not counted.
- **Raw blocks, not Parquet.** Readers' filters are already in the body; per-block pruning would skip little, and Parquet would add an encode pass.
- **Freed like the in-memory result.** The file is deleted when the last counted reader finishes (`release_cte_results`), or with the query's spill folder at query end, on success and failure. Plan-time stats over a spilled result (`distinct`, `filteredRows`) return None, as after a release; the row count comes from the file.

## Consequences

- A result that fits is unchanged: same plans, same parts, the same reservation.
- A spilled result is written once and read once per reader scan, serially encoded on the draining thread (the aggregation spill encodes in tasks; follow-up if a profile shows it).
- Results with `AnyT` columns, and readers that stop early (a limit), keep the old behaviour: in memory, and kept until the query ends.
