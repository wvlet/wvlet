# Dense selected rows (TPC-H q1)

- Work item: engine profile of TPC-H q1 and q9 at SF10 (goal: Wvlet engine performance, Phase 1).
- Profile at cccd36c7 (q1 alone, 10 runs): `ProjectOp` between `Filter(l_shipdate <= ...)` and `HashAgg` is 14% of CPU. Most of it is `Batch.column` gathering the six columns of the 98% of rows the filter keeps: `StrVec.gather` 4.7%, `LongVec.gather` 3.7%, plus `bzero`. Aggregation then gathers string key codes again (`StrVec.gatherCodes` 2.4%).
- Change (toggle `dense_selected_rows`, runtime only, no plan change):
  - `ProjectOp` passes deferred columns through, still deferred.
  - Expressions that cannot fail on a dropped row (column refs, constants, decimal / double `+` `-` `*`) compute over the source rows when the filter keeps at least half of them. The result is deferred at the kept positions.
  - `HashAggOp`: the fused accumulators read their arguments at the kept positions, single-argument accumulators use `updateSelected` on the computed values, and string keys with dictionary codes look up codes at the kept positions (`GroupTable.groupIdsAt`).
- q9: 1.5x DuckDB at the same commit. Its top cost is Snappy decode in uni (shared with q1). No change here; recorded as a follow-up.
