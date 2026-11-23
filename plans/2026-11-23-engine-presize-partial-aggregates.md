# Engine: pre-size the partial (per-morsel) aggregation tables

## Goals

- Cut the group-table growth in the partial aggregations of two-phase aggregation. An SF1 trace places most aggregation groups there, and growth costs 3–5% of engine CPU in q78 and q67.
- Change no results, and don't let an estimate that is too high slow down parts that hold few groups.

## Background

- #2312 pre-sizes final, single-phase, and partitioned aggregations from `Stats.groups`, capped at 2^18 groups. The `presize_aggregates` toggle turns these hints off.
- Partial aggregations run once per scan part (a Parquet row group) inside `MorselOp`, and they got no hint. One query-wide estimate would pre-size every part's table to the whole query's group count.
- SF1 trace: q23 builds about 656 partial tables of up to 72K groups each, q78 up to 112K, and q97 up to 124K. Each one starts at 1024 slots and doubles about 7 times.
- `Stats.groups` ignores filter selectivity, so it often runs high. A pre-size that is too high zeroes megabytes of slots and key storage for each part, and that work can cost more than the growth it saves.
- The Parquet footer stores each row group's exact row count (`RowGroupMeta.numRows`), so the part's row count costs nothing to read.

## Design

Lane: Express. This is an internal performance change with no change in results, and it touches no always-gate area. Merging still waits for the user's explicit approval, as the launch prompt asks.

- `ScanSplits` carries an optional row count for each part. Parquet sources fill it from the row-group metadata; other sources leave it unknown.
- `MorselOp` gives each part's pipeline copy its row count, so the partial `HashAggOp` can read it when the pipeline is built.
- The partial aggregations of one `MorselOp` share a small sizer:
  - Parts that start before any part has finished use min(query estimate, part rows).
  - Later parts use the largest group count seen in a finished part, capped by their own row count. This corrects an estimate that ignores filters after the first window of parts (about twice the parallelism; q23 has about 656 parts).
  - Each partial aggregation reports its group count to the sizer when it finishes.
- No hint when the query estimate is 0, which covers the `presize_aggregates` toggle being off and keys without statistics. The toggle then gives the same plan as origin/main, for the A/B. No new toggle.
- The sequential fallback (a source with a single part) keeps the query estimate, as a single-phase aggregation does.
- Tests: a `MorselOp` / two-phase case where parts report group counts and later parts get the observed size, plus the standard gates (TPC-H 22/22, TPC-DS 99/99, spec/basic conformance, the golden plans, the toggle test, and engine JS/Native compile). The PR reports an SF1 A/B with the toggle on q23a, q23b, q78, q97, and q67.

## Alternatives and Why Not?

- **Static min(estimate, part rows) only:** this is simple, but a high estimate on a filtered scan pre-sizes every one of ~650 tables to the full row-group size.
- **Pre-size to the part's row count alone:** this is an exact upper bound, but it wastes the most when groups are far fewer than rows (for example, a group by store or by date).
- **Grow by 4x in partial tables:** fewer copies, but up to 4x over-allocation per part, and it ignores what earlier parts have shown.
