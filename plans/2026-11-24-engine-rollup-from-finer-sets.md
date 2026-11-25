# Engine: compute ROLLUP / CUBE sets from finer sets

## Goals

- Cut TPC-DS q22 (SF10 935 vs 149 ms on DuckDB) and q67 (1,770 vs 462 ms) by aggregating the big input once per query, not once per grouping set.
- First PR of [the SF10 wide-aggregation item](../plans/2026-11-24-engine-sf10-gap.md) (follow-up list), behind a new toggle.

## Background

- `Planner.aggregation` plans grouping sets as one full aggregation per set over a shared, materialized input (`GroupingSetInput`): q22's rollup of 4 keys runs 5 aggregations over the ~26M joined inventory rows, q67's rollup of 8 keys runs 9 over the joined store_sales rows.
- async-profiler at SF10 (q22, 5 runs): 46% of engine CPU is group-table work on string keys (`GroupTable.find`, `StrStore.hashInto`, `sameKey`, `equalsAt`, `insert`), and 13% is gathers (`StrVec.gather` 9%) that materialize the shared input.
- The engine already splits sum, count, min, max, and avg into partial and final aggregates (`TwoPhaseAgg`), so a set's partial results can be re-aggregated by fewer keys.

## Design

Lane: Standard (a planner rewrite behind a toggle); the launch prompt set the scope (q22/q67 first, one toggled PR per fix), so I proceed and hold the merge for your decision.

- New toggle `chain_grouping_sets` (on by default). It applies when the union of all sets' keys is itself one of the sets (always true for ROLLUP and CUBE) and every aggregate is one `TwoPhaseAgg` supports (sum, count, min, max, avg; no DISTINCT, ORDER BY).
- The finest set aggregates the input directly into partial results (sum and count for avg); the input is no longer materialized, since only one aggregation reads it.
- Each coarser set aggregates the partial results of its smallest already-computed superset (for ROLLUP, the previous level), merging partials: counts are summed, sum / min / max re-applied. A set's partial results are shared when a coarser set reads them.
- Each set then finishes its partials (avg = sum / count, count of an empty grand total = 0) into the same output as today: keys outside the set as NULL, plus the grouping id.
- Other grouping sets (explicit sets whose union is not a set, or unsupported aggregates) keep today's plan.
- Tests: grouping sets, ROLLUP, CUBE with every supported aggregate, NULL keys, FILTER, an empty input (grand total row), and `grouping()`, against DuckDB, toggle on and off. Golden plans regenerated; an ADR for the decision.
- Measure: q22 and q67 at SF10 with `WVLET_ENGINE_BENCH_AB=chain_grouping_sets` (10 runs), and both SF1 suites.

## Alternatives and Why Not?

- **Faster string-key group tables only (dictionary codes, hash caching).** Helps every query a little, but still does 5–9x the group-table work on rollups; a later PR can still do it.
- **Compute every set from the finest set only.** Simpler, but q67's finest set has nearly as many groups as input rows, so 8 more passes over it gain little; chaining shrinks each level.
- **One pass that updates every set's group table per row (DuckDB-style).** More group-table work per row than chaining, since coarser tables see every input row.
