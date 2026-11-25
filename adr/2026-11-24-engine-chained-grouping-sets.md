# ADR: Compute ROLLUP / CUBE sets from the partial results of finer sets

Date: 2026-11-24

## Context

The engine planned grouping sets as one aggregation per set over a shared, materialized copy of the input (`GroupingSetInput`). A ROLLUP of k keys therefore ran k+1 full aggregations over the input: TPC-DS q22 (4 keys over ~26M joined inventory rows at SF10) spent 46% of its CPU in string-key group-table work and 13% materializing the input, and ran 6.3x DuckDB (935 vs 149 ms). q67 (8 keys) ran 9 aggregations over the joined store_sales rows.

## Decision

- Toggle `chain_grouping_sets` (on by default). When the union of the sets' keys is itself a set (always true for ROLLUP and CUBE) and every aggregate splits into partial aggregates (`TwoPhaseAgg`: count, sum, min, max, avg; no DISTINCT or ORDER BY), the finest set aggregates the input into partial results, and every coarser set merges the partial results of its smallest finer set (for ROLLUP, the previous level), so each level reads fewer rows than the one before.
- A set's partial results are a shared input (`GroupingSetPartials`) when more than one plan reads them; the input itself is read once and no longer materialized.
- Each set finishes its partials into the same rows as before: keys outside the set NULL, the grouping id, avg as sum / count, and a count over no rows (the grand total of an empty input) 0.
- Other grouping sets keep one aggregation per set over the shared input.

## Consequences

- SF10 one-JVM A/B (10 runs): q22 683 → 244 ms (0.36x), q67 1,017 → 831 ms (0.82x); the other rollup queries (q05, q14, q18, q27, q36, q70, q77, q80, q86) are unchanged, since their rollups run over small, already aggregated inputs. SF1: TPC-DS 0.985x total (q22 0.50x, q67 0.82x), TPC-H unchanged.
- 9 TPC-DS golden plans changed (q05, q14, q18, q22, q67, q70, q77, q80, q86).
- Floating-point sums are added in a different order, as two-phase aggregation already does.
- Explicit GROUPING SETS whose union is not a set could also chain through an extra aggregation by the union; left out until a query needs it.
- q67's remaining time is mostly its window (`rank() over (partition by i_category ...)` filtered to rank <= 100), which sorts millions of rows in about ten partitions: a top-N per partition is a separate follow-up.
