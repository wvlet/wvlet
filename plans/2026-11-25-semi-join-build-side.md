# Semi / anti joins build on the smaller input

## Goals

- Close part of the TPC-H straggler gap at SF10 (q21 1.9–2.7x, q4 1.95x DuckDB) where the engine builds hash tables on 38–60M lineitem rows for EXISTS / NOT EXISTS.
- DuckDB flips the same joins to RIGHT SEMI / RIGHT ANTI, builds on the outer rows (q4: 573K orders, q21: 734K), and its runtime filter cuts the lineitem probe scans to 2.3M / 140K rows.

## Background

- Semi and anti joins choose their build side once, at bind time (`Planner.correlatedJoin`, `plans.inJoin`, re-chosen for computed CTEs in `ReorderJoins`), with the inner-join rule: build on the outer input only when the subquery is more than 4x (`SwapBuildRatio`) larger.
- `CardinalityEstimator.rows` passes source rows through filters and joins, so q4 compares lineitem 60M with orders 15M and q21 the same pair: exactly 4.0, no flip (verified with a stderr print at SF10).
- The 4x margin exists for inner joins, where a swapped join adds a column-reordering projection and both sides' columns are output. A swapped semi / anti join outputs only the outer columns, needs no projection, and the build side's keys become a row-level runtime filter on the larger probe scan (`filteredProbe` already covers Semi / Anti with `buildLeft`, proved in the Lean `JoinKeyFilter.Probe` model).
- RIGHT SEMI / RIGHT ANTI hash joins with residuals and parallel probe parts already exist (TPC-H q22 uses RightAnti).

## Design

- Lane: Express — internal planner heuristic behind a new toggle, no change in results, no always-gate area.
- New `Optimization` toggle (on by default): semi and anti joins build on the outer input whenever the subquery input is estimated larger (ratio 1), instead of 4x larger.
- Applies at the three places that choose a semi / anti build side from estimates: correlated EXISTS / IN, uncorrelated IN, and the re-choice with exact CTE rows; the planner-generated reduction semi joins (`reduce_semi_joins`, `group_key_semi_joins`) keep the 4x rule for now (follow-up).
- Equal estimates do not flip (strictly larger), so unknown-size pairs keep today's plan.
- Tests: a planner test that a correlated EXISTS over a 4x-larger subquery builds on the outer input (and not with the toggle off), golden plans regenerated, SF10 A/B on q4, q21, q20, q22, q16, q2 and the TPC-DS EXISTS queries.

## Alternatives and Why Not?

- Filter-aware estimates (min/max-based range selectivity): fixes the root estimate for every join, but changes most plans and the inner-join order; too broad for this item, recorded as a follow-up.
- Adaptive build-side choice at runtime: needs both inputs' sizes before building; much larger change for the same two queries.
