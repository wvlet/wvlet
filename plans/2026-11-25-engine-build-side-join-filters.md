# Engine: runtime join filters on the kept build side of nested joins (TPC-DS q05)

## Goals

- Close most of the TPC-DS q05 gap (3.1x DuckDB at SF10, the second-worst TPC-DS query).
- Work item: `2026-11-25-engine-tpc-ds-q05-at-sf10-3-1x-left-joins-under-union-projec`.

## Evidence (SF10, 78f8a858)

- async-profiler on q05: the hottest frame is `LongJoinTable.firstRows` under the web channel's `web_sales ⟕ web_returns` probe (7.2M `web_sales` rows into a 719K-row build), then the `web_sales` decode.
- The LEFT join is `web_returns left join web_sales`; it lowers to `HashJoin[Right]` building on its preserved input `web_returns`. Above the UNION ALL, `date_sk = d_date_sk` joins the 15-row `date_dim` slice, so only ~1% of `web_returns` rows survive. The engine gives that join's runtime filter to every scan of the union's probe side except `web_returns`: `Lowering.scanColumns` stops at build sides.
- DuckDB has the same gap (its profile scans all 719K `web_returns` rows and all 7.2M `web_sales` rows), so closing it puts the engine ahead on that branch.
- The work item's hypothesis (outer-to-inner for LEFT joins under a UNION) does not help q05: converting the join leaves the same build and probe, and the date filter still can't reach `web_returns`.

## Design

Lane: Standard (Lean-modeled runtime join filters); merge authorized by the loop rules after green CI and a no-regression SF10 A/B.

- New toggle `build_side_join_filters` (needs `runtime_join_filters`, in `OptimizerOptionsTest`).
- `Lowering.scanColumns` also descends into the preserved build input of a nested LEFT, RIGHT, semi, or anti join. It never enters a NULL-padded input.
- Inner joins' builds stay out: a first version included them (24 golden plans changed), and q04 and q76 ran ~8% slower on wall time with flat CPU, because the morsel pipeline prepares those builds before the build above sets the filter.
- A build reads its scan before the filter is set when the morsel pipeline prepares the inner build first; the scan then reads without it, as a probe scan that starts early already does. No scheduling change.
- Lean first: `JoinKeyFilter/Probe.lean` gains `through_left`, `through_right_inner`, `through_right_of_right` (filtering a kept input by any test of its rows drops exactly the output rows from them, whatever the build side), and counterexamples for the padded inputs.

## Validation

- Engine tests: the q05 shape (an outer join's preserved build in a UNION ALL input takes the filter) and the padded input staying unfiltered; results equal with the toggle off and with runtime filters off.
- Golden plans regenerated.
- SF10 A/B `WVLET_ENGINE_BENCH_AB=build_side_join_filters` on q05, q49, q77, q80 and on every query whose plan changed.

## Alternatives and Why Not?

- **Outer-to-inner under UNION/projection (the item's plan).** Same build and probe after the rewrite; no effect on q05. Kept as a follow-up for queries where the inner join can then reorder.
- **Push the `date_dim` join into each UNION ALL input.** Gets the same reduction statically but duplicates dimension scans and needs a new costed rewrite; the runtime filter does it without plan changes.
- **Order builds so outer builds run first.** Would make more build-side filters effective, but changes `concurrent_join_builds` scheduling; a follow-up if the A/B shows build-side filters that are set too late.
