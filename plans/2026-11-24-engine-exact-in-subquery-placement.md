# Place uncorrelated IN semi joins by the subquery's exact row count

## Goals

- TPC-H q18 is the worst TPC-H query at SF10 (747 vs 154 ms DuckDB). Its `o_orderkey in (lineitem group by l_orderkey having sum > 300)` semi join runs over the whole `lineitem ⋈ orders ⋈ customer` tree, so the engine builds a 15M-row orders hash table and probes it with 60M lineitem rows.
- Join `orders` with the IN subquery first (624 rows at SF10), as DuckDB does, so that the inner joins see only the matching orders. Expected: q18 747 → ~300 ms at SF10.

## Background

- `place_semi_joins` already joins an uncorrelated IN to the one inner-join input its target reads, but only when the subquery is estimated no larger than that input. Otherwise the IN applies over the ordered tree, for subqueries of unknown size (e.g. a CTE) where the tree's joins may shrink the input first.
- q18 reaches that check (the `o_orderkey.in(...)` form and `o_orderkey = l_orderkey` don't block it) and fails it: row estimates pass through aggregations, so the subquery counts as lineitem's 60M rows against orders' 15M.
- A better estimate wouldn't be enough: the `l_orderkey` distinct count comes from the min/max range, 60M at SF10, and HAVING gets the default 0.3, so the estimate stays at 18M.
- The engine already runs uncorrelated subqueries at plan time: scalar subqueries, EXISTS, and NOT IN, whose values join as a `Values` leaf.

## Design

Lane: Express. The diagnosis is verified, the change is internal and sits behind a toggle, and it touches no always-gate area. The launch prompt says not to merge, so the PR waits for the user's merge decision.

- New toggle `count_in_subqueries` (on by default, requires `place_semi_joins`). When a placement candidate's estimate is larger than its input, run the subquery at plan time (with the physical passes, so it runs in parallel) and decide on the exact row count.
- Only for an input of at least 1% of the tree's largest input. Placing an IN on a small dimension saves little, and running the subquery apart from the query loses what it shares with the rest of the plan: TPC-DS q70's IN over `store` was 1.54x slower before this guard (likely because the subquery's store_sales aggregation no longer shares work with the outer query; the plan only moved the IN onto the ~100-row `store`).
- The materialized rows are the semi join's build input in both outcomes (a `Values` leaf, as NOT IN uses), so the subquery runs once. Its rows are held until the query ends, as a hash join holds its build input; no row cap, since the case that would exceed the input is the one the estimate already flags, and stopping early would mean running the subquery twice.
  - When the count is no larger than the input, the semi join goes on that input (q18: `orders ⋉ subquery`, then the inner joins).
  - When the count is larger, the IN stays over the tree as today, and the build side is chosen with the exact count (passed to the swap decision, since a `Values` leaf is always estimated at 1e3 rows).
- Join order still estimates the placed semi join at its input's rows; q18 already gets the intended order (lineitem probes the small `orders ⋉ subquery` build), so semi-join estimates stay unchanged.
- Candidates whose estimate already passes are not run at plan time, so those plans don't change and their subqueries still overlap with the rest of the query.
- EXPLAIN runs these subqueries, as it already does for NOT IN and EXISTS, and shows the computed rows as a `Values` leaf instead of the subquery's plan.
- No Lean model change: the uncorrelated IN is still the same semi join (formal/README.md, decorrelation), only its position and build input change; `DecorrelateConformanceTest` runs as before.
- Tests: a binder test over a subquery whose estimate is larger but whose result is small (placed when on, lifted when off), the toggle list, the regenerated q18 golden plan, and an SF10 A/B with `WVLET_ENGINE_BENCH_AB=count_in_subqueries`.
- Follow-ups, not in this PR: reuse the subquery's `sum(l_quantity)` (DuckDB AGGREGATE_REUSE), and row-level runtime filters on the lineitem probe (separate item).

## Results (SF10, one-JVM A/B, load 13–28)

- Golden plans change for TPC-H q18 and q20 and TPC-DS q14 and q95.
- TPC-H q18 0.61x (456 → 277 ms; CPU 0.62x). q20 1.10x (+20 ms; CPU 1.02x), where the plan-time subquery no longer overlaps with the query. q14 1.03x (unchanged plan).
- TPC-DS q14 1.02x, q95 0.95x, q70 0.95x (unchanged plan after the share guard).

## Alternatives and Why Not?

- **Estimate aggregations by their group keys and HAVING.** The range-based key NDV keeps q18's estimate above orders (see Background), and changing `rows` everywhere would move join orders in many plans.
- **Always place the semi join on its input.** That loses the case the size check exists for, where the tree's joins shrink the input before the IN.
- **Materialize every IN subquery.** Subqueries that already pass the check would stop overlapping with the rest of the plan, for no plan change.
