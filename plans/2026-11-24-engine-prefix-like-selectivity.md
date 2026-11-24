# Engine: reduce TPC-H q20's aggregation by estimating prefix LIKE as a range

## Goals

- Follow-up to #2364 (`group_key_semi_joins`) for the TPC-H SF10 gap. q20 is the largest remaining TPC-H ratio on main 61c674d9: about 2.7–2.9x DuckDB warm (265–315 ms vs 98 ms). The target is that warm gap; a cold single run showed 15x.
- Let q20's decorrelated `sum(l_quantity)` per part and supplier aggregate only the rows of the `forest%` parts, as #2364 already does for q17.

## Background

- SF10 baseline on main 61c674d9 (one run, load average about 10): q7 3.9x, q10 3.3x, q20 15.0x at 3.3 busy cores, q21 1.9x DuckDB.
- q20's plan aggregates the whole year of `lineitem` (about 9M rows, millions of groups), then joins it to `partsupp ⋉ σ(p_name like 'forest%')`. About 1% of parts match.
- Two things stop `group_key_semi_joins` there:
  - `like 'forest%'` gets the default selectivity of 0.3, so `σ(part)` is estimated at 600K of 2M parts and misses the bar of 1/8 of the group key's distinct values.
  - `ReorderJoins` turned the left join into a right join so that it builds on the smaller side. The aggregation ends up on the left, and the pass only matches an aggregation on the right.
- Profile of q20 at SF10 (async-profiler, `cpu,threads`, warm-up plus 3 runs, engine-thread samples): Parquet decoding of `lineitem` is 60% (Snappy alone 26%). Grouping (`GroupTable`, decimal sums, partitioned aggregation) is 17%, and zeroing (`bzero`) is 10%. All of that grouping and zeroing is spent on groups that the join never looks up.
- Not picked:
  - q7 and q10 spend their time in the probe and in Parquet decoding, with no single hot spot.
  - In q21, most zeroing happens in uni's Parquet reader, which needs a uni release.

## Design

Lane: Standard (an estimator change that moves plans, plus a wider rewrite).

- New toggle `prefix_like_selectivity` (on by default). It estimates `x like 'p%'` as the range `x >= 'p' and x < next(p)`: two range bounds at the default selectivity each, 0.09. That matches how a date range or a BETWEEN is estimated today.
- It covers only a non-negated, case-sensitive pattern with one trailing `%` and no `_` or escape character. `not like`, `%x%`, and suffix patterns keep the default.
- It applies to the runtime estimate (`Stats.selectivity`), which `ReorderJoins` and `GroupKeySemiJoins` read. The logical `CardinalityEstimator.selectivity` has no production caller left (only `JoinGraph` tests), so it stays as it is, and the `Stats` doc says where the two now differ.
- `GroupKeySemiJoins` also matches a right join with the aggregation on the left, when the join builds on the right. That is the mirror of the left join it already handles.
  - The aggregation's side is lowered first there. No runtime join filter from above passes through such a join into its build side, so the copy of the key source takes no filter meant for the original.
  - It stays under `group_key_semi_joins`, as more coverage of that pass. On its own (with the LIKE estimate off) it moves no golden, SF1, or SF10 plan, so the `prefix_like_selectivity` A/B measures the whole change.
  - The ADR of #2364 gets an amendment line, and the pass's Scaladoc is updated, both of which said right joins are left out.
- Plans change only for TPC-H q20 and TPC-DS q91 (`hd_buy_potential like 'Unknown%'`, a different join order): in the golden plans, and in the SF1 and SF10 plans dumped with the estimate on and off (q91 changes at SF1 only). TPC-H q14's LIKE is inside a CASE and q16's is negated, so their plans stay the same.
- Prototype at SF10: q20 runs in 225–231 ms (2.3–2.4x DuckDB). Grouping samples drop from 1620 to 276 and zeroing from 893 to 336; Parquet decoding is unchanged and is now the bulk of q20.
- Full A/B at SF10 (RUNS=3): q20 0.64 wall and 0.64 CPU (201 vs 315 ms). Suite CPU is 0.99 for both TPC-H and TPC-DS. The suite wall totals of 1.05 and 1.03 come from queries whose plans are unchanged (TPC-H q4, q21; TPC-DS q03, q24), so that is load noise, to recheck with RUNS=10.
- Tests cover the estimate (prefix patterns and the ones it refuses), the right-join rewrite and its refusals (right join building on the left, results equal with the toggle off), the toggle in `OptimizerOptionsTest`, and regenerated golden plans. Full-suite A/B at SF10 (RUNS=3) and SF1 (RUNS=5), plus RUNS=10 on q20 and q91 next to controls.
- No Lean model: `GroupKeySemiJoins` and `Stats.selectivity` are outside the areas listed in `design/formal-verification.md`.

## Results

`WVLET_ENGINE_BENCH_AB=prefix_like_selectivity` (on / off, one JVM, load average 7–15):

| run | wall | CPU | geomean |
|---|---:|---:|---:|
| TPC-H SF10, RUNS=3 | 1.054 | 0.991 | 1.041 |
| TPC-DS SF10, RUNS=3 | 1.031 | 0.990 | 1.021 |
| TPC-H SF1, RUNS=5 | 1.016 | 1.005 | 1.010 |
| TPC-DS SF1, RUNS=5 | 0.999 | 0.998 | 0.994 |

- TPC-H q20 at SF10: 0.64 (RUNS=3) and 0.73 (RUNS=10), with CPU the same. It is now about 2.3x DuckDB.
- The SF10 suite wall outliers have unchanged plans, and they come back near 1 with RUNS=10: TPC-H q4 1.03, q21 0.91, controls q1 1.05, q3 0.98; TPC-DS q03 0.96, q24 1.02, controls q01 1.01, q17 0.96. TPC-DS q91 1.00.
- TPC-H q20 at SF1: wall 1.04–1.05 (about +1.5 ms of 30 ms), CPU 0.69–0.70. At that size the scan of `lineitem` waits for the build of `σ(part)`, which saves no wall time.

## Alternatives and Why Not?

- **Lower the 1/8 bar**: q34 would then semi-join all of `customer` (the reason the bar exists), and it doesn't help with the right join.
- **A per-character LIKE estimate (PostgreSQL's 0.2^n)**: gives 6e-5 for `forest%`, against 1% actual. Underestimates that far can pick bad join orders.
- **Min/max string stats from Parquet**: more precise, but the footers' string bounds are often truncated, and it needs a new stats path.
- **Also change the logical `CardinalityEstimator`**: nothing in production reads its selectivity any more, so the change would be untestable through plans.
