# ADR: Filter an aggregation's input by the builds of the LEFT joins that a match-requiring filter sits above

Date: 2026-11-26 · PR #TBD

## Context

TPC-DS q78 aggregates the store_sales rows of one year by (year, item, customer), LEFT joins the result to the web and catalog aggregates on those three keys, and keeps a row only when `coalesce(ws_qty, 0) > 0 or coalesce(cs_qty, 0) > 0`. At SF10, 4.97M input rows make 4.90M groups, and only 6.3K groups have a web or catalog match (386 web, 5,904 catalog). The engine aggregated all of them in two phases, then probed both join tables (1.3M and 2.6M keys) with every group, and almost every probe missed. The probe misses were ~3% of q78's CPU; the near-unique aggregation cost much more.

## Decision

- A physical pass (`left_match_prefilter`, `plan/LeftMatchPrefilter.scala`) runs right after column pruning. It matches `Filter(c) ← LEFT joins ← Project* ← HashAgg`, where one conjunct of `c` is an OR whose every part reads the build columns of one join only and is not true when they are all NULL. The pass checks that by evaluating the part on a row of NULLs, so `coalesce(x, 0) > 0` qualifies and `x is null` doesn't. Each needed join's probe keys must map, through column-only projections, to group keys of the aggregation.
- The pass puts a `FilterOp` over `BuildKeyMatch` (`exec/BuildKeyMatch.scala`) between the aggregation and its input. The filter keeps a row when some needed join's build has its key. It probes the join's own `JoinBuild` through `firstMatches`, so it needs no separate key set and handles packed multi-column keys the same way. A dropped row's group has no match in any needed join, so `c` would drop that group anyway. The joins and `c` stay unchanged.
- The filter reads builds that the joins above it make. Each `HashJoinOp` carries its filter sources (`prefilters`), and `Parallelizer` binds them to the build of every join it makes, so the filter reads the build that the pipeline's `prepare` step fills before it drains the aggregate below. If a build isn't ready, the filter keeps every row. Because of that fallback, a wrong binding can only make the query slower, never wrong.
- The filter turns itself off when it keeps more than half of the first 64K rows it sees. With many rows per group, a pre-aggregation probe costs more than the probe of each group after the aggregation.
- The joins the filter probes also get the `probe_miss_filter` Bloom filter, since the filter's probes mostly miss.
- This is an executor-side filter rather than a logical semi join, which would need the web and catalog CTEs planned twice or shared, and would change the join order.

## Consequences

- q78: see PR for the SF10 A/B. Only q78's golden plan changes.
- The pass rebuilds the operators above a rewrite only through the types it knows (filters, projections, sorts, limits, hash joins, and aggregations), so a match under another operator type, or under a shared CTE input, is left alone.
- A general Bloom filter for mostly-missing LEFT joins without such a filter is not done. It would need a match-rate estimate or a probe sample.
