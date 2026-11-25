# ADR: Runtime join filters reach the preserved builds of nested outer joins, not inner builds

Date: 2026-11-25 · PR #2414

## Context

- A hash join's runtime filter (`runtime_join_filters`, `row_join_filters`) went only to scans on the probe side, and below it only through the probe sides of nested joins (`Lowering.scanColumns`).
- In TPC-DS q05 the web channel is `web_returns left join web_sales` inside a UNION ALL input. It lowers to a `HashJoin[Right]` building on its preserved input `web_returns` (719K rows at SF10). The `date_dim` join above the union keeps about 1% of those rows, but its filter could not reach a build side, so all 7.2M `web_sales` rows probed the full build.
- Dropping rows of an input whose rows the join keeps (by a test of that input's own columns) drops exactly the output rows that come from them, whichever side the join builds on (`through_left`, `through_right_inner`, `through_right_of_right` in `JoinKeyFilter/Probe.lean`). A NULL-padded input is unsound: its matched rows would come back padded.
- The filter is set when the build above finishes. A morsel pipeline prepares the builds nested in its probe chain before the build of the join above (`Parallelizer.pipeline`), so a nested build in the same pipeline reads its scan before the filter exists.

## Decision

- With `build_side_join_filters`, `Lowering.passesFilter` also passes a filter into the preserved build of a nested LEFT, semi, or anti join building on the left, or of a RIGHT join building on the right.
- Inner-join builds are left out. Including them changed 24 golden plans, and at SF10 q04 and q76 were about 8% slower on wall time with flat CPU. Their builds mostly read before the filter was set, so the filter only added cost.
- Build scheduling is unchanged. A build that reads first simply reads without the filter.

## Consequences

- q05 at SF10 ran 0.78x wall and 0.74x CPU with the toggle on. Its outer join sits in a UNION ALL input with a right / full tail, so the union is no single pipeline and the `date_dim` build runs first.
- Inner builds could take filters only if builds were ordered so the build above finishes first, which `concurrent_join_builds` does not do today. That is a follow-up.
