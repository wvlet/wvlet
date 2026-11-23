# M4b, PR 5: bind USING and NATURAL joins on the runtime plan

## Goals

- Next entry of the M4b list (ASOF, USING, and NATURAL joins), split in two: this PR binds USING and NATURAL joins as runtime-plan nodes instead of `Legacy` leaves; the ASOF join, which needs a new `RuntimePlan` node, follows in the next PR.
- Results stay identical on both planners, including the merged key columns of outer joins (NULL-padded sides coalesced).

## Background

- `RuntimeBinder.nonLateralJoin` sends every join that is neither an inner join tree nor an ON join (`Planner.isJoinOn`) to `planner.planJoinTree` as a `Legacy` leaf; filters over the join are applied there too.
- `Planner.usingJoin` plans `JOIN ... USING (cols)`: a hash join on the same-named columns (building on the smaller input for outer joins, `swapsOuterBuild`), then a projection with one merged column per key (the left key, the right key for RIGHT joins, `coalesce(left, right)` for FULL joins, cast to the common type), followed by the other left and right columns. A NATURAL join is the same with the key names shared by both inputs.
- In Wvlet, `join t on id` with bare column names parses as USING (`JoinUsing`); SQL `USING (...)` does too. No parser builds `NaturalJoin` today, but the planner handles it.
- No spec/basic, TPC-H, or TPC-DS query uses USING or NATURAL, so golden plans and benchmarks are unaffected.

## Design

- Lane: Express — an internal refactor with no change in results, pre-approved by the M4b item.
- `Planner.usingJoin` becomes a shared helper over the plan builder (`JoinTreeBuilder` and `AggPlanBuilder`), as `joinOn`, pivot, and grouping sets are, so both planners build the same operators from one piece of code.
- On the runtime plan, USING and NATURAL joins bind as a `Join` with keys and no residual, under a `Project` for the merged key columns. No new `RuntimePlan` node is needed.
- Filters over the join apply over the projection as for an outer ON join (`plans.conjuncts`); PushDownFilters may then move them through the projection and join as for other joins.
- The ASOF join stays legacy in this PR; the legacy-leaf test keeps using it.
- Tests: EngineRuntimePlanTest checks the bound shape (no `Legacy` leaf) and that inner, left, right, and full USING joins (with NULL keys and a filter above) return the same rows as the legacy planner; a spec/basic file covers USING joins on both planners and DuckDB.

## Alternatives and Why Not?

- A new `RuntimePlan.UsingJoin` node: every pass would need a case, though `Join` + `Project` express it.
- Rewriting USING into an ON join plus a projection in the binder alone: duplicates the planner's merge rules instead of sharing them.
