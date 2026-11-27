# ADR: Infer implied join equalities inside the join search, with one key per class

Date: 2026-11-27 · PR #2542

## Context

TPC-H q5 writes `c_nationkey = s_nationkey and s_nationkey = n_nationkey`. The join search (`JoinEnumerator`) only saw the written edges, so customer could join only through supplier and came last: the engine built all 2.3M orders of 1994 and probed 1.5M customers at the top. DuckDB infers `c_nationkey = n_nationkey`, joins customer to the ASIA nations first (300K rows), and builds only 457K orders. Adding the missing equality as a query condition would double-count selectivity in the estimates (two keys of one class divide the rows twice), and would make each join hash on redundant keys.

## Decision

With `implied_join_equalities`, `ReorderJoins.withImpliedEqualities` groups the column = column edges of an inner join tree into equivalence classes. Every pair of columns in a class gets an edge marked `implied`, so the class becomes a clique that the exact search may cut anywhere. Edges of one class share an `eqClass`, and the class is the unit of both estimating and keying:

- **Estimates:** `JoinEnumerator.emit` counts a join's keys of one class as one key, each side estimated by the fewest distinct values among its columns.
- **Keys:** `JoinEnumerator.build` keys each join on one column pair per class. The other equalities hold by induction: each subtree already holds equal values for all of the class's columns it contains, so one key across the cut equates them all.
- **Scope:** only classes of three or more bare columns of one type, each on a different input. Two columns of one input would need a filter, which the join graph has no place for.
- **Fallbacks:** when the clique exceeds `PairBudget`, the exact search runs again over the written edges. The greedy fallback sees only the written edges, since its estimates multiply every key.

## Consequences

- At SF10, q5 runs 0.84–0.87x wall and 0.88–0.89x CPU, and q9 0.85–0.90x wall. The DS queries whose plans changed (q04, q11, q31, q58, q65, q74, q83) are flat across repeated A/Bs.
- The query's conditions are never rewritten, and the implied equalities are never evaluated, so results don't depend on them. `ImpliedJoinEqualitiesTest` checks rows against DuckDB with the toggle on and off.
- Not covered: non-column sides (`a.k + 1 = b.k`) and classes with two columns of one input.
