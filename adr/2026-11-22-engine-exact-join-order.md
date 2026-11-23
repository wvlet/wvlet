# ADR: Exact join order enumeration for small join graphs in the engine

Date: 2026-11-22 · PR #2272

## Context

The engine ordered inner joins with a multi-start greedy search that builds only left-deep trees ([2026-11-22-engine-join-order-estimates.md](2026-11-22-engine-join-order-estimates.md)). Snowflake joins such as a sales table over customer and customer_address then probe the large table once per dimension, where joining the two dimensions first would probe it once. Decision D4 of the optimizer framework ([2026-11-22-engine-optimizer-framework.md](2026-11-22-engine-optimizer-framework.md)) chose exact enumeration for small graphs, with a budget and the greedy search as fallback.

## Decision

- `JoinEnumerator` plans graphs of 2 to 10 inputs. It costs every pair of connected input sets that an equality joins (the csg-cmp pairs of DPccp/DPhyp). An equality whose sides read several inputs is a hyperedge
- It visits input sets in increasing bitset order, with the half holding the lowest input first. Both halves of a pair are final before the pair is costed, so correctness doesn't depend on the order of a recursive neighborhood walk. At 10 inputs it visits at most 3^10 subset pairs. DPhyp's neighborhood recursion saves work only on graphs we leave to the greedy search anyway
- The cost of a join is its output rows (C_out), plus the rows it builds into a hash table, plus those rows again when the build side is itself a join. The last term is the pipeline break: it makes left-deep probe chains win ties, and the engine's morsel parallelism favors long probe chains. Build sides follow the `swap_build_side` rule, as in the greedy search
- The search only computes numbers: the rows of each input, and the distinct count of each equality side, read once from the input it reads (`JoinSearch.distinct`). Only the winning tree is built. The tree places each condition where the costing placed it: a hash key or a filter at the lowest join covering its inputs. Conditions that read no input are applied over the tree
- The greedy search, unchanged, plans graphs with more than 10 inputs, graphs with more than 10,000 emitted pairs (as in DuckDB), and graphs that need a cross join
- The `exact_join_order` optimization toggle turns the enumerator off, which restores the greedy plans exactly

## Consequences

- 69 of the TPC-H and TPC-DS golden plans changed, many to bushy trees. On SF1, the changed TPC-DS queries took 0.92 of main's time in total, TPC-H took 0.98, and q05, q06, q13, and q48 ran 25–60% faster. q17 and q25 looked slower, but by about as much as the noise on the shared machine
- Planning time is unchanged within noise. q64 stays at about 11 ms, as its larger graphs keep the greedy search
- The rows estimated for an input set depend on the split that produced its best plan, as in the greedy search, so the search is not strictly optimal under its own model. Split-independent estimates come with the `Stats` work in M4
