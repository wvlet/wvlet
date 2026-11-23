# Exact join order enumeration in the engine (optimizer M1b)

## Goals

- Pick the cheapest join tree, including bushy ones, for inner join graphs of up to 10 inputs, instead of the best of the multi-start greedy's left-deep trees.
- Keep planning time predictable: a budget on the enumerated pairs, and today's greedy for larger or denser graphs (q64 stays at about 14 ms).

## Background

- `JoinOrderOptimizer.plan` runs a greedy search from every input and keeps the tree with the lowest cost: C_out (rows of every intermediate result) plus the rows of every hash table built (#2233, #2239, #2259).
- Both planners use it: the legacy `Planner.innerJoinTree` over operators, and the runtime `ReorderJoins` pass over `RuntimePlan` (#2268). Each gives a `JoinSearch` that estimates rows on the partial tree it builds.
- Decision D4 of the framework plan: DPhyp/DPccp for up to 10 relations with a 10,000-pair budget (as in DuckDB), greedy fallback, bushy trees allowed, and a pipeline-break term so that left-deep probe chains win ties.

## Design

- Lane: Express. Internal optimizer change; the direction, thresholds, and gates come from the launch prompt and D4.
- Enumerate connected subgraph and complement pairs (the pairs DPccp/DPhyp emit) bottom-up over input bitsets, so every subplan is final before a larger set reads it. Equalities whose sides read several inputs are hyperedges.
- The DP works on numbers only: rows per input, the distinct count of each equality side read once from its input (a new `JoinSearch.distinct`), and the existing `CardinalityEstimator.joinOutputRows`. It builds a plan only for the winning tree.
- Cost of a join = output rows + build rows + build rows again when the build side is itself a join (the pipeline break). Build sides follow today's `swap_build_side` rule.
- Fall back to the greedy, unchanged, when the graph has more than 10 inputs, more than 10,000 emitted pairs, or no connected plan without a cross join.
- A new `exact_join_order` optimization toggle turns the enumerator off, so that `EngineOptimizerToggleTest` checks results without it and benchmarks can A/B it on one build.
- Tests: a property test over random connected graphs that every input appears once and every condition is placed once, plus bushy and fallback cases.
- Golden plans change only with benchmark medians (EngineTPCHBench/EngineTPCDSBench, A/B against main on the same machine), q64, q72, q85, and q24 first.

## Alternatives and Why Not?

- Recursive DPhyp enumeration (neighborhoods, exclusion sets): it skips unconnected subsets, which matters above about 15 inputs. At 10 inputs the bottom-up loop visits at most 3^10 subset pairs and needs no ordering argument for correctness.
- Left-deep-only DP: simpler, but D4 allows bushy trees, and snowflake joins (customer to customer_address under a sales table) gain from them.
