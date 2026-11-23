# Engine optimizer M3c, PR 9: Decorrelate on the runtime plan (uncorrelated EXISTS / IN / NOT IN)

## Goals

- Start framework pass 2 (Decorrelate): filter conjuncts `[NOT] EXISTS (subquery)`, `x [NOT] IN (subquery)` over an uncorrelated subquery bind as runtime-plan semi/anti joins instead of `Legacy` nodes, so the subquery's plan is on the IR (pushdown, pruning, scan filters, shared-CTE reader filters reach into it) and the filters around it can move.
- Keep golden plans unchanged unless a filter now moves, and keep NOT IN's NULL semantics exact.
- Scope it so it stays reviewable. Correlated forms and scalar subqueries are follow-ups recorded on the item.

## Background

- `Planner.applyConjuncts` turns a conjunct with a subquery into a join over the planned input (`existsJoin`, `inJoin`). Plain conjuncts then filter on top. The runtime binder delegates any filter with a subquery to it and wraps the result as `Legacy`: either `legacy(f)` for a filter over a non-join, or `plans.conjuncts` over an ordered join tree.
- For uncorrelated subqueries: EXISTS is decided at plan time (does the subquery return rows); IN is a semi hash join on `cast(x) = cast(first column)`, building on the subquery unless it is estimated as much larger (then RIGHT SEMI, `swapBuild`); NOT IN materializes the subquery: if any value is NULL no row passes, otherwise it is an anti join with the values, then `x is not null`.

## Design

Lane: Express. An internal planner change with no user-facing behavior change. The direction and gates came with the launch prompt.

- **Semi and anti joins on the IR.** `RuntimePlan.Join` with kind `Semi` / `Anti` outputs only its left input's columns; the residual reads both inputs. `buildLeft` means the hash table holds the left rows (RIGHT SEMI / RIGHT ANTI over swapped inputs), as `Planner.semiJoin` builds it. Lowering goes through one shared helper (`JoinOrderOptimizer.semiJoinOp`), which the planner also uses. PushDownFilters pushes conjuncts into the left input (all of them, since the output is that input). PruneColumns narrows the right input to its keys. PlanValidator checks the residual over both inputs.
- **Binding.** A conjunct list binds on the IR when every conjunct is either plain (no subquery besides uncorrelated scalar subqueries, which are constants) or a top-level uncorrelated `[NOT] EXISTS` / `[NOT] IN`. Then, in conjunct order over the input:
  - EXISTS / NOT EXISTS: evaluated at plan time, as today; a false result is `Filter(false)`.
  - IN: a `Semi` join with the bound subquery. The binder chooses the build side from `Stats`, with the same rule and estimates as `Planner.semiJoin`.
  - NOT IN: the subquery is materialized at plan time, as today, and joined as a constant (`Legacy(BatchesOp)`) by an `Anti` join that never swaps, then filtered by `x is not null`; a NULL value gives `Filter(false)`.
  - The plain conjuncts filter on top.
  Any other conjunct list (correlated subqueries, subqueries under OR / NOT / CASE, IN in a non-filter position) stays `Legacy` as today.
- **Where it applies.** Both places where the binder now calls the planner: a filter over a non-join input, and conditions with subqueries over an ordered inner join tree (`plans.conjuncts`). The join-tree case keeps its order: the semi join still sits above the ordered tree. Placing a single-input semi join on its input, inside `InnerJoins`, is the next PR. It changes plans, so it needs benchmarks.
- **Plan changes.** The input of a filter over a non-join is optimized before the semi joins are built (as for join trees), so build-side estimates match the planner's. The plain conjuncts above the semi joins may now be pushed below them by the outer passes. Golden plans show whether any TPC query moves; any change gets benchmarked against an origin/main worktree.

## Result

- Toggle `decorrelate` (off: the planner's path, golden plans equal to main byte for byte).
- 4/121 golden plans changed (TPC-H q16, q20; TPC-DS q14, q95). The subquery is now parallelized together with the outer plan, so a RIGHT SEMI probe over it runs inside its scan's morsel parts instead of on the calling thread after a separate gather. In q14 this also turns the aggregation above the join into a two-phase aggregation. In q16, `x is not null` now filters before the anti join. The legacy-planner plans of q14, q16, and q20 now differ from the runtime ones, so they are added under `spec/engine-plans/legacy/`.
- SF1 A/B on one build (toggle off/on, 6 alternating runs each, medians): changed queries −3% geomean (TPC-DS q95 −11%, TPC-H q20 −8%, TPC-DS q14 +2%, TPC-H q16 +5%); unchanged-plan controls between −26% and +0%.

## Follow-ups (recorded on the item)

1. Single-input semi/anti joins placed onto their input inside `InnerJoins`, so join order sees them (plan-changing; benchmarks).
2. Correlated EXISTS / NOT EXISTS / IN / NOT IN (`planCorrelated`, `correlationKeys`) on the IR.
3. Correlated scalar subqueries (`decorrelateScalar`) and EXISTS under OR / NOT / CASE (`decorrelateExists`, a mark join).

## Alternatives and Why Not?

- **All forms in one PR.** The correlated forms re-plan the subquery body around its correlated conjuncts. That is a different binding path, and doing both at once would make the review and any plan diff much harder to read.
- **Choose semi-join build sides in ReorderJoins, as for outer joins.** NOT IN's anti join must not swap, and a later pass can't tell it apart without a new flag. Deciding at bind time, over the optimized input, keeps the planner's exact choice.
