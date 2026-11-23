# M4b, PR 10: bind filters with subqueries on the runtime plan when Decorrelate is off, and NOT IN value lists as Values

## Goals

- Continue M4b with its "filters with subqueries when Decorrelate is off" entry: no `Legacy` leaf for a subquery filter in the default configuration or with `decorrelate` disabled.
- Remove the last default-configuration leaf, the `NOT IN` value list (`fromOp` in `subqueryJoin`), which is a plan-time constant.

## Background

- `Optimization.Decorrelate` is documented as "bind [NOT] EXISTS / [NOT] IN conjuncts as semi and anti joins on the runtime plan; disabled, the recursive planner joins them". Its off state is the legacy fallback: `RuntimeBinder.filter` returns `legacy(f)`, and `plans.subqueryJoins` returns None, so `legacyConjuncts` wraps `Planner.applyConjuncts` in a `Legacy` leaf (join trees, left joins, lateral unnest).
- Retiring the legacy planner needs every off state to mean something on the runtime plan.
- `Binder.bind` already evaluates uncorrelated `[NOT] IN (subquery)` as an `InList` constant with SQL NULL semantics (`Planner.bindInSubquery`), and `EXISTS` and scalar subqueries as constants. Select items and sort keys bind subqueries this way since #2293 and #2295.
- A correlated subquery has no constant form: it can only be joined.
- `NOT IN` materializes its subquery at plan time either way. The binder then wraps that `BatchesOp` in a `Legacy` leaf, which `RuntimePlan.Values` (lowered to `BatchesOp`, with 1e3 rows in both Stats and CardinalityEstimator) can replace.
- `subqueryJoins` also gives up when an IN conjunct's target holds a subquery. The legacy planner handles that case by binding the target with the `Binder` (an uncorrelated scalar becomes a constant). No probe query hits it.

## Design

- Lane: Express. This is an internal planner change. Default plans keep their operators, and the M4b item pre-approves the work.
- With `decorrelate` off, uncorrelated `[NOT] IN` / `EXISTS` conjuncts bind as plan-time constants in a runtime-plan `Filter` (`InList`, `Const`), not as semi or anti joins. Correlated conjuncts still join, as they do with it on. The toggle then means "semi joins vs. value-set filters", which is still worth A/B testing.
- The off-state plans change shape: a `Legacy` leaf over the planner's semi/anti join becomes a constant `Filter`, unlike the legacy planner, which ignores `decorrelate`. No golden plan covers the off state. Over an inner join tree, the constant filter applies above the ordered tree, as lifted subquery conditions do today.
- Update the `Decorrelate` scaladoc to that meaning. `RuntimeBinder.filter` drops `legacy(f)` and goes through `plans.conjuncts` on both settings.
- `NOT IN`'s value list binds as `RuntimePlan.Values` under the anti join. No new node. The only golden change is TPC-H q16's leaf label (`Batches` → `Values`), from `Lowering`'s label for `Values`. It is the same operator with the same estimate.
- Drop the IN-target exclusion in `subqueryJoins`: the target binds through the `Binder`, as `Planner.inJoin` binds it.
- Still legacy after this PR, recorded on the item for the retirement entry: `decorrelate_correlated` or `decorrelate_scalar` disabled (with both `decorrelate` and `decorrelate_scalar` off, uncorrelated conjuncts too). Their off state exists only as the legacy fallback, and correlated subqueries have no runtime alternative. The retirement entry removes these toggles, or redefines them.
- EngineRuntimePlanTest's legacy-leaf test moves to a correlated EXISTS with `decorrelate_correlated` off. The `legacy(other)` forms (column edits with aggregates) fail on the legacy planner too.
- Tests:
  - With `decorrelate` off, IN, NOT IN (including NULLs in the list and in the targets), and EXISTS filters bind with no `Legacy` leaf, as a `Filter` with no join, and match the legacy planner's rows.
  - The same holds over a join tree and under a left join.
  - A `NOT IN` binds a `Values` under the anti join.
  - An IN whose target is a scalar subquery binds with no `Legacy` leaf.

## Alternatives and Why Not?

- Ignoring `decorrelate` (always joining): the toggle would become a no-op that EngineOptimizerToggleTest still runs. A value-set filter is a real alternative plan.
- Removing all three decorrelate toggles now: that changes stable `wvlet.engine.optimizer.disabled` names, which belongs with the retirement entry.
- A label field on `RuntimePlan.Values` to keep `Batches`: that touches every match for a cosmetic difference.
