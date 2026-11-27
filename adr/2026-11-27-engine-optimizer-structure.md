# ADR: Restructure the engine optimizer through plan-identical refactors

Date: 2026-11-27 · PR #TBD

## Context

About 150 optimization PRs grew the engine planner around one `Planner` class (4,582 lines) that binds, runs plan-time subqueries, finalizes and replans CTEs, and configures physical operators. Several bugs and wrong A/Bs came from the structure rather than the logic:

- Operator flags are mutable fields set after lowering, and Parallelizer rebuilds operators copying each flag by hand. A flag a rebuild forgot ran its toggle-off A/B with the flag on (#2506).
- Memo tables keyed by object identity miss copies that `transform` or `expandTemplates` made (#2464, #2526).
- Some flags are applied both before and after Parallelizer.

Six optimization sessions edit the same files at once: since 2026-11-19, 152 of 259 engine commits touched `Planner.scala`. A rewrite would conflict with all of them and change plans. The full map and ranked plan are in [`plans/2026-11-27-engine-optimizer-structure.md`](../plans/2026-11-27-engine-optimizer-structure.md).

## Decision

- Restructure through small refactor PRs that change no plan: the golden plans under `spec/engine-plans/` stay identical, every `Optimization` toggle still works (`EngineOptimizerToggleTest`), and an SF10 check against an `origin/main` worktree in alternating rounds shows no regression.
- Such a refactor has no `Optimization` toggle. The rule "give every planner change a toggle" exists so that a change can be A/B'd and turned off; a refactor with identical plans has nothing to turn off, and the golden plans prove it.
- Each PR is a move or a helper with no logic change, so a parallel PR's conflict is a mechanical re-apply. Moves of the hottest files wait until few engine PRs are open.
- Operators copy their own flags: a rebuild calls `withFlagsOf` (`exec/JoinFlags.scala` for hash joins, like `ScanOp`'s), and `EngineJoinFlagsTest` finds the flags by reflection, so a new flag a rebuild doesn't copy fails the test without editing it.
- `Planner` is split into mix-in traits over its state, widening the moved members to `private[plan]`, rather than into helper objects that take the state as parameters: call sites stay unchanged.
- `Stats` and `CardinalityEstimator` stay separate. Unifying them would change estimates and plans, which needs toggles and A/Bs, not a refactor.

## Consequences

- Flags that rebuild sites leave out today (Parallelizer's single-thread aggregation fallbacks, the partial aggregation's tail) stay left out, so plans stay identical. Restoring them is a toggled follow-up.
- Logic changes found along the way (re-optimizing bound inputs once, copy-tolerant lookups for the other identity memo tables) are follow-ups with their own toggles.
