# M4b, PR 8: CTE references on the runtime plan

## Goals

- Close M4b's "CTE references (inlining/sharing)" entry: find which CTE forms still reach `Planner.planLegacy` from `RuntimeBinder`, bind them, and record what is left before the `Legacy` leaf can go.

## Background

- `RuntimeBinder.table` already resolves a table name against the CTEs in scope (`Planner.cteDef`). A CTE read once, or every CTE with `share_ctes` off, binds its body in place, so that the passes see through it (M3c). A CTE read more than once binds as `RuntimePlan.CTERef` over a `DeferredCTE` whose body is bound on the first read, then lowered once every reader is planned, keeping the union of the readers' columns and the rows their filters agree on (M3c, `-pushdown-shared-ctes.md`). `aliased` keeps a `CTERef` under another alias.
- `WithQuery` registers its CTEs and binds its body; reference counts come from `countCTERefs` (including reads in subqueries).
- To measure what still goes legacy, a temporary probe logged every `RuntimeBinder.legacy` / `legacyOp` call while EngineBasicSpecTest (spec/basic), EnginePlanGoldenTest (TPC-H and TPC-DS), and EngineRuntimePlanTest ran on the runtime planner. `legacy(relation)` was never called. Two `legacyOp` calls remained, and neither reads a CTE: the legacy-leaf test's sort key with a scalar subquery (`planSort`), and the value list of a `NOT IN (subquery)` evaluated at plan time (`fromOp` over a `BatchesOp`, a constant).

## Design

- Lane: Express — a test and a record; no change in planning or results.
- No code change: every CTE form binds on the runtime plan already. The model has no recursive CTE.
- EngineRuntimePlanTest pins the binding: a CTE read once binds as its body over a scan (no `CTERef`); a CTE read twice, once under another alias, binds as two `CTERef`s over one body with fresh column ids and no legacy leaves in it; with `share_ctes` off both reads bind in place. Each plan passes the validator and returns the legacy planner's rows.
- The M4b list records the forms still legacy, for the PRs that retire the leaf: sort keys with subqueries; filters with subqueries when Decorrelate is off; column edits whose new columns hold aggregates or unnest; unnest projections with windows (which fail on the planner too). Then the `Legacy` leaf, `Planner.planLegacy`, and the legacy planner mode (with `spec/engine-plans/legacy/`) go.

## Alternatives and Why Not?

- Moving the reference counting (`countCTERefs`) and the share decision into `ShareSubplans` as an IR pass: the binder decides sharing while binding, the same way as the planner, and plans are identical. Doing it on the IR needs the whole query bound before any CTE body is lowered, which the `DeferredCTE` already gets by lowering after every reader is planned. It buys nothing until the planner goes, so it waits for the retirement PRs.
