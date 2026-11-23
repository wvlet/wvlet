# Engine optimizer M4b: the `legacy(other)` fallthrough

## Goals

- Remove the last `RuntimeBinder.legacy(relation)` call: column edits whose new columns hold aggregates or unnest, and unnest projections with windows, which today plan through `Planner.planLegacy` under a `Legacy` leaf
- Keep the user-facing behaviour: the same error on both planners
- Leave nothing for the retirement PR to bind, only deletions

## Background

- These forms fail on the legacy planner already, from the shared `Binder` (a probe ran each form on both planners and got the same message on each):
  - `from nation add count(*) as n`: "Aggregate function count is not allowed here"
  - `from nation add unnest([1, 2]) as u`: "Unsupported function in the engine: unnest"
  - `select n_name, unnest([1, 2]) as u, row_number() over (...)`: "Window functions are not supported here"
- The runtime planner raises the same errors today only because `legacy(other)` calls `planLegacy`, which reaches the same `Binder`
- `Planner.planProject` routes a projection as: aggregation, then unnest (whatever else it holds), then windows. `planColumnEdit` binds every new column through `withWindows` and `bindItems`, without a guard

## Design

Lane: Express. An internal refactor of the runtime binder with no change in results, errors, or golden plans.

- Errors, not new semantics: none of these forms has one obvious meaning
  - `add count(*)` could be a whole-input window count or a grouped aggregate; SQL rejects it as well
  - `add unnest(list)` could repeat rows like `select *, unnest(list)`, which needs a design of its own (replace-in-place naming, prepend order)
  - unnest next to windows leaves open which runs first
- The binder routes these forms the way the planner does, so the shared `Binder` raises the error with no `Legacy` leaf:
  - column edits always bind through `columnEdit` (the `isPlainColumnEdit` guard goes away)
  - a projection with unnest binds through `unnestProject` even when it also has windows (`bindUnnestItems` raises the window error, as `projectWithUnnest` does); every other projection binds through `windowProject`
- The `case other` fallthrough raises the planner's own "Unsupported relation in the engine" error instead of calling `planLegacy`. The now-unused `legacy(relation)` helper and the alias case for a `Legacy` scan go with it
- What stays for the retirement entry: the `legacyConjuncts` leaves (the off states of `decorrelate_correlated` / `decorrelate_scalar`), `Planner.planLegacy`, and the legacy planner mode
- Retirement is the next PR, not part of this one: it deletes the legacy planner mode, `planLegacy`, the `spec/engine-plans/legacy/` files, and EngineBasicSpecLegacyTest, and redefines or removes the decorrelate toggles. That is a large deletion with its own design questions, and mixing it with a binding change makes both harder to review. This PR is the small step that leaves the retirement nothing but deletions
- The choice (errors, not new semantics) is recorded on the M4b item's struck-through entry with this PR's link
- Tests: each form raises the same error from `runtimePlan` and from both engines' execution. Gates: spec/basic on both planners, golden plans (expected unchanged), TPC-H, TPC-DS, the decorrelate and toggle tests, and engineJS/Native compile

## Alternatives and Why Not?

- Give `add count(*)` window semantics (`count(*) over ()`): it adds a language feature in a refactor PR, and the SQL and Wvlet compilers don't accept it either
- Keep `legacy(other)` until the retirement PR: then the retirement PR mixes binding changes with deletions
