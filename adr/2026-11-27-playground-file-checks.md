# ADR: Run every test and debug statement of the file in the playground

Date: 2026-11-27 · PR #2539

## Context

- The playground runs on the in-browser engine, which binds only the child of `test` and `debug`. A failing assertion showed nothing, and the docs listed `test` as unsupported.
- A run executes one statement: a newly opened example runs its last statement, and an edit runs the statement at the cursor. Most `test` lines in a file belong to other statements.
- The runner already evaluates assertions without engine access (`TestEvaluator` over `TableRows`), and `ExecutionPlanner` with `isDebugRun` orders debug plans, queries, and tests.

## Decision

- Every run plans the whole file with `isDebugRun` and checks all `test` and `debug` statements, as `wvlet test` does (`PlaygroundChecks`). A query runs only when a test or debug block reads its result, and the selected statement's own result is reused by its tests.
- Assertions go through the runner's `TestEvaluator`, so the playground depends on `runner.js`. Engine results become `TableRows` through `EngineTableRows`, shared with `WvletEngineSession`.
- Test results go in a **Tests** tab next to **Result**; debug rows go in a **Debug** tab of the right panel, which opens when a file with debug blocks is opened. Both link to source lines, and the editor gutter marks test and debug lines.
- A failing check never replaces the main result or sets the run's error. While an edit doesn't compile, the last checks stay.

## Consequences

- Checks re-run on every auto-run (400 ms after typing stops). That is cheap on the default SF 0.01 tables; at SF 0.1, a file with many tested queries runs each of them per edit.
- Rejected: checking only the selected statement (most markers stay blank), a playground copy of `TestEvaluator` (messages drift from the CLI), and one bottom tab for tests and debug rows (debug tables belong beside the plans and SQL they explain).
