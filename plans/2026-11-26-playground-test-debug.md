# Show `test` and `debug` results in the playground

## Goals

- Let playground users see what `test` assertions and `debug { ... }` blocks do: pass/fail per assertion and the intermediate rows of each debug block.
- Link every result to its source line, so a failing assertion or a debug block is one click from the code.
- Add a "Testing & Debugging" example category, so the syntax is discoverable from the example library.

## Background

- The playground runs on the in-browser engine, which skips `test` and `debug`: `RuntimeBinder` binds only their child, and `PlaygroundEngine` compiles with `withDebugRun(false)`. A failing assertion shows nothing today.
- The only example with `test` is `12_dbt_models/05_data_tests.wv`, whose comment says the tests are skipped in a normal run. No example uses `debug`.
- The runner already evaluates tests without engine access: `TestEvaluator` reads a materialized `TableRows` (`_.size`, `_.columns`, `_.rows`, `_.output`, `_.json`), and `ExecutionPlanner` with `isDebugRun` orders the debug plans, the query, and the trailing tests. Both run on Scala.js.
- The playground already links results to code: the lineage view and the plan view call `state.revealLine`.

## Design

Lane: Standard — a new panel and new run behavior. The user's request ("do both in one PR", a panel that links to code) approves the direction; the prototype gate decides the look.

- **Run semantics:** every run checks all `test` and `debug` statements in the file, like `wvlet test` does, and still shows the selected statement's rows in the Result tab. A newly opened example runs only its last statement, and typing runs only the statement at the cursor, so checking only the selected statement would leave most test lines blank.
  - The file is planned with `isDebugRun = true`. A statement's query runs on the engine only when a `test` or `debug` reads its result. Each `test` is checked against that result, and each `debug` block runs its own query and nested tests against its input.
  - Checks run on every auto-run (400 ms after typing stops). The engine runs on in-memory TPC-H tables that are 10 MB at the default scale factor, so this stays fast.
- **Reuse, not a copy:** the playground calls the runner's `TestEvaluator` on raw engine values converted to `TableRows`. The conversion is a shared runner helper, so `wvlet` on the engine and the playground build the same rows. Messages and semantics match `wvlet test` and spec files.
- **Failures stay local:** a failing assertion or a debug query the engine can't run is shown as an entry in the panel. It never replaces the main result or sets the run's error, and the status line adds `· 1 test failed`.
- **Tests tab:** the bottom panel gets two tabs, **Result** and **Tests**. The Tests label shows a count (`1 failed` in red, or `3 passed`). Each entry shows ✓/✗, the assertion line, and the evaluator's message (`was equal to` / `was not equal to`, expected vs. actual), in source order.
- **Debug tab:** the right panel gets a **Debug** tab next to SQL/Plan/Engine/Lineage. Each `debug` block shows its full source and its rows, in source order. Opening an example or a link with debug blocks shows this tab, and so does an edit that adds the file's first debug block.
- **Line links:** clicking an entry's line number reveals and selects that line in the editor.
- **Gutter markers:** the editor shows ✓/✗ next to each test line and ◆ on each `debug` line, with the message on hover. The Monaco facade gets a line-decoration call for this.
- **Examples:** a new `13_testing` category ("Testing & Debugging") over TPC-H tables: schema tests, row tests, output and JSON tests, comparisons (`should contain`, `>`), `debug` inside a pipeline, and one assertion that fails on purpose to show a failure. The dbt data-tests example's comment drops "skipped in a normal run".
- Docs: `website/docs/usage/playground.md` describes the new tab and drops "`test` assertions don't run in the playground" from Limitations.
- Tests cover the engine-side evaluation (pass, fail, debug results, line numbers) in `PlaygroundEngineTest`, plus a check that every bundled example's tests pass, except the one that fails on purpose.

## Alternatives and Why Not?

- **Copy `TestEvaluator` into the playground.** Two copies drift; messages would differ from `wvlet test`.
- **Show results only as editor markers.** Long expected/actual values and debug tables don't fit in a hover.
- **Auto-switch to the tab when a test fails.** It would flicker while the user types a half-finished assertion.

## Preferences learned

- Context: where debug rows show. Chose: one bottom "Tests & Debug" tab for both. User chose: debug tables in the right panel, which opens when a file has debug blocks. Principle: show intermediate data next to the code and plans it explains, and give the bottom panel to the statement's own result.
- Context: debug entry header. Chose: the block's first line (`debug {`). User chose: the whole block. Principle: a label must identify its subject on its own.
