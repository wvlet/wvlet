# The REPL in shared code (CLI unification phase 2)

## Goals

- npm and Native `wvlet` users get `wvlet repl`, and bare `wvlet` on a terminal opens the REPL, as on the JVM CLI (phase 2 of plans/2026-11-26-cli-unification.md).
- One REPL implementation: commands, key chords, highlighting, multi-line input, history, and completion behave the same on every distribution.
- The JVM REPL keeps its behaviour, including Ctrl-C cancelling a running query.

## Background

- The terminal layer (`REPLTerminal`, `UniTerminal`, `HeadlessTerminal`) and the REPL loop already use only `wvlet.uni.cli`, but live in the JVM-only `wvlet-cli` module (adr/2026-10-31-repl-uni-line-reader.md).
- The JVM-only pieces the REPL calls are: `WvletScriptRunner` over `QueryExecutor` (JDBC connectors, flows, item stores), the thread pool that runs each query so Ctrl-C can cancel it, the `less` pager, and `pbcopy` for `clip`.
- npm and Native already run queries through the cross-platform `PlanExecutor` over `SqlConnectorProvider` (in-process wvlet engine, DuckDB, Trino), and `QuerySelector` and `ExecutionPlanner` are shared, so describe (`ctrl-j ctrl-d`) and subquery runs (`ctrl-j ctrl-t`) can be planned the same way.
- Every query on Node.js is synchronous: DuckDB through koffi FFI, Trino over sync HTTP, and the in-process engine. While a query runs, the event loop is blocked, so no SIGINT listener or Rx callback can run until it ends.
- For the same reason, uni's synchronous key actions are enough on Node: a chord's query runs inside the key handler and returns its result. The uni gap "key actions run synchronously" does not block this phase, so no uni PR is needed.
- On Node.js, `Rx.await` throws, so the JVM's blocking read loop can't be shared as is, and the command returns before the REPL ends: today the JVM builds the REPL in a DI session that closes its runner on return.
- Scala Native has threads (the engine's task runner uses them), so Native can keep the JVM's run-on-a-thread cancellation.
- The shared parts still use `java.io.File` (history path, file completer, `-f` input), which Scala.js lacks.

## Design

Lane: Standard — new user-facing behaviour on npm and Native. The investigation changes the expected cancellation direction (no Rx cancellation, no uni PR), so the card confirms it.

User flow (npm):

- The user installs `@wvlet/wvlet` and runs `wvlet` (or `wvlet repl -p prod`) in a terminal; the `wv>` prompt opens with editing, history in `.wv_history`, highlighting, and the Ctrl-J chords.
- Without a terminal (pipes, CI, agents), bare `wvlet` still prints the version; `wvlet repl -c '...'` and `-f file` run headless and exit, as on the JVM.
- `exit`, `quit`, Ctrl-D, or Ctrl-C at an idle prompt leave the REPL.

Decisions:

- Move `WvletREPL`, the terminal layer, and the local file completer into the shared `wvlet-cli-core`. The REPL talks to a small runner interface: run a statement with a query selection, render and display a result, row limit, column width, and the current catalog/schema.
- A new cross-platform runner implements that interface over one long-lived compiler and `PlanExecutor`, so `use` and in-process engine tables persist (like the JVM REPL, `val`s do not carry over between inputs) across statements.
  - On the in-process engine it compiles against the engine session's catalog, as `WvletEmbed` does, so saved tables type-check and describe works; on DuckDB and Trino it compiles with the profile's dialect without live metadata, like npm `wvlet run`.
  - `PlanExecutor` gains statement selection (for the describe and subquery chords) and a settable row limit (for `rows`).
- The JVM `wv` and `wvlet repl` plug in an adapter over today's `WvletScriptRunner`, so JDBC connectors, flows, and item stores stay unchanged there.
- Running a query goes through a platform hook:
  - The JVM and Native run each query on a thread, so Ctrl-C cancels just the query (JVM behaviour unchanged).
  - Node runs the query on the caller with no SIGINT listener, so Ctrl-C during a query exits `wvlet`, as it does for `wvlet run`. Chord queries still leave raw mode so Ctrl-C arrives as a signal. True cancellation on Node needs async connectors or a worker thread: a follow-up.
- The read loop is a platform hook: the JVM and Native block on each read as today. Node chains reads with Rx, starting each read on a later event-loop turn so the chain stays stack-safe even after stdin ends.
- On Node the REPL owns its lifecycle: it closes the runner and connectors when the loop completes, and reports errors and the exit code from there.
- Shared code reads paths through uni's `IO` instead of `java.io.File`.
- Bare `wvlet` with no arguments on a terminal opens the REPL on npm and Native too, using uni's `TerminalIO.isTTY`.
- Wide results page through `less -FXRSn` and `clip` uses `pbcopy` on every platform, by writing the text to a temp file and running the command through uni's cross-platform `IO.run`. Where the command is missing, the result is cut to the terminal width, and `clip` warns, as today on non-macOS.
- `git` and `gh` commands already go through `IO.run` and work everywhere.
- Docs drop "JVM CLI only" for the REPL: `usage/repl.md`, `usage/cli-reference.md`, the `usage/install.md` table, `for-ai-agents.md`, the bundled skill (`references/cli.md`), and `ARCHITECTURE.md`. `usage/repl.md` states what the npm/Native REPL lacks: Snowflake and other JDBC connectors, flows, and (npm) query cancellation.
- Tests: the existing JVM REPL tests keep passing unchanged; new shared tests drive the REPL with `TerminalIO.inMemory` on the JVM and Node, cover `-c` runs, saved tables across statements, describe and subquery chords, and bare `wvlet` without a terminal on npm.

## Alternatives and Why Not?

- Rx-based cancellation on Node, as first expected: queries block the event loop, so a Ctrl-C callback can't run until the query has finished.
- Run Native queries on the caller like Node: Native has threads, so the JVM's cancellation works there unchanged.
- Worker threads for npm queries: they need DuckDB, connectors, and the engine session to live in the worker, a larger change better done separately.
- Swallow Ctrl-C during an npm query (keep the REPL alive, cancel nothing): a hung Trino query would then leave no way out but killing the terminal.
- Add stdin input to uni's `ProcessConfig` for the pager and `pbcopy`: cleaner, but it needs a uni release first for two call sites; a temp file works today.
- Replace the JVM runner with the cross-platform one: it would drop Snowflake, flows, and model item stores from the JVM REPL.
