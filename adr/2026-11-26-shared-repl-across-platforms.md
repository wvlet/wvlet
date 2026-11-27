# ADR: Share the REPL across the JVM, Node.js, and Native, without query cancellation on Node

Date: 2026-11-26 · PR #2531

## Context

- The REPL (`WvletREPL`, its uni-based terminal layer, and the file completer) lived in the JVM-only `wvlet-cli` module, so the npm `wvlet` (the primary install) had no REPL. Phase 2 of [the CLI unification](../plans/2026-11-26-cli-unification.md) moves it to shared code.
- The JVM REPL runs each query on a thread pool so that Ctrl-C cancels just the query ([the line reader ADR](2026-10-31-repl-uni-line-reader.md)). That ADR listed async key actions in uni as a prerequisite for Node.js.
- On Node.js, every query is synchronous: DuckDB through koffi FFI, Trino over sync HTTP, and the in-process engine. While a query runs the event loop is blocked, so neither a SIGINT listener nor an Rx callback can run until it ends. For the same reason, uni's synchronous key actions are enough: a key chord's query runs inside the key handler.
- `Rx.await` throws on Scala.js, so the JVM's blocking read loop can't be shared as is.

## Decision

- The REPL, the terminal layer, the completer, and `clip` live in the shared `wvlet-cli-core`. The REPL talks to a `ScriptRunner`: the JVM keeps `WvletScriptRunner` (JDBC connectors, flows, item stores), and npm and Native use the cross-platform `PlanScriptRunner` over `PlanExecutor`. On the in-process engine, `PlanScriptRunner` compiles against the engine session's catalog, as `WvletEmbed` does.
- Platform hooks follow the `XXXCompat` pattern (`REPLCompat` / `REPLCompatImpl`):
  - The JVM and Native run each query on a thread, so Ctrl-C cancels just the query. The shared code lives in `wvlet-cli-core/jvm-native`.
  - Node runs queries on the caller and registers no SIGINT listener during a query, so Ctrl-C exits `wvlet`, as it does for `wvlet run`. A listener would only swallow the signal: it can't run until the query ends.
  - The JVM and Native read loop blocks with `await` on each read. On Node, reads are chained through callbacks, each starting on a later event-loop turn so the stack stays flat even when a read completes synchronously. The REPL closes its resources and sets the exit code when it ends, since the command returns first.
- No uni change was needed. The pager (`less`) and `pbcopy` get their text through a temp file and uni's cross-platform `IO.run`, instead of adding stdin input to uni's `ProcessConfig`.
- Bare `wvlet` on a terminal opens the REPL on every distribution (`WvletCliCommands.withDefaultCommand` on npm and Native).

## Consequences

- npm users get the same REPL as the JVM CLI, except: Ctrl-C during a query exits `wvlet`, and Snowflake and other JDBC connectors and flows need the JVM CLI (documented in `usage/repl.md`).
- Real cancellation on Node needs queries off the event loop (a worker thread holding DuckDB, connectors, and the engine session, or async connectors). That is a follow-up.
- `WorkEnv` resolves the home folder with uni's `IO.homeDirectory`, because `sys.props("user.home")` is unset on Node.js and put the REPL history under a `./null/` folder.
