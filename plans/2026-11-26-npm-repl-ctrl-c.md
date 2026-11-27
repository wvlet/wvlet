# Let Ctrl-C cancel a running query in the npm REPL

## Goals

- In the npm (Node.js) `wvlet` REPL, Ctrl-C during a query stops that query and returns to the prompt, as it does on the JVM and Native. Today it exits `wvlet`, which loses the session's saved tables, `use` context, and history position.
- The session survives a cancel: tables saved in the engine session, the DuckDB connection, and the compiler state stay usable for the next query.
- JVM and Native behaviour stays unchanged.

## Background

- Every Node query runs synchronously on the main thread: DuckDB through koffi FFI (`duckdb_query`), the in-process engine, and Trino over uni's `NodeSyncHttpChannel` (a fetch in a short-lived worker plus `Atomics.wait` on the main thread). While the main thread blocks, no SIGINT listener or Rx callback can run, so Phase 2 (#2531) registers none and Ctrl-C kills the process.
- Node delivers signals only to the main thread's event loop; `process.on("SIGINT")` in a worker never fires. So whatever notices Ctrl-C during a query must run outside the blocked main thread and outside Node's signal plumbing.
- The JVM "cancels" by interrupting the query thread and returning to the prompt. The engine has no cancel check, so on the JVM an engine query keeps running in the background after Ctrl-C. On Node we can't abandon the main thread, so the query itself has to stop.
- Stop points that already exist or are cheap:
  - DuckDB has `duckdb_interrupt(connection)`, made to be called from another thread while `duckdb_query` runs; the connection stays usable.
  - `TrinoQueryHandle` already has `cancel()`: the `nextUri` loop exits at the next page and sends `DELETE`.
  - The engine pulls batches through operators; a flag check per batch costs one atomic load.
  - Each Trino page fetch blocks in uni's `NodeSyncHttpChannel` (`Atomics.wait` until the fetch returns), and Trino long-polls about a second per page, so the `nextUri` loop reaches a check point about once a second.

## Design

Lane: Standard — changes user-visible REPL behaviour on npm and adds a helper process and thread to the npm CLI.

Feasibility (probed in plain Node before writing this): with the main thread busy, a terminal `\x03` reached the helper child and the worker raised the flag within the second; `duckdb_interrupt` called from a worker through koffi stopped a running `duckdb_query` (`INTERRUPT Error`) and the connection answered the next query.

User flow:

- The user runs a long query in `wvlet` (npm), e.g. a big engine scan or a Trino query, and presses Ctrl-C.
- The query stops within about a batch or a Trino page, `Cancelled the query` is logged, and the prompt returns, as on the JVM.
- Tables saved earlier in the session still answer queries. Ctrl-C at an idle prompt still exits, as today.
- If the query ignores the cancel (a single long native call that has no stop point), a second Ctrl-C during the same query exits `wvlet`, as today, so there is always a way out.

Decisions:

- Keep queries on the main thread and keep the REPL, `ScriptRunner`, and session state where they are. Add a small Ctrl-C watcher that runs while the main thread is blocked:
  - A worker thread spawns one helper child (`process.execPath`) per REPL session, lazily on the first query. The child exits when its stdin pipe closes, so it never outlives `wvlet`, and the REPL kills it on close. The child is in the terminal's foreground process group, so it receives the same SIGINT as `wvlet`; it writes a byte to its pipe. The worker's event loop is free, so it sees the byte and raises the cancel flag.
  - The main thread registers a SIGINT listener during each query only so Node doesn't exit; the watcher does the work. The listener can't run while the query blocks, so on a second signal for the same query the watcher ends the process itself.
  - The watcher is armed only while the query body runs, not while the result is displayed or paged, so Ctrl-C inside `less` never counts toward the exit.
  - This reacts to the real signal, so it doesn't fight `less` or the line editor for keystrokes, and key-chord queries keep using `withInterruptSignal`.
- The cancel flag is a `SharedArrayBuffer` slot behind a small cross-platform `QueryCancel` token in `wvlet-lang`. Stop points check it:
  - DuckDB: the watcher calls `duckdb_interrupt` with the connection's address through its own koffi binding of the same `libduckdb`.
  - Engine: operators check the token per batch and throw a cancel error.
  - Trino: the main thread checks the token in the `nextUri` loop after each page and calls `TrinoQueryHandle.cancel()`, which sends `DELETE`. The in-flight fetch still finishes first; waking it early would need a uni change, which this PR doesn't make.
  - The per-batch engine check is not behind an `Optimization` toggle: on the JVM and Native `QueryCancel.isRequested` is a `final def` returning `false`, which the JIT and the Native optimizer fold away, so the engine benchmarks (JVM) see no change. (Revised during implementation: the note first planned a benchmark against origin/main.)
- The REPL's Node `REPLQueries` turns the cancel error into `None`, as `ThreadQueries` does with `CancellationException`, and resets the flag before each query.
- JVM and Native keep `ThreadQueries` unchanged; they never raise the token, so the new checks are no-ops there. Wiring the token into the JVM's Ctrl-C (to stop abandoned engine work) is a follow-up.
- No uni change: the watcher uses Node's `worker_threads` and `child_process` directly from `wvlet-cli-core/.js`, as uni's `NodeSyncHttpChannel` does. Windows has no foreground-group SIGINT for a child in the same way; if the helper can't start, Ctrl-C keeps today's behaviour.
- Tests: a unit test that a raised token stops an engine query and the session still answers the next one; the pty check (`node sdks/cli-node/bin/wvlet.js repl`: save a table, run a long query, send `\x03`, then query the saved table) on DuckDB and the engine. Trino is checked against a local Trino container if one starts; otherwise the PR says it wasn't. Docs: `usage/repl.md`, `design/platforms.md`, and a new ADR; the shared-REPL ADR's Consequences line about worker threads gets a pointer to it.

## Alternatives and Why Not?

- **Run the runner in a worker thread, terminal on the main thread.** Main stays free for a normal SIGINT listener, but every `ScriptRunner` call, `QueryResult`, progress line, pager call, and completer lookup would cross a message boundary. A large refactor for the same stop points.
- **Terminate the worker on Ctrl-C and rebuild the session.** Loses saved tables, which is the main reason not to exit today.
- **Make the connectors and engine async (Rx) on Node.** Frees the event loop, but turns the sync `PlanExecutor` path into Rx across DuckDB, Trino, and the engine. Much larger, and the engine would still need stop points.
- **Raw mode during the query plus a worker polling `/dev/tty` for `\x03`.** No child process, but it steals keystrokes from `less` (which runs inside the query region) and type-ahead, and needs care to stop reading after the query.
- **DuckDB's pending API (`duckdb_pending_execute_task` loop checking the flag) instead of `duckdb_interrupt`.** Keeps DuckDB in one thread but changes every DuckDB call path, including multi-statement queries `duckdb_prepare` can't take. Fallback if koffi in a worker proves unreliable.
