# ADR: Cancel npm REPL queries with a Ctrl-C watcher and a cooperative flag

Date: 2026-11-26 · PR #2540

## Context

- The [shared REPL](2026-11-26-shared-repl-across-platforms.md) runs every npm query synchronously on Node's main thread: DuckDB through koffi FFI, the in-process engine, and Trino over uni's sync HTTP channel. While a query runs, no SIGINT listener can run, so Ctrl-C exited `wvlet` and lost the session's saved tables.
- Node delivers signals only to the main thread's event loop: `process.on("SIGINT")` in a worker never fires. So whatever notices Ctrl-C during a query must run outside the blocked main thread and outside Node's signal handling.
- The JVM and Native cancel by interrupting the query thread. Node can't abandon its only thread, so the query itself has to stop, and its session (engine tables, DuckDB connection, compiler, `use`) has to stay usable.

## Decision

- Queries stay on the main thread. A `CtrlCWatcher` worker thread spawns one helper `node` process per REPL session, in the terminal's foreground process group. Ctrl-C sends the helper the same SIGINT; it writes a byte to its pipe, which the worker (whose event loop is free) turns into a cancel request.
- The cancel flag is `QueryCancel`, a `SharedArrayBuffer` in `wvlet-lang` that both threads see. Stop points:
  - Engine source operators check it per batch.
  - Trino's `nextUri` loop checks it per page, then sends `DELETE` to abort the query.
  - A blocking `duckdb_query` publishes its connection's address; the worker calls `duckdb_interrupt` on it through its own koffi binding of the same libduckdb. The connection stays usable.
- The flag only counts while a query is armed. `REPLQueries.runThen` arms it around the query, not around showing the result, so Ctrl-C in `less` is never a cancel request. During an armed query a no-op SIGINT listener keeps Node from exiting; a pending signal is dropped once it is removed.
- A second Ctrl-C during the same query makes the worker end `wvlet` (SIGTERM), so a query without a stop point never traps the user.
- Without a TTY, or if the helper doesn't start, no listener is registered and Ctrl-C exits as before.
- JVM and Native are unchanged: their `QueryCancel.isRequested` is a constant `false`.

## Alternatives

- Run the runner in a worker thread, with the terminal on the main thread: every `ScriptRunner` call, result, progress line, pager call, and completer lookup would cross a message boundary, and the stop points would still be needed.
- Terminate a worker on Ctrl-C and rebuild the session: loses saved tables, which was the problem.
- Async (Rx) connectors and engine on Node: turns the sync `PlanExecutor` path into Rx everywhere; much larger.
- Raw mode during the query plus a worker polling `/dev/tty` for `\x03`: steals keystrokes from `less` and type-ahead.
- DuckDB's pending API (a `duckdb_pending_execute_task` loop checking the flag): keeps DuckDB on one thread but changes every DuckDB call, and `duckdb_prepare` can't take multi-statement SQL.

## Consequences

- npm users get the JVM's Ctrl-C behaviour. An engine query stops at its next source batch: a long phase between batches (a big in-memory sort or join after the scans) runs on until it pulls again, or until a second Ctrl-C.
- Long-running loops added to Node code paths need a `QueryCancel.check()` ([platforms](../design/platforms.md)).
- The npm REPL runs one extra `node` process (the helper) while a terminal session is open; it exits when its stdin pipe closes.
- Ctrl-C in the moment between a query's end and the next prompt still exits `wvlet`, as before.
- Follow-up: raise the flag from the JVM's Ctrl-C too, so an abandoned engine query stops instead of running on in the background.
