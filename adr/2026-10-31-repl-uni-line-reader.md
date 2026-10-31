# ADR: The wv REPL uses uni's LineReader instead of JLine

Date: 2026-10-31 · PR #2158

## Context

The `wv` REPL used JLine for line editing, history, completion, highlighting, and the Ctrl-J key chords. JLine is JVM-only, so the REPL could not move to the cross-built (JVM, Scala Native, Node.js) runner. uni 2026.1.39 added `wvlet.uni.cli.LineReader`, a cross-platform line editor covering the features the REPL uses. On the JVM it reaches libc through the FFM API, which needs Java 22 or later; wvlet itself still supports JDK 17.

## Decision

- The REPL's terminal layer (`UniTerminal`) uses only `wvlet.uni.cli` APIs, and JLine is no longer a dependency. Multi-line input is `withInputComplete`, highlighting returns ANSI text, completion is a uni `Completer`, the chords are `LineAction.Custom`, and history stays in `.wv_history` (uni reads JLine's format).
- Line editing needs Java 22+. On JDK 17–21 `wv` reads input line by line; we chose not to add an `stty` fallback to uni for older JVMs.
- Ctrl-C at an idle prompt still exits the REPL, as it did with JLine. Ctrl-C during a query, including one started by `ctrl-j ctrl-d` or `ctrl-j ctrl-t`, cancels only the query: key-binding queries leave raw mode while they run, so Ctrl-C arrives as a signal.
- The read loop blocks on each read with `.await`. A recursive `Rx.flatMap` loop is not stack-safe on the JVM, where reads complete synchronously, and it overflowed after a few thousand inputs.

## Consequences

- Users on JDK 17–21 lose interactive editing in `wv`. Headless runs (`-c`, `-f`) are unaffected.
- uni gaps that wvlet accepts rather than works around:
  - `isInputComplete` gets no cursor, so Enter mid-buffer runs a complete statement.
  - There is no auto-indent after `{`.
  - A key binding cannot open a pager, so `ctrl-j ctrl-t` prints above the prompt instead of opening `less`.
  - Key actions run synchronously.
  - `flatMap` loops are not stack-safe.
  - `TerminalIO` has no clear-screen or set-size call.
- Moving the REPL into a cross-built module still needs the runner and the query thread pool to work off the JVM, and needs async key actions in uni for Node.js.
