# Replace JLine in the wv REPL with uni's LineReader

## Goals

- Remove JLine (`org.jline:jline 4.4.8`) from wvlet so the REPL's terminal layer uses only cross-platform uni APIs (`wvlet.uni.cli.*`).
- Keep today's REPL experience: emacs editing, history file, Ctrl-R, file-path completion, syntax highlighting, multi-line statements, and the Ctrl-J chords.
- Unblock a later move of the REPL into a cross-built module, so the same code runs on the JVM, Scala Native, and Node.js.

## Background

- JLine is used in five places:
  - `JLine3Terminal`, the REPL's terminal, reader, key bindings, and history.
  - `WvletREPL`: `ReplParser` for multi-line input, `ReplHighlighter`, widgets, and catching `UserInterruptException`/`EndOfFileException`.
  - `LocalFileCompleter`.
  - `WvletScriptRunner.displayOutput`, which takes a JLine `Terminal` only to read its width.
  - An unused `OSInfo` import in `WvletServer`.
- `REPLTerminal` exists only to keep JLine out of headless (`-c`) runs. Half of its methods (`getBuffer`, `moveCursor`, `callWidget`, ...) exist so widgets can edit JLine's buffer.
- uni 2026.1.39 adds `LineReader`. Custom key actions receive a `LineEditor`, which provides the buffer, cursor, insert, accept, and `printAbove`.
- uni's JVM terminal needs Java 22+ (FFM). Older JVMs fall back to line mode without editing, and wvlet still supports JDK 17.

## Design

Lane: Standard. This changes interactive REPL behavior and the JVM baseline for line editing.

- Bump `UNI_VERSION` to 2026.1.39 in `build.sbt` and `project/plugin.sbt`.
- Replace `JLine3Terminal` with a terminal built on `LineReader` and `TerminalIO.default`, wired through Design so tests can bind `TerminalIO.inMemory`.
- Shrink `REPLTerminal`: drop the buffer and widget methods, because custom actions get the `LineEditor` directly. Headless mode stays as it is.
- Map each JLine feature to uni:
  - **Parser:** `ReplParser` becomes `withInputComplete(String => Boolean)`, with the same brace, triple-quote, `;`, and known-command rules.
  - **Highlighter:** `ReplHighlighter` returns an ANSI string instead of an `AttributedString`.
  - **Completer:** `LocalFileCompleter` implements `Completer.complete(buffer, cursor)`.
  - **History:** `withHistoryFile(cacheFolder/.wv_history)`. uni reads JLine's existing history files.
  - **Key bindings:** `KeyMap.emacs.unbind("^J")`, then bind `^J^A`, `^J^E`, `^J^R`, `^J^D`, and `^J^T` as `LineAction.Custom`.
  - **Continuation prompt:** a fixed gray `│` prompt with the width of `wv> `.
  - **Ctrl-C while a query runs:** `terminal.onInterrupt` interrupts the query thread.
  - **Ctrl-C at the prompt:** `LineResult.Interrupted` exits the REPL, as JLine's `UserInterruptException` does today (user decision).
- The read loop matches on `LineResult` (`Line`, `Interrupted`, `EndOfInput`) and blocks on each read with `.await`; see the stack-safety gap below.
- `displayOutput` takes the terminal width as an `Int`. JLine leaves `wvlet-runner`'s dependencies, and the unused `OSInfo` import goes away.
- Interactive editing requires Java 22+ (user decision: drop older JDKs for the REPL). On JDK 17–21, `wv` reads input in line mode and the docs say so. Headless `-c` runs still work on JDK 17.
- The `-c` command path and the `--enable-native-access` launcher option (already set for JDK 25) do not change.
- Tests replay keystrokes with `TerminalIO.inMemory` to cover multi-line input, the Ctrl-J chords, history, and completion.

### Gaps in uni (not worked around here)

- **`isInputComplete` has no cursor.** JLine's parser accepted a complete statement only when the cursor was at the end. With uni, Enter accepts it from anywhere in the buffer.
- **No auto-indentation.** JLine indented continuation lines by 2 after an unclosed `{`. uni has no equivalent setting.
- **Key actions cannot leave raw mode.** `^J^T` (subquery run) could open `less` for wide results from inside a key handler. With uni, it prints the result above the prompt, cut to the terminal width.
- **Key actions block input.** `^J^D` and `^J^T` run queries synchronously inside the action. That works on the JVM and Native but would stall Node.js, so the cross-platform move needs an async action API.
- **Recursive `flatMap` read loops are not stack-safe on the JVM.** Reads complete synchronously in `RxRunner`, so a loop like `examples/repl` overflows the stack after a few thousand inputs. wvlet blocks with `.await` per read until uni trampolines `flatMap`.
- **No clear-screen or set-size call on `TerminalIO`.** wvlet writes the ANSI clear sequence itself and uses 120×40 when the reported size is 0.

## Alternatives and Why Not?

- **Keep JLine on the JVM and use uni only for Native/JS:** this keeps two code paths, which defeats the goal.
- **Keep JLine for JDK < 22:** JLine stays a dependency and every terminal feature needs two implementations. Users of those JVMs are asked to upgrade instead.
- **An `stty` fallback in uni for JDK 17–21:** possible, but not worth maintaining just for JVMs we no longer support for the REPL.
- **Move the REPL into a cross-built module in this PR:** `WvletScriptRunner` and the REPL's thread executor are JVM-only. That needs its own PR.

