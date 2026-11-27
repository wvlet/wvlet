# ADR: One wvlet command, with wv as an alias of wvlet repl

Date: 2026-11-26 · PR #2529

## Context

- Three entry points had grown: `wv` (`WvletREPLMain`, JVM), `wvlet` (`WvletMain`, JVM), and the npm and Native `wvlet` (`WvletCli` in `wvlet-cli-core`).
- The split dates from JLine (JVM-only) and DuckDB being the default executor. The REPL terminal now uses uni's `LineReader` (adr/2026-10-31-repl-uni-line-reader.md), and queries run on the cross-built wvlet engine by default (adr/2026-11-23-wvlet-engine-default.md).
- The two `wvlet` implementations had drifted. `-p`, `--format`, and `--host` existed only on npm. `--strict`, `--catalog`, and `--schema` on `compile`, and `-L`, `--debug`, and `--version`, existed only on the JVM. npm parsed `-l` and ignored it.
- npm is the primary install (adr/2026-11-18-npm-cli-distribution.md), but its users had no REPL.

## Decision

- **One command surface.** The `WvletCliCommands` trait in `wvlet-cli-core` defines `version`, `compile`, `to_wvlet`, `skill`, `run`, and `repl`. `WvletCliOptions.scala` defines their options. Each distribution implements only the hooks: how to compile, how to run locally, and whether it has the REPL. The JVM `WvletMain` adds `ui`, `build`, `flow`, and `catalog`.
- **Execution stays per platform.** The JVM runs queries through `QueryExecutor` with its JDBC connectors. npm and Native run them through `SqlConnectorProvider`. Remote wvlet servers (`-t wvlet --host`) are handled in the trait for every distribution. Their credentials come from the profile only, never from CLI flags.
- **Profile rule.** `-p` loads a profile and `-t` alone uses the engine's built-in profile. `-t` together with `-p` replaces the type of the profile's default engine. `--catalog`, `--schema`, and the connection flags override the default engine's settings.
- **Bare `wvlet` opens the REPL only on a terminal.** "Terminal" means `Console.isTerminal()` on JDK 22+, or a non-null `System.console()` before that. Without a terminal it prints the version, so scripts and agents never wait for input. With flags but no subcommand (`wvlet -p prod`), `wvlet` keeps its old behaviour; `wvlet repl -p prod` is the form that takes options.
- **`wv` stays as an alias of `wvlet repl`.** Removing it would break muscle memory and the Homebrew formula for no gain.
- **The REPL is JVM-only for now.** npm `wvlet repl` exits with a message that names the JVM CLI. Porting the REPL to shared code needs a script runner over `PlanExecutor`, cancellation without a thread pool, and async key actions in uni. That is a later phase (plans/2026-11-26-cli-unification.md).

## Consequences

- The same command line behaves the same in every distribution. JVM `run` gains `--format csv`, remote servers, and a non-zero exit on failed tests. npm `compile` gains `-p`, `--catalog`, `--schema`, and `--strict`.
- This supersedes adr/2026-11-18's statement that Homebrew and the release package are how to get the "`wv` REPL": they are how to get the REPL (`wvlet repl`) until the npm `wvlet` has it.
- `-t` with `-p` is no longer an error on the JVM `wvlet`. The REPL still rejects that combination.
- A new shared command goes in `WvletCliCommands`. A command that needs JVM-only modules goes in `WvletMain`.
