# Unify the wv and wvlet CLIs

## Goals

- Users learn one command, `wvlet`, and the same command line means the same thing whether it came from npm, Homebrew, or a release tarball.
- The interactive REPL is reachable from `wvlet` itself; `wv` keeps working as an alias.
- Later phases can move the REPL and `build` into shared code without another round of CLI-surface churn.

## Background

- Three entry points exist today: `wv` (`WvletREPLMain`, JVM), `wvlet` (`WvletMain`, JVM), and `wvlet` (`WvletCli` in `wvlet-cli-core`, shipped on npm, linked into the Native `wvc`).
- The split came from JLine (JVM-only) and DuckDB being the default executor. Both reasons are gone: the REPL terminal uses uni's `LineReader` (adr/2026-10-31-repl-uni-line-reader.md), and `run` and the REPL default to the cross-built wvlet engine.
- The two `wvlet` implementations drifted: `-p` exists only on npm, `--format`/`--host`/`--https` only on npm, `--strict`/`--catalog`/`--schema` on `compile` and `-L`/`--debug`/`--version` only on the JVM, and npm's `-l` is parsed but ignored.
- npm is the primary install (adr/2026-11-18-npm-cli-distribution.md), yet its users have no REPL; porting the REPL needs the JVM-only `WvletScriptRunner`/`QueryExecutor` and thread-pool cancellation replaced, which is phase 2.

## Design

Lane: Standard (user-facing CLI behaviour); the direction was approved in chat with merge authorization.

Phases:

- **Phase 1 (this PR): one command surface.**
- **Phase 2:** the REPL in shared code (script runner over the shared `PlanExecutor`, Rx-based cancellation, uni async key actions), so the npm `wvlet` gets the REPL.
- **Phase 3:** `build` in shared code where its item store allows; JDBC-backed `catalog import`, `ui`, and the flow scheduler stay JVM-only.
- **Phase 4:** reposition distributions: npm as the full interactive CLI, the JVM tarball as the server/ops package, and possibly a Native binary for Homebrew.

Phase 1 decisions:

- The shared commands (`version`, `compile`, `to_wvlet`, `skill`, `run`) are defined once in `wvlet-cli-core` as a trait. npm/Native and the JVM CLI implement only the platform hooks: how to compile, and how to execute locally.
- The JVM `wvlet` extends that surface with its JVM-only commands: `ui`, `build`, `flow`, `catalog`, and a new `repl` subcommand.
- One option set in `wvlet-cli-core`, used by both:
  - Global: `-l,--loglevel`, `-L`, `--debug`, `--version`.
  - `compile`/`to_wvlet`: `-w`, `-f`, `-t`, `-p,--profile`, `--catalog`, `--schema`, `--strict`.
  - `run`: the compile options plus `--format box|csv`, `--host`, `--port`, `--user`, `--https`.
- `--profile` keeps working; `-p` is added as its short form everywhere (JVM `wvlet`, `wvlet repl`, `wv`).
- JVM `run` gains npm's behaviour: `--format csv`, remote execution on a wvlet server with `-t wvlet --host`, and a non-zero exit when a `test` fails or a statement errors.
- npm `compile -p` uses the profile's default engine for the dialect and its catalog/schema, so no shared option is parsed and ignored.
- Remote execution keeps its credential rule on every platform: the server token and password come from the profile only (with `${ENV}` interpolation), never from CLI flags. This touches the credentials always-gate area; it reuses npm's existing path unchanged.
- `--version` exits 0 everywhere: the shared mains treat `EXIT_SUCCESSFULLY` as success instead of printing it as an error.
- Execution engines stay per platform: the JVM keeps `QueryExecutor` with all JDBC connectors; npm/Native keep `SqlConnectorProvider`.
- Bare `wvlet` (no arguments at all) on an interactive terminal opens the REPL, like `duckdb` or `python`. "Interactive" means `Console.isTerminal()` on JDK 22+ (stdin and stdout are a terminal), or a non-null `System.console()` on older JDKs. Without a terminal (pipes, CI, agents) it prints the version, as today, so scripts don't change. `wvlet -p prod` with flags but no subcommand keeps today's behaviour; `wvlet repl -p prod` is the form with options.
- On JDK 17–21 the REPL opens without line editing, as `wv` does today (adr/2026-10-31).
- `wvlet repl [-c ...] [-f ...] [-p ...]` is the explicit form. `wv` remains a launcher of the same REPL, and the docs present it as an alias.
- npm's bare `wvlet` keeps printing the version until phase 2, and npm `wvlet repl` exits with a message that names the JVM CLI instead of "Unknown command".
- Docs (`usage/cli-reference`, `usage/repl`, `usage/install`, `for-ai-agents.md`), the bundled skill, and AGENTS.md's CLI line describe `wvlet` as the single command and mark `repl` as JVM-only for now. Stale help is fixed on the way: `-t` defaults to `wvlet`, not `duckdb`; `--host` also addresses a wvlet server; `--format` is `box` or `csv` (no `json`).
- An ADR records the single-command decision, the bare-`wvlet` TTY rule, and `wv` as an alias, and notes the change to adr/2026-11-18's statement that Homebrew is how to get the `wv` REPL.
- Tests cover the shared option parsing on both surfaces, JVM `run --format csv`, the non-TTY bare `wvlet`, and `wvlet repl -c`.

## Alternatives and Why Not?

- Make JVM `run` call npm's `SqlConnectorProvider` path: it would drop Snowflake, flows, and model item stores on the JVM.
- Drop `wv` now: it breaks muscle memory and the Homebrew formula for no gain; an alias costs nothing.
- Make bare `wvlet` always open the REPL: it would hang scripts and agents that call `wvlet` to check that it is installed.
