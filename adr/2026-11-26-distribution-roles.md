# ADR: npm is the default wvlet CLI, and the JVM CLI is the server and ops package

Date: 2026-11-26 · PR #TBD

## Context

- Phases 1–3 of plans/2026-11-26-cli-unification.md gave the npm `wvlet` the same command surface as the JVM CLI for interactive use: `run`, `repl`, `build`, `compile`, `to_wvlet`, and `skill` (#2529, #2531, #2537).
- The JVM CLI still alone has `ui`, `flow` and its scheduler, `catalog import`, the JDBC connectors such as Snowflake, and parallel builds with timeouts and run records.
- adr/2026-11-18-npm-cli-distribution.md said Homebrew and the release tarball "remain the way to get the JVM-only commands (`wv` REPL, `wvlet build`, `flow`, `catalog`, `ui`)". adr/2026-11-26-single-wvlet-command.md already superseded the REPL part; phase 3 superseded `build`.
- The unification plan left open a Native `wvlet` binary for Homebrew. Homebrew stays on the JVM CLI instead, with a bundled Java runtime (#2538).

## Decision

- **npm is the default.** The install page, the quick start, and the bundled skill send new users to `npm install -g @wvlet/cli`, and use `wvlet` (not `wv`) to start the shell.
- **The JVM CLI is the server and ops package.** It is what Homebrew and the release packages install, for `ui`, flows, catalog import, JDBC connectors, and parallel builds with run records. `wvlet-cli-full` stays the name of the package with DuckDB and Snowflake.
- **No Native distribution for now.** Scala Native builds `libwvlet` for the language bindings; no `wvlet` binary is released.
- **JVM-only commands name the JVM CLI.** On npm and Native, `wvlet ui`, `flow`, and `catalog` exit 1 with a message that links to the JVM CLI's install section, instead of the launcher's unknown-command error. The command name is the first argument after the global options, so `wvlet run "ui"` is unaffected.
- **Moving between them is documented both ways.** `install.md` has "Moving from Homebrew" and "Moving from npm". Projects, profiles, built tables, and command lines carry over; only the first `wvlet` on `PATH` runs.

## Consequences

- An npm user who moves to the Homebrew package loses the bundled DuckDB; the migration steps point DuckDB and Snowflake users to `wvlet-cli-full`.
- A new JVM-only command must be added to `WvletCli.jvmOnlyCommands`.
