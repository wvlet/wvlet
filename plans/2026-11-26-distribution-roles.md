# Distribution roles: npm as the default CLI, the JVM CLI as the full package (CLI unification phase 4)

## Goals

- A new user knows which distribution to install without reading a feature matrix: npm (`@wvlet/cli`) for interactive use, the JVM CLI for the server, ops, and JDBC features.
- A user who outgrows npm moves to the JVM CLI in a few documented steps, keeping their `.wv` folders, profiles, and command lines.
- A JVM-only command typed on npm says where to get it, instead of a generic unknown-command error.

## Background

- Phases 1–3 (#2529, #2531, #2537) made `wvlet` one command surface: npm and Native now have `run`, `repl`, `build`, `compile`, `to_wvlet`, and `skill`. The JVM CLI adds `ui`, `flow` (and its scheduler), `catalog import`, JDBC connectors such as Snowflake, and parallel builds with timeouts and run records.
- `usage/install.md` already has the npm-vs-JVM table and "Start with npm unless you need a JVM-only command"; `cli-reference.md`, `repl.md`, `model-pipelines.md`, and the bundled skill already tag JVM-only options.
- Homebrew stays on the JVM CLI. #2538 (open) ships JVM CLI packages with a bundled jlink'd Java runtime, so the formula can drop OpenJDK in a follow-up. This PR doesn't touch release packaging, the formula, or #2538's install-page lines; no Native binary is proposed.
- No released artifact is a Native `wvlet`: Native builds only `libwvlet` (wvc-lib) and an unreleased `wvc`.
- What is still stale or missing after phases 1–3:
  - `syntax/quick-start.md` says `wv` (the REPL) needs the JVM CLI and npm users should use `wvlet run`; npm has had the REPL since #2531.
  - `install.md` covers moving from Homebrew to npm, not from npm to the JVM CLI.
  - On npm, `wvlet ui`, `wvlet flow`, and `wvlet catalog` fail with the launcher's generic unknown-command error, which names no next step.
  - `for-ai-agents.md:26` says the skill ships in an "npm, Homebrew, native" CLI; there is no native distribution. Other "npm and Native `wvlet`" mentions describe the Native build target's behaviour and stay.
- The JVM CLI comes in a default package (Homebrew installs it; wvlet engine, Trino, remote servers) and `wvlet-cli-full` (adds DuckDB and Snowflake). npm bundles DuckDB, so an npm user who moves to Homebrew loses `-t duckdb`.

## Design

Lane: Standard — user-facing CLI messaging and install guidance. Always-gate: install paths (new migration steps in `install.md`); no packaging, release workflow, formula, or `wvlet upgrade` change.

User flow:

- **Who installs what.** Most users run `npm install -g @wvlet/cli` and get the full interactive CLI: `wvlet` (REPL), `run`, `build`, `compile`, with DuckDB bundled and no JDK. Users who run the Web UI, schedule flows, import catalogs from a database, query Snowflake or another JDBC source, or need parallel builds with run records install the JVM CLI: Homebrew or `wvlet-cli-<version>.tar.gz`, or `wvlet-cli-full-<version>.tar.gz` when they need DuckDB (`-t duckdb`) or Snowflake on the JVM.
- **Discovering the JVM CLI.** An npm user who types `wvlet ui` (or `flow`, `catalog`) sees: "`wvlet ui` is in the JVM CLI. Install it with Homebrew or a release package: https://wvlet.org/docs/usage/install#jvm-cli", and exit code 1.
- **Moving from npm to the JVM CLI.** A new "Moving from npm" section in `install.md`: `npm uninstall -g @wvlet/cli`, install the JVM CLI (the full package if they use `-t duckdb`), check `which wvlet`. The same `.wv` folders, `~/.wvlet` profiles, `target/warehouse` tables, and command lines keep working; the JVM CLI adds the `wv` alias. Keeping both installs is possible, but the first `wvlet` on `PATH` wins.
- **Moving back** is the existing "Moving from Homebrew" section, unchanged.

Decisions:

- `install.md`: open with a two-line "Which one?" (the who-installs-what above) before the table, describe the JVM CLI as the CLI for ui, flows and their scheduler, catalog import, JDBC connectors, and parallel builds with run records ("full package" stays the name of `wvlet-cli-full`), and add the "Moving from npm" section. #2538's platform-package lines are kept; this PR rebases over it if it merges first.
- `quick-start.md`: the tutorial starts the shell with `wvlet` (any distribution), mentioning `wv` as the JVM alias; the stale npm tip goes.
- npm and Native `wvlet`: `ui`, `flow`, and `catalog` exit with the message above. It lives in the shared `WvletCli` launcher path, so the JVM `WvletMain`, which has these commands, is untouched. `--help` doesn't change.
- `for-ai-agents.md` and the bundled skill: drop "native" from the distributions list; the skill's install bullets lead with npm as the default and name the JVM CLI's role.
- REPL examples with a `wv>` prompt (`engines/duckdb.md`, `engines/trino.md`) stay: the prompt is the shared REPL's on every distribution.
- An ADR records the distribution roles (npm default; JVM CLI full package, including Homebrew; no Native distribution for now) and supersedes adr/2026-11-18's "Homebrew and the release tarball remain the way to get the JVM-only commands (`wv` REPL, `wvlet build`, `flow`, `catalog`, `ui`)" for `build` (the REPL part was superseded by adr/2026-11-26-single-wvlet-command.md). The phase 4 line of plans/2026-11-26-cli-unification.md drops "possibly a Native binary for Homebrew".
- Tests: a shared CLI test that `ui`, `flow`, and `catalog` on `WvletCli` fail with the JVM CLI message; the existing skill snippet and freshness tests gate the docs.

## Alternatives and Why Not?

- **Docs-only PR, npm message as a follow-up:** the message is the one change a user hits by mistake rather than by reading, and it is a few lines plus a test.
- **Close phase 4 with no PR:** the install table is already right, but the npm unknown-command dead end, the stale quick-start tip, and the missing npm → JVM migration steps are user-facing gaps that this small PR closes.
- **Native `wvlet` for Homebrew:** out of scope by decision; Homebrew stays on the JVM CLI with a bundled runtime (#2538).
- **Stub `ui`/`flow`/`catalog` as real npm `@command`s:** they would need the JVM options to parse, and their `--help` would suggest the commands work there.
