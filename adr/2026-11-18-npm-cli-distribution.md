# ADR: npm is the primary distribution channel for the wvlet CLI

Date: 2026-11-18 · PR #TBD

## Context

- The Homebrew formula installs the JVM `wvlet-cli` package and pulls in OpenJDK, which makes installs slow and heavy. Linux and Windows users had to download a tarball and set up a JDK themselves.
- The Scala.js build of `wvlet-cli-core` (`@wvlet/cli` on npm) covers `version`, `compile`, `to_wvlet`, and `run`. Its DuckDB backend loads `libduckdb` through koffi, but found the library only on system paths, so `run` still needed a separate DuckDB install.
- DuckDB publishes prebuilt `libduckdb` shared libraries as `@duckdb/node-bindings-<os>-<arch>[-musl]` npm packages for macOS, Linux (glibc and musl), and Windows on x64 and arm64.

## Decision

- **npm first.** The install docs lead with `npm install -g @wvlet/cli`. Homebrew and the release tarball remain the way to get the JVM-only commands (`wv` REPL, `wvlet build`, `flow`, `catalog`, `ui`), and the release workflow keeps updating the formula.
- **Bundled DuckDB through optional dependencies.** `@wvlet/cli` lists every `@duckdb/node-bindings-*` platform package as an optional dependency, so npm installs only the host's. The entry script passes that library to the Scala.js bundle in `WVLET_BUNDLED_LIBDUCKDB`; `WVLET_LIBDUCKDB` still takes precedence, and system paths are the fallback. No postinstall download: install scripts are often disabled, and optional platform packages are the npm-native way to ship native code.
- **Version lock.** The libduckdb C API is used directly, and its struct layouts change across DuckDB releases. A test fails when the npm bindings and `DUCKDB_JDBC_VERSION` are on different DuckDB releases.
- **Update notice, not self-replacement.** The CLI prints a one-line notice from a cached registry lookup (`~/.wvlet/update-check.json`, refreshed at most daily by a detached process, so no command waits on the network). `wvlet upgrade` delegates to the owning package manager (`npm install -g --prefix <this prefix>`, `pnpm add -g`, or `bun add -g`). Replacing files in place would fight the package manager's ownership of the install. Only the `latest` dist-tag is compared, so pre-releases never trigger the notice.
- **On by default, quiet where it matters.** The check is skipped in CI, when stderr is not a terminal, for npx runs and dev builds, and when `WVLET_NO_UPDATE_CHECK` is set.
- **Release gate.** The packed tarball is installed globally and run (`compile` and a DuckDB `run`) on Linux, macOS, and Windows on every relevant PR and before every npm publish.

## Consequences

- A JDK is needed only for the JVM-only commands.
- The npm install is about 70 MB larger per platform because of the bundled libduckdb.
- Bumping `duckdb_jdbc` now also requires bumping the `@duckdb/node-bindings-*` versions in `sdks/cli-node/package.json`.
- `upgrade` exists only in the npm distribution. It is handled by the Node entry script, not the Scala launcher, and is listed under `[npm commands]` in `--help`.
- Homebrew users who switch must `brew uninstall wvlet`, or two `wvlet` commands end up on PATH.
