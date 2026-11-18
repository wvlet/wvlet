# npm as the primary install and update channel for the wvlet CLI

## Goals

- Install wvlet with a single `npm install -g @wvlet/cli` and no JDK, Homebrew, or system DuckDB.
- `wvlet run` against DuckDB works right after that install, on macOS, Linux, and Windows.
- Users learn about new releases from the CLI itself and upgrade with one command.
- Homebrew stops being the recommended path; its `openjdk` dependency makes installs slow and heavy.

## Background

- `@wvlet/cli` is already published on every release tag (latest 2026.3.0). It is the Scala.js build of `wvlet-cli-core`: `version`, `compile`, `to_wvlet`, and `run` (DuckDB, Trino, remote wvlet server). Its README still says `run` is unavailable.
- 2026.3.0 has no update check, so its users must reinstall once by hand; the notice works from the next release on.
- The Node build runs DuckDB through koffi + the libduckdb C API, but finds the library only via `WVLET_LIBDUCKDB` or system paths (`/opt/homebrew/lib`, `/usr/local/lib`, ...). So `run` on a fresh npm install fails unless the user separately installs DuckDB, typically with Homebrew again.
- DuckDB's official `@duckdb/node-bindings-<os>-<arch>` packages ship a prebuilt `libduckdb` shared library (≈66 MB unpacked on darwin-arm64) and are published for darwin, linux (glibc and musl), and win32 on x64 and arm64.
- The Homebrew formula (`wvlet/homebrew-wvlet`) installs the JVM `wvlet-cli` tarball and pulls in `openjdk`. It also provides JVM-only features the Node build lacks: the `wv` REPL, `wvlet ui`, `wvlet build`, `wvlet flow ...`, and `wvlet catalog import`.
- The docs install page and `website/docs/for-ai-agents.md` lead with Homebrew; the `@wvlet/cli` README is the only place npm install is mentioned.
- The JVM build uses DuckDB JDBC `2.0.0.1`; the matching npm bindings are `2.0.0-r.1`. The koffi binding's `duckdb_result` layout and chunk API are sensitive to the DuckDB version.

## Design

Lane: Standard — changes packaging and adds an auto-update notice, both always-gate areas.

User flow:
- New user: `npm install -g @wvlet/cli` → `wvlet run -f q.wv` works immediately against a local DuckDB; no other installs.
- Release ships: on a later run, after the command output, stderr shows one line, for example `wvlet 2026.12.0 is available (current 2026.11.0). Run 'wvlet upgrade' to update.`
- `wvlet upgrade` runs the package manager's global install of `@wvlet/cli@latest`, streams its output, and ends by printing the new version. On failure it prints the exact command to run by hand.
- Opt out with `WVLET_NO_UPDATE_CHECK=1`; the check is also skipped in CI and when stderr is not a terminal.
- Homebrew users moving over: `brew uninstall wvlet`, then the npm install. The docs say so, because two `wvlet` binaries on PATH would make the notice and `upgrade` act on the wrong one.
- If the bundled DuckDB is missing (`--omit=optional`, unsupported platform), `run` fails with a message naming the expected `@duckdb/node-bindings-<os>-<arch>` package and the `WVLET_LIBDUCKDB` override.

Decisions:
- **Bundled DuckDB**: `@wvlet/cli` declares the DuckDB platform packages as `optionalDependencies`; npm installs only the one matching the host. The libduckdb lookup tries `WVLET_LIBDUCKDB` first, then the bundled package, then today's system paths (fixing the malformed Windows path).
- **Version lock**: a test asserts the npm bindings version matches `DUCKDB_JDBC_VERSION` (`2.0.0.1` ↔ `2.0.0-r.1`), so a Scala Steward bump of one fails CI until the other follows.
- **Update check is non-blocking**: the CLI reads a cached "latest version" from `~/.wvlet/update-check.json` and prints the notice from that cache. When the cache is older than 24 h it refreshes it in a detached background process, so no command waits on the network. Only the npm registry is contacted, and only the package name is sent.
- **Pre-releases**: the check compares against the `latest` dist-tag only, so rc builds never trigger the notice.
- **`wvlet upgrade`** lives in the Node entry script, not in Scala, because it is npm-specific. The wrapper intercepts `upgrade` before loading the Scala.js bundle, and appends it to the `--help` output. It uses `npm` by default and `pnpm`/`bun` when the running script sits in their global directory. The JVM and Native builds do not get this command.
- **Homebrew is kept but demoted**: the install docs, `for-ai-agents.md`, and the npm README lead with npm (Node.js 22+), list the JVM-only commands, and keep Homebrew and the release tarball as the way to get them. The release workflow keeps updating the formula.
- An ADR records npm as the primary CLI distribution channel.
- **Release check**: a CI job smoke-tests the packed `@wvlet/cli` tarball in a clean directory (`compile` plus a DuckDB `run`) on Linux, macOS, and Windows, and npm-publish runs it before publishing, so a broken bundle never ships.
- Tests cover libduckdb resolution from the bundled package, the version comparison and cache staleness, and the opt-out conditions.

## Alternatives and Why Not?

- **Depend on `@duckdb/node-api` instead of loading libduckdb via koffi.** Would mean rewriting the JS DuckDB connector; the platform packages already give us the same library for the existing koffi binding.
- **Download libduckdb in a postinstall script.** Postinstall is often disabled (`--ignore-scripts`, pnpm defaults) and needs network at install time; optional platform packages are the npm-native way.
- **Self-updating binary (replace files in place).** Fights the package manager's ownership of the global install; delegating to `npm install -g` keeps uninstall and permissions sane.
- **Drop the Homebrew formula now.** The npm build has no REPL or UI yet, so Homebrew users would lose features; revisit once `wv` runs on Node.
