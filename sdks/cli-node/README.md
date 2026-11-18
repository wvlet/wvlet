# @wvlet/cli

The wvlet command-line tool for Node.js. Compile `.wv` flow-style queries to SQL and run them on DuckDB, Trino, or a wvlet server, without a JVM.

## Install

```bash
npm install -g @wvlet/cli
```

Requires Node.js 22+. DuckDB is included: the matching `@duckdb/node-bindings-<os>-<arch>` package (macOS, Linux glibc/musl, Windows; x64 and arm64) is installed as an optional dependency, so do not install with `--omit=optional`. To use another libduckdb, set `WVLET_LIBDUCKDB` to its absolute path.

## Usage

```bash
wvlet version
wvlet compile -f query.wv > query.sql
wvlet compile -t trino "from x select a, b"
wvlet to_wvlet "select a, b from x where c > 10"
wvlet run "from 'data.parquet' limit 10"
wvlet run -f query.wv --format csv
```

The `wv` REPL and the JVM-only commands (`wvlet build`, `flow`, `catalog`, `ui`) are not in this package; install the JVM CLI for them (see [Installation](https://wvlet.org/docs/usage/install)).

## Updating

When a new release is published, the CLI prints a one-line notice after a command finishes. Run `wvlet upgrade` to reinstall the latest release with the package manager you used (npm, pnpm, or bun).

The check runs at most once a day in a background process and caches its result in `~/.wvlet/update-check.json`. It is skipped in CI and when stderr is not a terminal; set `WVLET_NO_UPDATE_CHECK=1` to turn it off.

## Building from source

```bash
# In this folder:
pnpm run build               # produces lib/main.js (production, ~6 MB)
pnpm run build:scalajs:dev   # produces lib/main.js (dev, ~14 MB, faster)
pnpm test                    # unit tests for the Node entry script
```

The build invokes `./sbt cliCoreJS/{fullLinkJS,fastLinkJS}` from the repo root, which links the Scala.js compiler bundle into `lib/`.

## License

Apache-2.0
