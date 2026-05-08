# @wvlet/cli

The wvlet command-line compiler for Node.js. Compile `.wv` flow-style queries to SQL without a JVM.

## Install

```bash
npm install -g @wvlet/cli
```

Requires Node.js 20+.

## Usage

```bash
wvlet version
wvlet compile -f query.wv > query.sql
wvlet compile -t trino "from x select a, b"
wvlet to_wvlet "select a, b from x where c > 10"
```

This package ships the same `version` / `compile` / `to_wvlet` subcommands as the JVM `wvlet` binary and the Scala Native `wvc` binary. JVM-only features (`run`, `ui`, REPL) are not available in this Node distribution.

## Building from source

```bash
# In this folder:
pnpm run build       # produces lib/main.js (production, ~6 MB)
pnpm run build:dev   # produces lib/main.js (dev, ~10 MB, faster)
```

The build invokes `./sbt cliCoreJS/{fullLinkJS,fastLinkJS}` from the repo root, which links the Scala.js compiler bundle into `lib/`.

## License

Apache-2.0
