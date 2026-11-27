---
description: "Install the Wvlet command-line tool with npm (no JDK needed), or the full JVM CLI (flow, ui, JDBC connectors) with Homebrew or a release package."
sidebar_label: Installation
sidebar_position: 1
---

# Installation

Wvlet is available as one command, `wvlet`, in two distributions:

| | npm (`@wvlet/cli`) | JVM CLI (Homebrew or release package) |
|---|---|---|
| Requires | Node.js 22+ | JDK 17+ (Homebrew installs OpenJDK) |
| `wvlet version`, `compile`, `to_wvlet`, `run` | ✓ | ✓ |
| [`wvlet build`](../syntax/model-pipelines.md#building-models) | ✓ (one model at a time) | ✓ (parallel, with run records) |
| DuckDB (`-t duckdb`) | Included | Full package only |
| Snowflake connector | — | Full package only |
| [REPL](repl.md) (`wvlet` on a terminal, or `wvlet repl`) | ✓ | ✓ (also `wv`) |
| `wvlet flow`, `catalog`, `ui` | — | ✓ |
| Updates | Update notice + `wvlet upgrade` | `brew upgrade wvlet` |

Which one to install:

- **npm** is the default: the [REPL](repl.md), `run`, `build`, and `compile`, with DuckDB included and no JDK. Start here.
- **The JVM CLI** adds the [Web UI](ui.md), [flows](../syntax/flow.md) and their scheduler, `catalog import` from a live database, the JDBC connectors such as Snowflake, and parallel model builds with run records. Install it when you need one of these; see [Moving from npm](#moving-from-npm).

## npm (macOS, Linux, Windows)

```bash
npm install -g @wvlet/cli
wvlet version
```

The package bundles DuckDB through DuckDB's official prebuilt `@duckdb/node-bindings-<os>-<arch>` package, so `wvlet run` works with no other installs. Supported platforms are macOS, Linux (glibc and musl), and Windows on x64 and arm64. `pnpm add -g @wvlet/cli` and `bun add -g @wvlet/cli` also work.

### Updating

Once a day, the CLI checks the npm registry in the background. When a new release is out, it prints one line after a command finishes:

```
wvlet 2026.12.0 is available (current 2026.11.0). Run 'wvlet upgrade' to update.
```

`wvlet upgrade` reinstalls the latest release with the package manager you installed it with (npm, pnpm, or bun). If it fails, for example due to permissions, it prints the command to run yourself.

The check never delays a command: it reads the cached result in `~/.wvlet/update-check.json` and refreshes it in a background process. It is skipped in CI and when stderr is not a terminal. To turn it off, set `WVLET_NO_UPDATE_CHECK=1`.

### Moving from Homebrew

If you installed Wvlet with Homebrew and only need the npm commands, uninstall the formula first so that only one `wvlet` is on your `PATH`:

```bash
brew uninstall wvlet
npm install -g @wvlet/cli
which wvlet   # should point to the npm global bin directory
```

### Troubleshooting

- npm may warn that it skipped the install script of `koffi`. The warning is harmless: koffi ships prebuilt binaries that work without the script.
- `libduckdb is not available`: the DuckDB platform package was not installed, usually because of `--omit=optional` or an unsupported platform. Reinstall without `--omit=optional`, or set `WVLET_LIBDUCKDB` to the absolute path of a `libduckdb` shared library.

## JVM CLI

The JVM CLI includes the [Web UI](ui.md), flows, and the JDBC connectors, and builds models in parallel with run records, in addition to the commands and [REPL](repl.md) of the npm `wvlet`. Each release ships it in two packages with the same `wvlet` command and its `wv` alias (`wvlet repl`):

- `wvlet-cli-(version).tar.gz` (about 60 MB, installed by Homebrew): queries run on the in-process [wvlet engine](../engines/wvlet-engine.md), [Trino](../engines/trino.md), and remote wvlet servers.
- `wvlet-cli-(version)-(platform).tar.gz` (about 95 MB) for `macos-arm64`, `macos-x64`, `linux-x64`, and `linux-arm64`: the `wvlet-cli` package with a bundled Java runtime in `runtime/`, so no JDK is needed. The `wvlet` and `wv` commands run the bundled runtime even when `JAVA_HOME` points to another JDK.
- `wvlet-cli-full-(version).tar.gz` (about 230 MB): adds the [DuckDB](../engines/duckdb.md) and [Snowflake](../engines/snowflake.md) connectors, whose JDBC drivers make up most of its size. Use it for `-t duckdb`, `.duckdb` files, DuckDB extensions, `sql"..."` blocks run on DuckDB, or Snowflake profiles.

Without the full package, selecting DuckDB or Snowflake fails with an error that names the full package.

### macOS (Homebrew)

```bash
brew install wvlet/wvlet/wvlet
```

This installs the `wvlet` command (and the `wv` alias) of the `wvlet-cli` package (without DuckDB and Snowflake), along with OpenJDK as a dependency. For DuckDB or Snowflake, use the full release package below. To upgrade, run `brew upgrade wvlet` (run `brew update` first if the latest version is not picked up).

:::info
The formula for homebrew can be found at
https://github.com/wvlet/homebrew-wvlet
:::

### Linux, Windows, and Other Platforms

Download a binary package, `wvlet-cli-(version)-(platform).tar.gz`, `wvlet-cli-(version).tar.gz`, or `wvlet-cli-full-(version).tar.gz` (with DuckDB and Snowflake), from the release page https://github.com/wvlet/wvlet/releases

You can find the `wvlet` command and the `wv` alias in the bin directory of the package. Add the bin directory to your PATH environment variable.

The platform packages (`wvlet-cli-(version)-(platform).tar.gz`) need no JDK. The other packages require JDK 17 or later: set the JAVA_HOME environment variable to the JDK path.

:::info
Language SDKs are also available for [Python and TypeScript](../bindings) to compile Wvlet queries programmatically.
:::

### Moving from npm

Your `.wv` folders, profiles in `~/.wvlet`, tables built into `target/warehouse`, and command lines work the same on the JVM CLI, which adds the `wv` alias of `wvlet repl`. To switch:

```bash
npm uninstall -g @wvlet/cli
brew install wvlet/wvlet/wvlet   # or a release package
which wvlet                      # should point to the JVM CLI
```

The npm package bundles DuckDB, but the Homebrew package does not. If you use `-t duckdb`, `.duckdb` files, or Snowflake, install `wvlet-cli-full-(version).tar.gz` instead. Keeping both installs works too, but the first `wvlet` on your `PATH` is the one that runs.

## Quick Start

After installing Wvlet, start learning the query syntax of Wvlet:
- [Query Syntax](../syntax) to learn the query syntax


## Language SDKs

If you want to integrate Wvlet compilation into your applications, install one of our language SDKs:

### Python
```bash
pip install wvlet
```

### TypeScript/JavaScript
```bash
npm install @wvlet/wvlet
```

See [Language Bindings](../bindings) for more details and usage examples.

## VS Code Extension

For VS Code users, we provide a language extension that adds syntax highlighting and language support for `.wv` files:

### Installation

Install directly from the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet) or search for "Wvlet" in the Extensions view (`Ctrl+Shift+X` / `Cmd+Shift+X`) within VS Code.

### Features

- Syntax highlighting for all Wvlet keywords, operators, and syntax
- Bracket matching and auto-closing
- Comment toggling support (`--` and `---`)
- String interpolation highlighting
- Number literal support (integers, floats, hex)

The extension supports both stable and pre-release versions. Pre-release versions allow early access to new features.

For full details, including troubleshooting and development guides, please see the [VS Code Extension documentation](./vscode.md).

## Building From Source

See [Building Wvlet](../development/build.md) for instructions on building it from source.
