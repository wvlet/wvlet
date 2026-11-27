---
description: "Install the Wvlet command-line tool with npm (no JDK needed), or the full JVM CLI (wv REPL, build, flow, ui) with Homebrew or a release package."
sidebar_label: Installation
sidebar_position: 1
---

# Installation

Wvlet is available as a command-line tool in two distributions:

| | npm (`@wvlet/cli`) | JVM CLI (Homebrew or release package) |
|---|---|---|
| Requires | Node.js 22+ | JDK 17+ (Homebrew installs OpenJDK) |
| `wvlet version`, `compile`, `to_wvlet`, `run` | ✓ | ✓ |
| DuckDB (`-t duckdb`) | Included | Full package only |
| Snowflake connector | — | Full package only |
| `wv` REPL, `wvlet build`, `flow`, `catalog`, `ui` | — | ✓ |
| Updates | Update notice + `wvlet upgrade` | `brew upgrade wvlet` |

Start with npm unless you need a JVM-only command.

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

The JVM CLI includes the [wv REPL](repl.md), [Web UI](ui.md), model builds, and flows, in addition to the [wvlet command](cli.md). Each release ships it in two packages with the same `wv` and `wvlet` commands:

- `wvlet-cli-(version).tar.gz` (about 60 MB, installed by Homebrew): queries run on the in-process [wvlet engine](../engines/wvlet-engine.md), [Trino](../engines/trino.md), and remote wvlet servers.
- `wvlet-cli-full-(version).tar.gz` (about 230 MB): adds the [DuckDB](../engines/duckdb.md) and [Snowflake](../engines/snowflake.md) connectors, whose JDBC drivers make up most of its size. Use it for `-t duckdb`, `.duckdb` files, DuckDB extensions, `sql"..."` blocks run on DuckDB, or Snowflake profiles.

Without the full package, selecting DuckDB or Snowflake fails with an error that names the full package.

### macOS (Homebrew)

```bash
brew install wvlet/wvlet/wvlet
```

This installs the `wv` and `wvlet` commands of the `wvlet-cli` package (without DuckDB and Snowflake), along with OpenJDK as a dependency. For DuckDB or Snowflake, use the full release package below. To upgrade, run `brew upgrade wvlet` (run `brew update` first if the latest version is not picked up).

:::info
The formula for homebrew can be found at
https://github.com/wvlet/homebrew-wvlet
:::

### Linux, Windows, and Other Platforms

Download a binary package, `wvlet-cli-(version).tar.gz` or `wvlet-cli-full-(version).tar.gz` (with DuckDB and Snowflake), from the release page https://github.com/wvlet/wvlet/releases

You can find the `wv` and `wvlet` commands in the bin directory of the package. Add the bin directory to your PATH environment variable.

JDK 17 or later is required. Set the JAVA_HOME environment variable to the JDK path.

:::info
Language SDKs are also available for [Python and TypeScript](../bindings) to compile Wvlet queries programmatically.
:::

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
