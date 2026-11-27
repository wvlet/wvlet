---
description: "Install the Wvlet command-line tool with Homebrew (recommended) or a release package, or from npm."
sidebar_label: Installation
sidebar_position: 1
---

# Installation

Install `wvlet` with Homebrew. It is the full command-line tool: the [REPL](repl.md), `run`, `build`, [flows](../syntax/flow.md), the [Web UI](ui.md), and the JDBC connectors.

## Homebrew (recommended)

```bash
brew install wvlet/wvlet/wvlet
wvlet version
```

This installs the `wvlet` command and its `wv` alias (`wvlet repl`). Homebrew sets up the Java runtime it needs, so there is no JDK to install. To upgrade, run `brew upgrade wvlet` (run `brew update` first if the latest version is not picked up). The formula is at https://github.com/wvlet/homebrew-wvlet.

## Release packages

Without Homebrew, for example on Windows, download a package from the [release page](https://github.com/wvlet/wvlet/releases) and add its `bin` folder to your `PATH`:

- `wvlet-cli-(version)-(platform).tar.gz` for `macos-arm64`, `macos-x64`, `linux-x64`, and `linux-arm64`: bundles a Java runtime, so no JDK is needed.
- `wvlet-cli-(version).tar.gz`: the same command without a runtime; it needs JDK 17 or later in `JAVA_HOME`.
- `wvlet-cli-full-(version).tar.gz`: adds the [DuckDB](../engines/duckdb.md) and [Snowflake](../engines/snowflake.md) connectors; it needs JDK 17 or later.

## npm

`wvlet` is also on npm, for machines that have Node.js 22+ but no Homebrew:

```bash
npm install -g @wvlet/cli
```

It runs the same command lines on the in-process [Wvlet engine](../engines/wvlet-engine.md): the [REPL](repl.md), `run`, `compile`, and `build` (one model at a time). `flow`, `catalog`, `ui`, and the JDBC connectors need the Homebrew or release-package `wvlet`. It runs on macOS, Linux, and Windows on x64 and arm64; `pnpm add -g` and `bun add -g` work too.

- **Updates**: once a day it checks for a new release in the background and prints a one-line notice; `wvlet upgrade` reinstalls with the package manager you used. Set `WVLET_NO_UPDATE_CHECK=1` to turn the check off.
- **With Homebrew too**: the first `wvlet` on your `PATH` is the one that runs; check with `which wvlet`.
- **Troubleshooting**: a warning that npm skipped `koffi`'s install script is harmless.

## Quick Start

After installing Wvlet, follow the [Quick Start](../syntax/quick-start.md) to run your first queries.


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
