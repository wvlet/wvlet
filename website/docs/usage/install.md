---
sidebar_label: Installation
sidebar_position: 1
---

# Installation

Wvlet is available as a command-line tool. You can install wvlet using Homebrew on macOS or download a binary package for other platforms.

## Command Line Tools

- [wv (Interactive Shell (REPL))](repl.md)
- [wvlet (Command-line tool)](cli.md)
- [Web UI](ui.md)

:::info
Language SDKs are also available for [Python and TypeScript](../bindings) to compile Wvlet queries programmatically.
:::

## Mac OS X

For macOS, you can install wvlet using [Homebrew](https://brew.sh/):

```bash
brew install wvlet/wvlet/wvlet
```

This will install `wv (wvlet)` command to your system.

To upgrade wvlet, run:
```bash
brew upgrade wvlet
```

If the latest version is not installed, try `brew update` to update the formula, then run `brew upgrade wvlet` again.


:::info
The formula for homebrew can be found at 
https://github.com/wvlet/homebrew-wvlet
:::

## Linux, Windows, and Other Platforms

For other platforms (e.g., Linux, Windows, etc.), download a binary package wvlet-cli-(version).tar.gz from the release page https://github.com/wvlet/wvlet/releases

You can find wv command in the bin directory of the package. Add the bin directory to your PATH environment variable.

JDK17 or later is required to run wvlet. Set JAVA_HOME environment variable to the JDK path.

## Quick Start

After installing wvlet, start learning the query syntax of Wvlet:
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

1. **From VS Code Marketplace** (Recommended):
   - Install directly from the [VS Code Marketplace](https://marketplace.visualstudio.com/items?itemName=wvlet.wvlet)
   - Or search for "Wvlet" in the Extensions view (`Ctrl+Shift+X` / `Cmd+Shift+X`) within VS Code

2. **From VSIX File**:
   - Download the latest `.vsix` file from [GitHub Releases](https://github.com/wvlet/wvlet/releases)
   - In VS Code: Extensions → "..." → "Install from VSIX..."

### Features

- Syntax highlighting for all Wvlet keywords, operators, and syntax
- Bracket matching and auto-closing
- Comment toggling support (`--` and `---`)
- String interpolation highlighting
- Number literal support (integers, floats, hex)

The extension supports both stable and pre-release versions. Pre-release versions allow early access to new features.

## Building From Source

See [Building Wvlet](../development/build.md) for building wvlet from source.
