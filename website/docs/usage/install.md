---
sidebar_label: Installation
sidebar_position: 1
---

# Installation

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


## Building From Source

See [Building Wvlet](../development/build.md) for building wvlet from source.
