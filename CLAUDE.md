# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Wvlet is a cross-SQL flow-style query language for functional data modeling and interactive data exploration. It compiles .wv query files into SQL for various database engines (DuckDB, Trino, Hive). The project consists of a language compiler, runtime system, web UI, and multi-platform bindings.

## Implementation Details

- wv command is implemented in WvletREPLMain

## Key Development Commands

### Building and Installing
```bash
# Enter SBT shell
./sbt

# Install wvlet CLI command to ~/local/bin/wv
sbt:wvlet> cli/packInstall

# Build native library (requires clang, llvm, libgc)
sbt:wvlet> wvcLib/nativeLink

# Build standalone native compiler
sbt:wvlet> wvc/nativeLink
```

[Rest of the file remains unchanged...]
