# ADR: Native binaries load libduckdb at runtime

Date: 2026-11-23 · PR #2315

## Context

- Scala Native binaries (`wvc`, `cliCoreNative`, `wvcLib`, native tests) linked `-lduckdb` through wvlet-lang's C helpers and the `@link("duckdb")` extern object, so every command, including ones that never touch DuckDB, failed to start without libduckdb, builds needed `duckdb.h`, and build.sbt embedded rpaths to find the library.
- The goal is a lightweight CLI whose default engine is the in-process `wvlet` engine, with DuckDB as an opt-in connector.
- The native CLI still defaults to DuckDB while that default change lands separately.
- The Scala.js backend already loads libduckdb at runtime through koffi and reports it unavailable when it can't.

## Decision

- `wvlet_duckdb_loader.c` opens libduckdb with `dlopen`/`dlsym` (`LoadLibrary`/`GetProcAddress` on Windows) and exposes `wvlet_duckdb_*` forwarding wrappers; `DuckDBApi` binds the wrappers with `@name`, and no binary links libduckdb.
- The loader declares the few DuckDB types it uses itself (opaque handles and the 48-byte `duckdb_result`), so building needs no DuckDB headers; the wrappers also keep taking `duckdb_result *` where the C API takes the struct by value.
- `DuckDBLoader` tries `WVLET_LIBDUCKDB`, then the bare library name (the OS loader's search path), then Homebrew and `/usr/local` paths, the same order as the Scala.js backend, once per process.
- When no candidate loads, `DuckDB.isAvailable` and `canExecute` are false and `DuckDB.notAvailableMessage` carries the first candidate's load error.
- Moving the DuckDB natives into a separate native module that the CLI omits was rejected: no native binary would include it, so the native CLI would lose DuckDB while DuckDB is still its default, and Scala Native has no runtime service discovery to plug it back in.

## Consequences

- Native binaries start on machines without libduckdb; file schema inference falls back to the engine and the duckdb connector reports how to install libduckdb or set `WVLET_LIBDUCKDB`.
- A new DuckDB C API call needs an entry in the loader's `WVLET_DUCKDB_FUNCTIONS` list, a wrapper, and a `@name` binding.
- CI's Scala Native job runs once without libduckdb, as the native CLI ships: DuckDB-backed native tests skip on `DuckDB.canExecute` / `DuckDB.isAvailable`, and the fallback tests run. DuckDB itself is tested on the JVM and Scala.js jobs. (PR #2315 first installed libduckdb and reran `SqlConnectorProviderTest` without it; that second pass cost about a minute on the slowest CI job.)
