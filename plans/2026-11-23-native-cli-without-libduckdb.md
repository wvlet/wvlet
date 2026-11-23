# Native CLI links without libduckdb

## Goals

- `wvc` and `cliCoreNative` (and every native test binary) build and start on a machine with no libduckdb and no `duckdb.h`.
- DuckDB stays usable from the native CLI when libduckdb is installed, as an opt-in, the same way the npm CLI finds it today.
- Remove the rpath workarounds in build.sbt that exist only to find libduckdb at load time.

## Background

- `wvlet-lang/.native` compiles `wvlet_duckdb_helpers.c` (which includes `duckdb.h`) into every native binary, and `DuckDBApi` is an `@extern @link("duckdb")` object. So `lang`, `engine`, `cliCore`, `wvc`, `wvcLib` all link `-lduckdb`.
- The binary then needs libduckdb at load time for every command; `nativeLibRpathLinking` embeds Homebrew/`/usr/local` rpaths to make that work on macOS.
- Shared code reaches DuckDB only through the `DuckDB` facade (`isAvailable`, `canExecute`, `schemaOf`, `execute`, `newSession`): `RelationRefResolver` already skips file-schema inference when `isAvailable` is false, and `SqlConnectorProvider` already raises a clear error when `canExecute` is false.
- The Scala.js backend already loads libduckdb at runtime (koffi, `WVLET_LIBDUCKDB` then standard install paths) and reports unavailable when it can't.
- The native CLI's default engine is still `duckdb`; Step 1 changes that in parallel. This PR must not break DuckDB on native while Step 1 is in flight.

## Design

Lane: Standard — it changes how the shipped native binaries are linked and packaged (always-gate: packaging).

- Load libduckdb at runtime on native, mirroring the JS backend: a small C loader resolves the DuckDB C API functions with `dlopen`/`dlsym` (`LoadLibrary`/`GetProcAddress` on Windows) and the Scala side calls through it.
- The loader declares the few opaque DuckDB types it needs itself, so building no longer needs `duckdb.h`.
- Lookup order: `WVLET_LIBDUCKDB`, then the loader's default search (`DYLD_LIBRARY_PATH` / `LD_LIBRARY_PATH` / `PATH`), then Homebrew and `/usr/local` paths. Same order and env var as the JS backend.
- When none loads, `DuckDB.isAvailable` / `canExecute` are false; existing callers already degrade (engine-side file scans, a clear "libduckdb not available" error for the duckdb connector).
- Build: drop `-lduckdb` from `engine`, `cliCore`, `lang` native settings and delete `nativeLibRpathLinking` (and `WVLET_NATIVE_LIB_PATH`). Add `-ldl` on Linux only if the linker needs it.
- CI: native jobs no longer install `duckdb.h`; they keep libduckdb so DuckDB-backed native tests still run. Add a check that the linked `wvc` binary has no libduckdb dependency (`otool -L` / `ldd`).
- Tests cover: DuckDB still works on native when libduckdb is present, and `isAvailable` is false with `WVLET_LIBDUCKDB` pointing at a missing file and no other candidate.
- Docs: update CLAUDE.md's native build note.
- npm CLI (`@duckdb/node-bindings-*`): left as a follow-up in the work item, after Step 1 makes `wvlet` the default engine.

## Alternatives and Why Not?

- Separate native-only DuckDB module that the CLI omits (the item's wording): no native binary would include it after this PR, so DuckDB on native is lost outright while Step 1 is still landing; it also needs a registration hook since Scala Native has no runtime service discovery. Runtime loading gives the same "no link dependency" result and keeps DuckDB opt-in.
- Keep `@link("duckdb")` but make it weak-linked: platform-specific linker flags (`-weak-l` on macOS, none portable on Linux/Windows), and still needs `duckdb.h`.
