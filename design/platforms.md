# Platforms: JVM, Scala.js, and Scala Native

This page owns how cross-platform modules split shared and platform code, and how native binaries find DuckDB.

## Platform-specific code

- Cross-platform modules are sbt `crossProject`s. Shared code lives in `src/main/scala`; platform code in `.jvm/`, `.js/`, and `.native/src/main/scala`. Use `%%%` for cross-platform dependencies.
- Platform differences go behind an `XXXCompat` trait: a shared `trait FileIOCompat`, a `trait FileIOCompatImpl extends FileIOCompat` per platform, and a shared `object Compat extends FileIOCompatImpl`. `IOCompat` is the model: Scala.js has no file I/O, and needs none.
- Scala.js code must not use Java-specific libraries.
- Compile a whole platform with `./sbt "projectJVM/Test/compile"`, `projectJS/...`, or `projectNative/...`.

## Native builds and libduckdb

- Native builds need clang, llvm, libstdc++-12-dev, and libgc (Boehm GC). `wvcLib/nativeLink` builds the C library (`wvc-lib`, tested with `cd wvc-lib && make test`); `wvc/nativeLink` builds the standalone compiler.
- Native binaries do not link libduckdb. `wvlet_duckdb_loader.c` in wvlet-lang loads it at run time, so building needs no DuckDB headers. See [the ADR](../adr/2026-11-23-native-libduckdb-runtime-loading.md).
- DuckDB-backed features (file schema inference, the duckdb connector) work when libduckdb is installed (`brew install duckdb`, `/usr/local/lib` or Homebrew paths, the loader's search path, or `WVLET_LIBDUCKDB=<path>`), and report "libduckdb is not available" otherwise.
- CI's Scala Native job runs without libduckdb. A shared test that needs DuckDB skips itself with `if !DuckDB.canExecute then ignore(...)` (or `DuckDB.isAvailable` for schema inference). DuckDB is tested on the JVM and Scala.js jobs.
- No CI job runs the native DuckDB bindings. After changing them, run `./sbt "langNative/testOnly *DuckDB*"` locally with libduckdb installed.
