# Platforms: JVM, Scala.js, and Scala Native

This page owns how cross-platform modules split shared and platform code, and how native binaries find DuckDB.

## Platform-specific code

- Cross-platform modules are sbt `crossProject`s. Shared code lives in `src/main/scala`; platform code in `.jvm/`, `.js/`, and `.native/src/main/scala`. Use `%%%` for cross-platform dependencies.
- Platform differences go behind an `XXXCompat` trait: a shared `trait FileIOCompat`, a `trait FileIOCompatImpl extends FileIOCompat` per platform, and a shared `object Compat extends FileIOCompatImpl`. `IOCompat` is the model: Scala.js has no file I/O, and needs none.
- Scala.js code must not use Java-specific libraries.
- Compile a whole platform with `./sbt "projectJVM/Test/compile"`, `projectJS/...`, or `projectNative/...`.

## Native builds and libduckdb

- Native builds need clang, llvm, and libstdc++-12-dev. `wvcLib/nativeLink` builds the C library (`wvc-lib`, tested with `cd wvc-lib && make test`); `wvc/nativeLink` builds the standalone compiler.
- Native binaries do not link libduckdb. `wvlet_duckdb_loader.c` in wvlet-lang loads it at run time, so building needs no DuckDB headers. See [the ADR](../adr/2026-11-23-native-libduckdb-runtime-loading.md).
- DuckDB-backed features (file schema inference, the duckdb connector) work when libduckdb is installed (`brew install duckdb`, `/usr/local/lib` or Homebrew paths, the loader's search path, or `WVLET_LIBDUCKDB=<path>`), and report "libduckdb is not available" otherwise.
- CI's Scala Native job runs without libduckdb. A shared test that needs DuckDB skips itself with `if !DuckDB.canExecute then ignore(...)` (or `DuckDB.isAvailable` for schema inference). DuckDB is tested on the JVM and Scala.js jobs.
- No CI job runs the native DuckDB bindings. After changing them, run `./sbt "langNative/testOnly *DuckDB*"` locally with libduckdb installed.

## Native garbage collectors

- Every native artifact (`wvc`, `wvcLib`, the native test binaries) uses Scala Native's default Immix GC, with no heap cap. Native builds no longer need libgc.
- Immix and Commix corrupted live objects under multi-threaded allocation up to Scala Native 0.5.12 (scala-native#5046, #5160), and the engine runs its morsels on a thread pool. 0.5.13 fixed this. TPC-H SF1 on 0.5.13 completed every query in 110 runs with Immix, at 0.61x the wall time of Boehm and about 2x its peak RSS. See [benchmarking](../website/docs/development/benchmark.md#columnar-engine-on-scala-native).
- `wvc` sets no heap cap, because the engine's live data grows with the input (SF1 needs more than 512 MB). Users bound memory with `GC_MAXIMUM_HEAP_SIZE`. Since 0.5.13, running out of heap throws OutOfMemoryError ("Scala Native heap space") instead of aborting.
- `wvcLib` (`libwvlet`, loaded into Rust, C, C++, and Python hosts) used Boehm from #925 to avoid a segfault in host processes. On 0.5.13, Boehm segfaults in the Rust host test on linux-x64 and fails to link on Windows (`GC_least_plausible_heap_addr`), so `wvcLib` uses Immix too. The Native workflow's `make test` in `wvc-lib` is the gate.
- 0.5.13 leaves zlib out of `wvcLib`'s Windows link libraries but still compiles javalib's zlib glue, so `build.sbt` links `zlib` explicitly for Windows targets.
- After a Scala Native bump, rerun `spec/tpch/bench/run_native_bench.sh` with a release-fast `wvc` and check the Native workflow before changing a GC setting.
