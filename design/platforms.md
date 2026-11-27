# Platforms: JVM, Scala.js, and Scala Native

This page owns how cross-platform modules split shared and platform code, and how native binaries find DuckDB.

## Platform-specific code

- Cross-platform modules are sbt `crossProject`s. Shared code lives in `src/main/scala`; platform code in `.jvm/`, `.js/`, and `.native/src/main/scala`. Use `%%%` for cross-platform dependencies.
- Platform differences go behind an `XXXCompat` trait: a shared `trait FileIOCompat`, a `trait FileIOCompatImpl extends FileIOCompat` per platform, and a shared `object Compat extends FileIOCompatImpl`. `IOCompat` is the model: Scala.js has no file I/O, and needs none.
- Scala.js code must not use Java-specific libraries.
- Code shared by the JVM and Native but not Scala.js (threads, blocking waits) goes in a `jvm-native/src/main/scala` folder added to both platforms' sources, as `wvlet-cli-core` does for the REPL's query threads (`REPLCompatImpl`).
- The REPL is shared code in `wvlet-cli-core`. Node.js runs every query synchronously on the event loop, where no SIGINT listener can run until the query ends, so Ctrl-C reaches a running query through `CtrlCWatcher`: a worker thread whose helper `node` process gets the terminal's SIGINT and raises the shared `QueryCancel` flag ([ADR](../adr/2026-11-26-npm-repl-ctrl-c-watcher.md)).
  - Code that can run long on Node checks `QueryCancel.check()`: engine source operators per batch (`ScanOp`, `BatchesOp`, `IteratorOp`), and Trino's `nextUri` loop per page. A blocking DuckDB call registers its connection with `QueryCancel.withDuckDBConnection`, so the watcher can call `duckdb_interrupt` on it. Add a check to any new loop that can run for seconds on Node.
  - On the JVM and Native, `QueryCancel.isRequested` is a constant `false`; their REPL cancels through the query thread.
  - Without a terminal, or when the helper can't start, the REPL registers no SIGINT listener during a query, so Ctrl-C exits `wvlet` instead of being swallowed until the query ends.
- `wvlet build` is a shared command too. The JVM runs it as a flow on the JVM-only flow executor (parallel stages, timeouts, run records); npm and Native run the shared `ModelBuilder`, which builds one model at a time on the caller, so Node never needs threads or an interruptible query.
- The npm and Native `wvlet` reject the JVM CLI's own commands (`ui`, `flow`, `catalog`) with a message naming the JVM CLI. A new command added only to `WvletMain` goes in `WvletCli.jvmOnlyCommands` too, or npm users get the launcher's generic unknown-command error. See [the ADR](../adr/2026-11-26-distribution-roles.md); Homebrew is the recommended install ([ADR](../adr/2026-11-27-homebrew-recommended-install.md)).
- Compile a whole platform with `./sbt "projectJVM/Test/compile"`, `projectJS/...`, or `projectNative/...`.

## Native builds and libduckdb

- Native builds need clang, llvm, and libstdc++-12-dev. `wvcLib/nativeLink` builds the C library (`wvc-lib`, tested with `cd wvc-lib && make test`); `wvc/nativeLink` builds the standalone compiler.
- Native binaries do not link libduckdb. `wvlet_duckdb_loader.c` in wvlet-lang loads it at run time, so building needs no DuckDB headers. See [the ADR](../adr/2026-11-23-native-libduckdb-runtime-loading.md).
- DuckDB-backed features (file schema inference, the duckdb connector) work when libduckdb is installed (`brew install duckdb`, `/usr/local/lib` or Homebrew paths, the loader's search path, or `WVLET_LIBDUCKDB=<path>`), and report "libduckdb is not available" otherwise.
- CI's Scala Native job runs without libduckdb. A shared test that needs DuckDB skips itself with `if !DuckDB.canExecute then ignore(...)` (or `DuckDB.isAvailable` for schema inference). DuckDB is tested on the JVM and Scala.js jobs.
- No CI job runs the native DuckDB bindings. After changing them, run `./sbt "langNative/testOnly *DuckDB*"` locally with libduckdb installed.

## Native garbage collectors

- Every native artifact (`wvc`, `wvcLib`, the native test binaries) uses Scala Native's default Immix GC, with no heap cap. Native builds no longer need libgc. The Windows job in `native.yml` still installs `bdwgc` from its cached vcpkg set; dropping it changes the cache key and rebuilds every vcpkg port.
- Immix and Commix corrupted live objects under multi-threaded allocation up to Scala Native 0.5.12 (scala-native#5046, #5160), and the engine runs its morsels on a thread pool. 0.5.13 fixed this. TPC-H SF1 on 0.5.13 completed every query in 110 runs with Immix, at 0.61x the wall time of Boehm and about 2x its peak RSS. See [benchmarking](../website/docs/development/benchmark.md#columnar-engine-on-scala-native).
- `wvc` sets no heap cap, because the engine's live data grows with the input (SF1 needs more than 512 MB). Users bound memory with `GC_MAXIMUM_HEAP_SIZE`. Since 0.5.13, running out of heap throws OutOfMemoryError ("Scala Native heap space") instead of aborting.
- `wvcLib` (`libwvlet`, loaded into Rust, C, C++, and Python hosts) used Boehm from #925 to avoid a segfault in host processes. 0.5.13's Boehm glue fails to link on Windows (`GC_least_plausible_heap_addr` is missing from vcpkg's bdwgc), so `wvcLib` uses Immix too. Its host tests (`make test` in `wvc-lib`) pass with Immix on macOS arm64 and linux-arm64, and the Windows DLLs link.
- `wvcLib` starts the Scala Native runtime on the first call into its C API (`wvc-lib/src/main/resources/scala-native/wvlet_lib.c`), not in the library constructor: `build.sbt` compiles it with `-DSCALANATIVE_NO_DYLIB_CTOR`, which also switches that file's lazy start on. Since 0.5.13, `initLibrary` fills in a preallocated OutOfMemoryError's stack trace, and from the constructor libunwind walks into the dynamic loader's frames. With Nix's x86_64 glibc 2.42 `ld-linux`, whose `.eh_frame` has no zero terminator, that walk reads past the section and the host segfaults before `main` (`ScalaNativeInit` → `initLibrary` → `fillInStackTrace`). Hosts linked against Ubuntu's glibc did not crash. Keep this lazy start until Scala Native stops walking the stack during library start-up; the public `wvlet_compile_*` symbols are defined in that C file and forward to the `@exported` `wvlet_internal_*` functions in `WvcLib`.
- 0.5.13 leaves zlib out of `wvcLib`'s Windows link libraries but still compiles javalib's zlib glue, so `build.sbt` links `zlib` explicitly for Windows targets.
- After a Scala Native bump, rerun `spec/tpch/bench/run_native_bench.sh` with a release-fast `wvc` and check the Native workflow before changing a GC setting.
