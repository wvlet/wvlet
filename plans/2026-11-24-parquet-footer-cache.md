# Cache parsed Parquet footers in ParquetSource

## Goals

- Parse each Parquet footer once per file version instead of once per `Parquet.open`. At TPC-H SF10 the repeated parses cost about 50–90 ms of serial time per query (q21 opens the 2.2 GB lineitem.parquet about 5 times).
- Keep results correct when a file changes: a stale footer reads wrong data.
- Keep an escape hatch (an engine toggle) and an A/B switch for the bench.

## Background

- `ParquetSource` calls `Parquet.open(path)` for the schema, `estimatedRows`, column statistics (`estimatedDistinct`), and again in every `scan` / `scanSplits`. Each call re-reads and re-parses the footer on the calling thread.
- File paths in queries create a new `ParquetSource` per query, so its lazy vals don't help them. Catalog-registered tables keep their lazy vals, but still parse once per scan.
- uni 2026.3.9 (wvlet/uni#1183) adds `Parquet.open(path, metadata)`, which reuses a parsed `FileMetaData` with no read. uni keeps no cache; the caller owns invalidation.
- `columnStatistics` is a lazy val on `ParquetFile` (its builder is `private[parquet]`), so each new `ParquetFile` sums the row-group statistics again.
- `ParquetSource` reads only local files today: `Parquet.open(path)` goes through `ByteSource.ofFile`. uni's `FileInfo` has `size` and `lastModified`, no ETag.
- DuckDB's `parquet_metadata_cache` (default off) keys on the file's last-modified time and re-reads a footer when the file changed within a few seconds of reading it, since mtime can be coarse.
- Engine toggles are `Optimization` values that are on unless listed in `wvlet.engine.optimizer.disabled`; there is no default-off toggle today.

## Design

Lane: Standard — a new engine toggle and a choice of default that trades stale-read risk for speed.

- New toggle `parquet_footer_cache`, **on by default** (proposed):
  - wvlet's engine reads local, mostly immutable files (spec data, bench data, exports), and the key below detects ordinary rewrites, so the stale-read case DuckDB guards against is narrow here.
  - Default-off would need a new "disabled unless enabled" kind of toggle, and most users would never turn it on.
  - Off, `ParquetSource` behaves exactly as today.
- A process-wide footer cache, shared by every `ParquetSource` (like the existing distinct-count cache), keyed by path and validated by `size` + `lastModified` from `FileSystem.info(path)`:
  - One `info` call per open (a stat), taken before the footer is read, then `Parquet.open(path, metadata)` on a hit. Stat-first means a rewrite during the read can't file an old footer under the new size and mtime.
  - A size or mtime change re-parses and replaces the entry.
  - No `lastModified` → not cached.
  - Not cached when that mtime is within 10 seconds of the current time; such a footer is used once and parsed again next time (DuckDB's rule for coarse mtimes).
- Each entry computes `columnStatistics` lazily once from its first `ParquetFile`, and `ParquetSource` reads statistics from the entry instead of each new file's own lazy val.
- Bounded LRU (256 entries) under a lock; parsing happens outside the lock, so concurrent scans of other files don't wait.
- Remote files: not applicable today. `ParquetSource` only opens local paths, and uni gives no ETag, so the cache only handles local paths; a later remote source would need an ETag-keyed entry or no caching.
- How the toggle reaches each read (anything not told to cache keeps today's parse):
  - Scans: a new `ScanSpec` field, default off. `ScanOp` carries the engine's value from lowering, so the bench's on/off engines can share one catalog. Other callers (`TableWriter`) keep today's behavior.
  - Planning reads (schema, row count, statistics): a `ParquetSource` constructor flag, default off. The engine's resolver sets it from the toggle for file paths in queries, which are new per query. Catalog-registered sources and `ManagedTable` keep today's parse there; they read these once per instance anyway (lazy vals and the distinct-count cache).
  - `computeDistinct`'s own string-column scan passes the source's flag.
- Shared code: `FileSystem.info` and `lastModified` come from uni-core on every platform, so the cache lives in shared `src/main`; the mtime tests are JVM-only because they set file times.
- The new toggle is listed in `OptimizerOptionsTest` (the JS/Native gate in design/engine.md), and design/engine.md gets a line on the cache.
- Out of scope: the existing distinct-count cache is keyed by path and column only, so it keeps old estimates after a rewrite (estimates only, not results). Recorded as follow-up.
- Tests cover a hit skipping the parse, a size or mtime change re-parsing, a just-written file not being cached, and the toggle off parsing every time.
- Measure TPC-H SF10 q21 (and the suite) with `WVLET_ENGINE_BENCH_AB=parquet_footer_cache` if SF10 data is available here, and put the numbers in the PR.

## Alternatives and Why Not?

- **Cache per `ParquetSource` instance.** File-path sources are new per query, so most of the gain is lost.
- **Default off like DuckDB.** Safer against external writers on object stores, which wvlet doesn't read through this path; costs every user the parses by default.
- **Key by path only (no stat).** Saves a stat per open but serves a stale footer after a rewrite.
- **Bound by footer bytes instead of entries.** uni doesn't expose a footer's size; an entry count is simpler and footers are small relative to data.
