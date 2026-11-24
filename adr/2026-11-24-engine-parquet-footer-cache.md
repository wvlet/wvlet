# ADR: Parsed Parquet footers are cached, on by default, keyed by a stat

Date: 2026-11-24 · PR #2365

## Context

- `ParquetSource` opened each file several times per query (schema, row count, statistics, every scan), and each open read and parsed the whole footer on the calling thread. At TPC-H SF10, lineitem.parquet has 488 row groups and one parse takes about 14 ms, so q21 spent about 70 ms of serial time on footers.
- uni 2026.3.9 can open a file with a footer parsed earlier (`Parquet.open(path, metadata)`) but keeps no cache, so the engine must decide when a footer is stale. A stale footer reads wrong data or fails.
- DuckDB's `parquet_metadata_cache` caches footers, keyed by last-modified time, and is off by default.

## Decision

- `ParquetFooterCache` keeps parsed footers and their column statistics process-wide, LRU up to 256 files, behind the `parquet_footer_cache` optimization.
- It is **on by default**, unlike DuckDB. The engine reads local files that are mostly immutable (spec data, bench data, exports), and every engine optimization is on unless disabled. A default-off toggle would need a new kind of toggle that few users would turn on.
- An entry is valid while the file's `size` and `lastModified` match a stat taken before the footer read, so a rewrite during the read can't be cached under the new key. Nothing is cached for a file without an mtime, a symbolic link (uni stats the link itself), or a file modified within the last 10 seconds, since mtime can be coarse (DuckDB's rule).
- Only local paths are cached. A remote source would need a key that changes on every overwrite, such as an ETag, which uni's `FileInfo` doesn't provide.
- Scans take the flag from `ScanSpec.footerCache`, which lowering sets from the engine's options. Planning reads take it from the `ParquetSource` constructor, which the engine's resolver sets for file paths in queries. Callers that don't pass it parse as before.

## Consequences

- A cache hit costs one stat (about 0.2 ms) instead of a parse.
- A file overwritten with the same size and the same mtime more than 10 seconds after its previous write would serve the old footer. With ordinary file systems and writers this doesn't happen. `wvlet.engine.optimizer.disabled=parquet_footer_cache` turns the cache off.
- The distinct-count cache now stores the file version (size, mtime) each estimate was computed from and recomputes when it changes, following the same skip rules, with or without the toggle. It is LRU-bounded to 4096 (path, column) entries.
