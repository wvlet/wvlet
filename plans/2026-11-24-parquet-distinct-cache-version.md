# Key the Parquet distinct-count cache by file version

## Goals

- After a local Parquet file is rewritten, `ParquetSource.estimatedDistinct` should compute a new estimate instead of serving the old file's one (follow-up to #2365).
- The process-wide distinct-count map should have a size bound.

## Background

- `ParquetSource.distinctCache` is a `ConcurrentHashMap[(path, column), Option[Double]]` with no bound and no validation. File sources are new `ParquetSource` instances per query, so the cache is process-wide and keyed by path.
- #2365 added `ParquetFooterCache`, which validates footers by a stat (size + lastModified) taken before the read, and skips symbolic links, files without an mtime, and files modified within 10 s.
- Estimates only feed join ordering, so a stale estimate never changes query results; it can only pick a worse plan.
- A string-column estimate scans the column (up to 1M rows), so losing the cache for common files would slow planning.

## Design

- Lane: Express — an internal fix with a clear diagnosis, no always-gate area, no user-visible behavior beyond plan choice after a rewrite.
- Share one stat helper in `ParquetFooterCache` (file version = size + mtime, with the same symlink / no-mtime / recent-write rules) and use it in both caches.
- Each distinct-count entry records the file version it was computed from, stated before computing; a lookup whose current version differs recomputes.
- Files the footer cache would not cache (symbolic link, no mtime, written within 10 s) are not cached across queries: the estimate is kept on the `ParquetSource` instance (one query) only.
- Always validate, independent of the `parquet_footer_cache` toggle: the toggle stays a footer-only switch so its A/B isn't confounded, and a stat costs microseconds next to an estimate.
- Bound the map with an access-ordered LRU of 4096 (path, column) entries, guarded by a lock; compute outside the lock as today.
- Tests (JVM): a rewritten file (different size; same size with a different mtime) recomputes, an unchanged file reuses, a just-written file isn't cached.

## Alternatives and Why Not?

- Store estimates on the footer cache entry: ties them to the toggle, so turning it off would lose estimate caching (a column scan per call) and skew footer-cache A/B runs.
- Keep caching unstatable files by path only: keeps the staleness this change removes, for symlinked files.
