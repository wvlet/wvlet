# File-schema inference: sampling + cache (learning from DuckDB's binder)

## Context

Question: how does DuckDB plan queries that scan local files, and what can wvlet learn, given
that wvlet must scan the input file at compile time to obtain the schema?

### How DuckDB does it (research summary)

- DuckDB never *executes* to get a schema: `DESCRIBE` / `PREPARE` stop at the binder. But the
  table-function bind itself still does I/O:
  - **JSON** (`read_json`): samples up to `sample_size = 20480` records per file (up to
    `maximum_sample_files = 32` files), i.e. a file with < 20k records is fully parsed at bind.
  - **CSV**: sniffer over `sample_size = 20480` rows (24 dialect candidates, then type detection).
  - **Parquet**: reads only the footer (last 8 bytes → footer length → footer).
- Caching: `parquet_metadata_cache` caches parsed footers keyed on path and validated by
  ETag / `last_modified`; the 1.3+ external file cache caches byte ranges validated by ETag or
  mtime. **CSV/JSON sniff results are never cached** — every bind re-sniffs.
- Multi-file globs bind against the first file only unless `union_by_name=true`.

### What wvlet does today

Path: `Typer.structuralResolutionRule` (`wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/Typer.scala:816`)
→ `RelationRefResolver.resolveDataFileRef` (`.../analyzer/RelationRefResolver.scala:375`)
→ `JSONAnalyzer.analyzeJSONFile` (JSON) or `DuckDB.schemaOf` (parquet/csv).

- **Parquet/CSV**: `select * from '<path>' limit 0` — already bind-only (LIMIT 0 pipeline start
  is negligible). Fine. The dominant cost is opening a fresh in-memory DuckDB per call
  (`.jvm/.../duckdb/DuckDBCompat.scala:23-48`, same in `.js`/`.native`).
- **JSON**: `JSONAnalyzer` (`.../analyzer/JSONAnalyzer.scala:29-83`) reads the **entire** file into
  a String, parses all of it, and traverses every value. No sampling. This is the real gap vs
  DuckDB's 20 480-record sample.
- **No cache anywhere**: every Typer run (each LSP edit, each REPL statement) re-infers every
  `from 'file'`. `GlobalContext` (`Context.scala:41`) is per-`Compiler` and already hosts other
  per-compiler caches (`resolvedCatalogs`, `connectorCatalogs`), so it is the natural home.

### Two optimizations to adopt

1. **Sample JSON like DuckDB does** — stop after N top-level records instead of parsing the whole
   file.
2. **Cache inferred file schemas per compiler, validated by (path, mtime)** — mirrors DuckDB's
   `parquet_metadata_cache` / external file cache validation. Removes repeated DuckDB
   open/close and JSON parses across recompiles.

Out of scope (noted as follow-ups): reusing a single DuckDB session for inference across the three
platform backends; multi-file globs; `s3://` JSON.

## Design

### 1. `JSONAnalyzer` sampling

- Add `JSONAnalyzer.analyzeJSONFile(path, sampleSize: Int = DefaultSampleSize)` with
  `DefaultSampleSize = 20480` (same as DuckDB).
- Implement sampling with uni's streaming scanner instead of `JSON.parse`:
  `wvlet.uni.json.JSONScanner.scan(JSONSource, handler)` with a handler subclassing
  `wvlet.uni.json.JSONValueBuilder` (public ctor; `arrayContext`/`add` overridable). The
  top-level array context counts `add(...)` calls and throws a private control exception once
  `sampleSize` records are collected; the caller catches it and runs `guessSchema` on the
  collected prefix. Non-array roots (single object) go through the normal path unchanged.
- Keep the existing whole-file read (`SourceIO.readAsString` / `readGzipAsString`) — uni's
  `IO` has no streaming read and the parse, not the read, is the expensive part. Note this in
  a comment.
- `guessSchema(json: JSONValue)` stays public and unchanged (used by tests / other callers).

### 2. Per-compiler file-schema cache

- New `wvlet.lang.compiler.analyzer.FileSchemaCache` (shared source, cross-platform):
  ```scala
  class FileSchemaCache:
    private case class Key(path: String, lastModified: Long)
    def getOrElseUpdate(path: String)(infer: => RelationType): RelationType
  ```
  Key on absolute path + `SourceIO.lastUpdatedAt(path)` (already exists in all three
  `SourceIOCompat`s). Missing file → `lastUpdatedAt` returns 0 → do not cache (so a later-created
  file is picked up). Use a plain `mutable.Map` guarded by `synchronized` (compilers may be
  driven from multiple threads on the server). Evict the stale entry for the same path when the
  mtime changes.
- Hang one instance on `GlobalContext` (`Context.scala:41`): `val fileSchemaCache = FileSchemaCache()`,
  and expose `Context.fileSchemaCache`.
- `RelationRefResolver.resolveDataFileRef` wraps both branches:
  `context.fileSchemaCache.getOrElseUpdate(file)(JSONAnalyzer.analyzeJSONFile(file))` and the
  DuckDB branch likewise. Existing `DuckDBAnalyzer.guessSchema` remains the uncached entry point.

### Files

- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/JSONAnalyzer.scala` — sampling
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/FileSchemaCache.scala` — new
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/Context.scala` — add cache to `GlobalContext`
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/RelationRefResolver.scala` — use cache
- Tests (shared `wvlet-lang/src/test/...`, `UniTest`):
  - `JSONAnalyzerTest`: sampling stops at N records (generate an array of > N objects where a
    later record has a different type; assert the schema reflects only the prefix); a
    single-object root still works; `.json.gz` still works.
  - `FileSchemaCacheTest`: second lookup does not re-invoke `infer`; rewriting the file (new
    mtime) re-infers; missing file is not cached.
- Docs: no user-facing syntax change; add a short note to the `from 'file'` docs page about
  JSON sampling (first 20 480 records) so users know why a late-appearing field may be missed.

## Verification

- `./sbt "langJVM/testOnly *JSONAnalyzerTest *FileSchemaCacheTest *DuckDBAnalyzerTest"`
- `./sbt "langJS/testOnly *JSONAnalyzerTest *FileSchemaCacheTest"` (JSON path is pure Scala)
- `./sbt "runnerJVM/testOnly *RunnerSpecBasic"` — specs reading `spec/basic/*.json` still pass
- `./sbt "langJVM/testOnly *TyperCoverageCheck"` — ratchet unaffected
- `./sbt scalafmtAll` before commit

## Outcome (PR #2043)

Implemented as designed, rebased on #2042 (`DataFilePath`, jsonl/tsv/zst). Learnings:

- **uni `JSONScanner` contexts push values into their parent.** The scanner calls
  `stack.head.objectContext/arrayContext` to create child contexts and never calls `add` from the
  outside; each `JSONValueBuilder` child calls `$outer.add(result)` on `closeContext`. A wrapper
  that delegates `objectContext` therefore loses the elements — the limited array context must
  *be* the parent, i.e. subclass `JSONValueBuilder` and override `add`. `JSON.parse` scans with
  `builder.singleContext(...)` as the root handler, so the sampling root is likewise a tiny
  `JSONValueBuilder` subclass rather than the builder itself.
- `JSON.JSONNull` is not typed as a `JSONValue` in uni 2026.1.21, so an empty root holder is an
  `Option`, not a `JSONNull` default.
- Simplify-review consolidation: the four angles converged on (1) the 5-line root builder instead
  of an 11-method delegator, (2) one cached funnel through `DuckDBAnalyzer.guessSchema` instead of
  per-extension branches, (3) caching remote paths (mtime 0 otherwise meant "never cached" for the
  slowest inputs), (4) `ConcurrentHashMap` like the other `GlobalContext` caches, (5) byte-based
  `JSONSource` to skip the String round-trip.
- JS tests need `pnpm install` in a fresh worktree (`koffi` is loaded by `wvlet-lang.js` test
  bundle) — otherwise the runner exits with "Cannot find module 'koffi'".

Deferred: single DuckDB session for parquet/csv inference; bounded prefix read for huge JSON.
