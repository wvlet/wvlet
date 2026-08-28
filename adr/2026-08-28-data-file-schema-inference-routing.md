# ADR: Routing of `from '<file>'` schema inference

Date: 2026-08-28 · PR #2042

## Context

`from '<path>'` needs the file's columns at compile time so downstream expressions type-check.
Two schema sources exist: the pure-Scala `JSONAnalyzer` (reads and parses the file itself,
runs on JVM/JS/Native) and `DuckDB.schemaOf` (`select * from '<path>' limit 0`, needs a DuckDB
backend — absent on Scala.js without `libduckdb`). Before this change the choice was made by
hard-coded `endsWith` checks scattered over three files, and only four extensions were accepted
even though DuckDB reads many more (`.jsonl`, `.ndjson`, `.tsv`, `.gz`/`.zst` variants).

## Decision

- `wvlet.lang.compiler.analyzer.DataFilePath` is the single classifier: `(Format, Compression?)`
  parsed once from the path and threaded through `DuckDBAnalyzer.guessSchema` and
  `JSONAnalyzer.analyzeJSONFile`.
- Routing rule (`DuckDBAnalyzer.usesJsonAnalyzer`):
  1. **local** JSON-family file, plain or gzip → `JSONAnalyzer` — keeps JSON typing working on
     every platform and gives a proper `FILE_NOT_FOUND` for missing files;
  2. anything else — csv/tsv/parquet, `.zst`, and **any remote** `s3://`/`https://` path — →
     `DuckDB.schemaOf`, because DuckDB fetches remote files itself and a local existence check
     would wrongly reject them;
  3. if DuckDB is unavailable on the platform, leave the `FileRef` unresolved rather than throw,
     so the query still compiles and the engine reads the file at run time.
- Compression suffixes attach only to `compressible` formats; `.parquet.gz` is not a data file.

## Worked examples

- `from 'person.jsonl.gz'` → JSONAnalyzer, gzip decoded, line-delimited parse (`5d29a091`,
  `64e6e503`).
- `from 'https://host/events.jsonl'` → DuckDB, no local stat (`c8db1fc2`).
- `from 'people.csv.gz'` on Scala.js without libduckdb → unresolved FileRef (`c8db1fc2`).

## Consequences

- JSONL inference reads the whole decompressed file before sampling (same as `.json`); a
  streaming reader per platform is a follow-up.
- The write side (`ActivationSink`, `QueryExecutor`) still has its own extension table; it
  should converge on `DataFilePath`.
- Adding a format is one `Format` enum row plus, if DuckDB cannot sniff it, a routing tweak.
