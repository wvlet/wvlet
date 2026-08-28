# Support reading JSONL / NDJSON / TSV and compressed data files

PR: https://github.com/wvlet/wvlet/pull/2042

## Problem

`from 'file.jsonl'` (and `.ndjson`, `.tsv`, `.csv.gz`, `.jsonl.gz`, `.zst`) failed to type
in Wvlet although DuckDB's replacement scan reads all of them natively.

`RelationRefResolver.isDataFilePath` whitelisted only `.json`, `.json.gz`, `.parquet`, `.csv`.
Any other extension left the `FileRef` unresolved (`UnresolvedRelationType`), so column
references / `_.columns` could not resolve. `DuckDBAnalyzer.guessSchema` and
`JSONAnalyzer.analyzeJSONFile` mirrored the same hard-coded suffix checks.

## Design (as shipped)

1. `DataFilePath(format: Format, compression: Option[Compression])` in
   `wvlet-lang/.../compiler/analyzer/DataFilePath.scala` classifies a path once:
   - formats: `json`, `jsonl`, `ndjson`, `csv`, `tsv`, `parquet`
   - compressions: `gz`, `zst` — only on `compressible` formats (never `.parquet.gz`)
   - case-insensitive, requires a non-empty stem (`.json` dotfile is rejected), ignores URL
     `?query`/`#fragment` so S3 presigned URLs still classify
2. Routing (`DuckDBAnalyzer.usesJsonAnalyzer(path, dataFile)`):
   - local JSON-family, plain or gzip → pure-Scala `JSONAnalyzer` (cross-platform, no DuckDB)
   - everything else, including remote `s3://`/`https://` JSON → `DuckDB.schemaOf`
   - no DuckDB backend on the platform → reference stays unresolved (previous behavior for
     unknown extensions), never throws during typing
3. `JSONAnalyzer` handles JSON Lines: parses each non-blank line (10k-record sample cap) and
   raises `SYNTAX_ERROR` with the line number on malformed records.
4. The Typer's `structuralResolutionRule` calls `resolveDataFileRef` for every non-`.wv`/`.sql`
   `FileRef`; `None` means "not a data file", so no separate `isDataFilePath` guard exists.
5. SQL generation unchanged: `FileScan.sqlExpr` emits the quoted path, DuckDB sniffs the format.

## Tests

- Specs: `spec/basic/select-jsonl.wv`, `select-jsonl-gz.wv`, `select-csv-gz.wv`, `select-tsv.wv`
  with fixtures `person.jsonl(.gz)`, `people.csv.gz`, `people.tsv`
- `DataFilePathTest`, `DuckDBAnalyzerTest`

## Learnings from the PR cycle

- The first cut routed remote `https://x.jsonl` through `context.getDataFile`, whose existence
  check is local-only → would have thrown FILE_NOT_FOUND for URLs that previously executed fine.
  Remote paths must always go to the engine.
- Widening the extension whitelist silently turns "unresolved but executable" into
  "DuckDB.schemaOf throws" on platforms without libduckdb; guard with `DuckDB.isAvailable`.
- Classify the path once and thread the `DataFilePath` down; the naive version parsed it 4×
  (Typer guard, resolver, analyzer dispatcher, JSONAnalyzer).

## Deferred

- Streaming JSONL reads (needs a `SourceIOCompat` line reader per platform); today the whole
  decompressed file is read, same as `.json`.
- Write side (`ActivationSink.extensionOf`, `QueryExecutor` `.parquet` check) has its own
  extension table; unify on `DataFilePath`.
- Other DuckDB-readable formats (`.xlsx`, `.arrow`) remain unsupported.
