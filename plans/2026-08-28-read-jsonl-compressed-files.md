# Support reading JSONL / NDJSON / TSV and compressed data files

## Problem

`from 'file.jsonl'` (and `.ndjson`, `.tsv`, `.csv.gz`, `.jsonl.gz`, `.zst`) fails to type
in Wvlet although DuckDB's replacement scan reads all of them natively.

`RelationRefResolver.isDataFilePath` (wvlet-lang/.../analyzer/RelationRefResolver.scala:367)
whitelists only `.json`, `.json.gz`, `.parquet`, `.csv`. Any other extension leaves the
`FileRef` unresolved (`UnresolvedRelationType`), so column references / `_.columns` can't
resolve. `DuckDBAnalyzer.guessSchema` and `JSONAnalyzer.analyzeJSONFile` mirror the same
hard-coded suffix checks.

## Design

1. Introduce a `DataFilePath` helper that splits a path into `(baseExtension, compression)`:
   - compression suffixes: `.gz`, `.zst` (DuckDB auto-detects both for CSV/JSON)
   - base extensions: `json`, `jsonl`, `ndjson`, `csv`, `tsv`, `parquet`
2. `isDataFilePath` = base extension is in the set above (compression optional).
3. Schema inference routing (`resolveDataFileRef` / `DuckDBAnalyzer.guessSchema`):
   - JSON family (`json`/`jsonl`/`ndjson`) with no compression or `.gz` → `JSONAnalyzer`
     (pure Scala, works on JS too). Extend `JSONAnalyzer` to parse line-delimited JSON:
     for `jsonl`/`ndjson`, parse each non-blank line as a JSON value.
   - Everything else (csv/tsv/parquet, any `.zst`) → `DuckDB.schemaOf(path)`.
4. SQL generation is unchanged: `FileScan.sqlExpr` emits the quoted path, DuckDB handles it.

## Tests

- `spec/basic/person.jsonl`, `person.jsonl.gz`, `people.csv.gz`, `people.tsv` fixtures +
  `select-jsonl.wv`, `select-jsonl-gz.wv`, `select-csv-gz.wv`, `select-tsv.wv` specs.
- `DuckDBAnalyzerTest`: jsonl dispatches to JSONAnalyzer; csv.gz through DuckDB.
- Unit test for the extension parser.

## Decisions

- Keep JSONAnalyzer for JSON (not DuckDB) so Scala.js keeps working without DuckDB.
- `.zst` inference goes only through DuckDB (no pure-Scala zstd decoder).

## Open questions

- Other DuckDB-readable extensions (`.xlsx`, `.arrow`) are out of scope.
