# Read dictionary-encoded strings as entries only (dictionary_entry_scans)

## Goals
- Stop copying a dictionary entry's bytes for every row of a dictionary-encoded string column, which TPC-H q1 profiles show as ~7% (uni `BinaryBuffer.appendFromDictionary`) for `l_returnflag` / `l_linestatus`.
- Keep results identical, behind an Optimization toggle, and measure at SF10 on H q1, q10, q16, q3, q12 and DS q47, q57, q89, q63.

## Background
- uni decodes a dictionary page by copying each row's entry bytes into the batch's binary buffer, then records the entry index (#2409 / #2430 made the engine read the index and one shared String per entry).
- The engine reads strings only through `BinaryColumn.getString`, which already returns the entry's cached String; the per-row bytes are never read by the engine.
- So the copy is pure waste for the engine, whatever the consumers of the column are (grouping, filters, joins, output): no per-consumer analysis is needed.
- uni has no way to skip the copy today, so this needs a uni API (a uni release needs the user's OK).

## Design
- Lane: engine optimization loop item, pre-approved by the coordinator's work item; merge per the loop rules (green CI + no-regression SF10 A/B).
- uni: a read option `ParquetReadOptions.withDictionaryCodesOnly(columns*)`; for those top-level BYTE_ARRAY columns, dictionary-encoded rows keep only their entry index and hold an empty value in the raw buffer.
- uni: `BinaryColumn.getString` / `getBytes` / `length` / `getValue` read the entry for such rows, so the accessors return the same values; raw `offsets`/`data` are documented as empty for those rows.
- uni: columns a row filter reads are read as before, and when a chunk falls back from dictionary to PLAIN mid-buffer, the kept entries are copied in then (rare), so correctness never depends on the encoding mix.
- Engine: a new toggle `dictionary_entry_scans` (on by default) asks for this on every string column a Parquet scan reads; it is runtime-only (no plan change), so it gets an ignore entry in `EngineOptimizerToggleTest`.
- Tests: uni test compares codes-only reads with full reads (plain, filtered, PLAIN fallback, small batches); engine test compares on/off results for grouping, projection, filters and joins, and that the flag reaches the scans.
- Until the uni release, the engine builds against a local uni snapshot; the uni change and release go to the coordinator as a request.

## Alternatives and Why Not?
- Make engine `StrVec.values` lazy so even the per-row String array isn't built: touches every `StrVec` consumer; the item's measured cost is uni's byte copy, so take that first and leave this as a follow-up.
- Engine-only change: impossible, the copy happens inside uni's page decoder.
