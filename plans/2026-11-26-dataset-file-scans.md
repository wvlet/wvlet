# Iceberg tables and schema-evolved folders read file by file

## Goals

- Give Iceberg tables and schema-evolved Parquet folders parallel scans, planning statistics and file pruning, as #2502 and #2508 gave plain and Hive-partitioned folders. Today uni's dataset reader reads them on one thread, and planning gets no row counts or ranges.

## Background

- uni 2026.3.16 reads Iceberg (manifest list, manifests, position and equality deletes, field-ID column mapping, `initial-default`s) inside `ParquetDataset`. Public: `files` with partition values and manifest statistics (`DatasetFile.stats`: rows, per-column min/max/null count), `selectFiles` (prunes by partition values and manifest statistics), `withFiles` (a dataset of some files sharing the table's deletes and column mapping), and exact row filters (`ParquetReadOptions.withFilter`).
- Not public: each file's deletes, field-ID mapping and defaults, and reading a file's row groups one at a time. Without them the engine can't read an Iceberg data file as a plain single file without risking wrong rows.

## Design

- Lane: Express. The coordinator relayed the direction; the change is internal to the engine behind a new toggle, `dataset_file_scans`, and touches no always-gate area.
- `catalog/DatasetFiles.scala`: a local Iceberg table splits into one part per data file that `selectFiles` keeps. Each part reads its file through `dataset.withFiles(Seq(file))`, so deletes, column mapping and defaults stay uni's. Scans take exact row filters (uni applies them per file). Planning reads row counts, int/date ranges and one part range per file from manifest statistics. Value samples read the first rows of a few files spread over the table.
- A folder whose files hold only some of its columns (same types, any order) stays a `ParquetFolder` (uni's `withFiles` on a plain folder loses the folder-wide schema): it splits by row group like a partitioned folder, reads missing columns as NULLs, and drops filter conjuncts on them per file. No source row filter. Widened types stay on uni's dataset reader.
- An Iceberg table is relisted after the recheck interval and always reopened (its deletes may change without its file list changing).
- uni follow-up (needs a release): expose whether a data file reads as a plain file (no deletes, by-name columns, no defaults), so such files split into row groups and use row filters during decode through the single-file path. Requested under "Coordinator requests" in the work item.

## Alternatives and Why Not?

- Parse Iceberg metadata in the engine: duplicates uni's reader, including deletes and schema evolution.
- Read Iceberg data files as single files by name: wrong after a column rename or with deletes.
