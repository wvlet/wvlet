# ADR: A single row group's columns are decoded in parallel in groups of at least 2 MB

Date: 2026-11-26 · PR #2494

## Context

The engine parallelizes a Parquet scan by row group (`ParquetSource.scanSplits`, one morsel per row group). A file with one row group to read has no parts, so `MorselOp` runs the scan on one thread. TPC-H `supplier` at SF10 is a single 100K-row row group. In q2, its join build decoded `s_name`, `s_address`, `s_phone` and `s_comment` (Snappy, plain decoding, UTF-8 to String) on one coordinator thread for ~20 ms while every other thread waited, which made q2's wall time 1.6–1.8x DuckDB's at about the same CPU. The SF10 files hold one page per column chunk, so a chunk can't be split by rows without decompressing it again for each part.

## Decision

- Split by column, not by row range: column chunks are independent. Behind `parallel_column_decode`, `MorselOp` gives a scan it runs as a single part its runner (`ScanSpec.decodeRunner`), and `ParquetSource` decodes the row group's column groups as tasks of `TaskRunner.inParallel`. The calling thread takes the first group, so it never waits on queued work.
- Only without a row filter: the filter decides which rows every column decodes, so the columns can't be decoded independently.
- Group columns into tasks of at least 2 MB uncompressed (`ParallelDecodeMinBytes`, from the column chunk metadata), and read as before (`readBatches`) when that makes a single task. One task per column made TPC-DS q06's read of item's three small columns (~1.5 MB) cost 8% more CPU.

## Consequences

- TPC-H q2 went to 0.78–0.85x wall at SF10. The TPC-H suite and the TPC-DS queries that read single-row-group dimensions (item, date_dim) are neutral.
- The longest column task (`s_comment` in q2) and the build's narrowing and string encoding stay serial. Splitting a large string column's UTF-8 conversion by row range would be the next step.
- Native codecs remain out ([pure-Scala decode ADR](2026-11-24-engine-parquet-decode-pure-scala.md)): this cuts wall time, not decode CPU.
