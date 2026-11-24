# ADR: Parquet decode stays pure Scala; fetch buffers are pooled in uni

Date: 2026-11-24 · wvlet/uni#1186 (uni 2026.3.10)

## Context

- At TPC-H SF10, uni's Parquet decode is 32–62% of the engine's CPU (q1, q6, q17, q21, async-profiler on main 473d2567). Snappy decompression alone is 27–31% in q6, q17, and q21. Memory zeroing inside decode is 3–8%: new value arrays (`LongBuffer.reserve`, `IntBuffer.reserve`) and a new `byte[]` for every fetched range (`RandomAccessFileSource.read`).
- uni's pure-Scala Snappy decodes TPC-H lineitem pages at 1.74 GB/s; snappy-java (native) runs at 3.40 GB/s. uni's ADR `2026-11-23-snappy-decoder-loop-and-string-buffer-sizing` found branch-free loops slower, and parsing the tags alone sets the floor (180 of 220 ms).
- `ParquetSource.toVec` already hands uni's `long[]` / `int[]` / `boolean[]` arrays to the engine's vectors without copying, so those arrays have to be new for every batch.

## Decision

- The engine keeps uni's pure-Scala decoders on every platform. It does not register a native Snappy (snappy-java) on the JVM, even though that would roughly halve Snappy's share of CPU, because it would add a native library to the engine and CLI.
- uni reads the merged column-chunk ranges of synchronous local reads into `FetchBuffers`, a per-thread pool (at most 4 buffers and 8 MB per thread, ranges up to 8 MB). The buffers return to the pool when the row group's batches end. Decoders copy values out of pages, so no vector points into a fetched buffer.

## Consequences

- TPC-H SF10 0.93x in total (RUNS=3). Rechecks with RUNS=10 gave minimums of 0.81x for q21 and 0.79x for q14, with mixed medians on a loaded machine. GC time fell, for example q14 from 24–30 ms to 8 ms. SF1 TPC-H and TPC-DS are neutral (0.99x).
- Snappy remains the largest single decode cost. Further work there should reduce how many bytes are decompressed (for example, one scan shared by q21's three lineitem reads) rather than speed up the decoder.
- Row groups with ranges larger than 8 MB (for example, Spark's default 128 MB row groups) still allocate a new array per read.
