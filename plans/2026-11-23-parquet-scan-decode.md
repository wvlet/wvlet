# Cut the next Parquet scan cost: uni's decode temporaries, not engine Strings

## Goals

- Find and remove the next Parquet scan cost on TPC-DS SF1, as a follow-up to the uni #1168 bump. The engine is 3.0–3.2x slower than DuckDB in total, and both read the same Parquet files.
- Let the profile pick the direction. The brief's candidates were a binary- or dictionary-backed `StrVec`, late materialization of strings, and cheaper scan splits.

## Background

Profile: async-profiler (CPU at 2 ms, allocation at 512 KB) over one full `EngineTPCDSBench` run at SF1, with 2 timed runs per query, on the 7ff9cf33 base with local uni 2026.3.5-local1168. The machine was loaded (load average around 16), so the shares below matter more than absolute times.

- Parquet decode is the largest scan cost, and almost all of it is inside uni:
  - It takes 43% of the CPU on the engine worker threads (15.7k of 36.6k samples), and GC threads add another 5.7k samples.
  - It allocates 79 GB of the run's 158 GB.
- Engine-side String materialization is negligible: `ParquetSource.toVec` and `getString` take 0.5% of CPU and 0.6 GB (0.4%) of the allocation. That rules out a binary-backed `StrVec` and late string materialization for now.
- Scans are already split by row group and run on the engine workers (87% of decode CPU is there). Splits are not the bottleneck.
- The fact tables are mostly nullable. store_sales has 12 nullable INT32 `decimal(7,2)` columns, 8 nullable INT64 keys plus a nullable INT64 `ss_quantity`, and 2 required INT64 keys (handed over without a copy) out of 23, in row groups of about 120k rows. catalog_sales and web_sales look the same.
- Where uni's 79 GB goes:
  - 21 GB of `long[]` value buffers, and 2.9 GB of `boolean[]` null masks. Both become the vectors, so they are needed.
  - 10.4 GB of dictionary-index `int[]`. Each column chunk's cursor grows one array, by doubling, to its largest page. It is a temporary: indices are expanded, then looked up in the dictionary.
  - 8.4 GB of definition-level `int[]`, one Int per row. They are temporaries that are turned into the null mask.
  - INT32 decimals read as Long (`withInt32DecimalAsLong`) are decoded into an `int[]` (3 GB, a temporary) and then widened into a new `long[]` (7 GB, the vector). This bypasses #1168's in-place spread, which only same-width columns get. The widening loop in `Assembly.directLeaf` is also the largest single uni CPU frame: about 9% of decode samples, run in the interpreter.
  - 9.8 GB of raw chunk bytes and 6.8 GB of Snappy output. These are per-chunk and per-page buffers.
- In CPU, zeroing the new arrays (`__bzero`, `zero_blocks`: about 3k samples), the scatter pass, and RLE fills of the index and level arrays (`jint_fill`) together make up about a quarter of the decode samples. Boxing of the `(buf, off)` tuple on the page-read path adds about 3%.

## Design

- Lane: **Standard**. The fix lives in uni, which I must not edit without your approval. The wvlet-side work depends on a uni change that isn't released.
- Direction: propose a uni PR that removes the decode temporaries, and leave `ParquetSource.toVec` as it is. Ranked by allocation saved and risk:
  1. Dictionary indices are decoded and looked up in fixed blocks (for example 1024), so a chunk's index array no longer grows to its largest page. This saves about 10 GB and most of the `jint_fill` time.
  2. Levels are kept one byte per entry instead of one Int. 1-bit levels are expanded straight from the packed bits, and range checks and the non-null count run in the same loop. This saves about 6 GB and two passes. (In the implementation, uniform Byte levels replaced a null-mask path that would only cover flat columns.)
  3. INT32 read as Long decodes straight into a `LongBuffer`. Plain values are widened as they are decoded, and dictionary values go through a Long dictionary. Nullable columns then get #1168's in-place spread, and required columns hand over the buffer. This saves the 3 GB `int[]` and the widening pass, which is the largest single CPU frame. The `long[]` vector stays.
  4. The page-read path returns the buffer and offset without a boxed tuple. This is a small, cheap fix.
  - Pooling the raw chunk and Snappy buffers (about 16 GB) waits. Buffer lifetime across splits needs more care, and its gain is mostly GC time.
- Expected effect, as an estimate to confirm: uni's scan allocation drops from 79 GB to about 58 GB, and decode CPU falls by about a fifth. On TPC-DS SF1 that is roughly 5–10% of engine time, more on scan-heavy queries such as q78, q87, and q95.
- Engine side (this wvlet PR): no `Vec` or kernel change. After the uni change is published locally, the PR re-runs the profile and the SF1 A/B against 7ff9cf33, alternating rounds, and states the result. If a uni API changes, it adjusts `toVec`. It stays stacked on the bump branch, and it never commits the local uni pin.
- Validation in uni: unit tests that compare items 1–4 against the current decode on nullable, required, dictionary, and plain pages, plus a full-scan probe of store_sales for allocation and time.

## Alternatives and Why Not?

- **Binary- or dictionary-backed `StrVec`:** it targets 0.5% of the CPU and every string kernel would need a second path. Revisit it when a string-heavy workload shows up.
- **Late materialization in the engine:** decoding is the cost, not materialization. Row groups of 120k random rows rarely skip whole pages, so the engine can't avoid the decode.
- **Int-backed decimal vectors in the engine:** these would avoid the widening copy, but every decimal kernel would need an Int path. Fixing it in uni (item 3) is local to one decode path.
- **Cheaper splits or more row-group parallelism:** scans already run per row group on the workers, and each split's footer and metadata work is under 2% of decode.

## Result

- uni PR: wvlet/uni#1171, with items 1–4 plus the byte-level decode.
- `EngineParquetScanBench` measures every column of the SF1 fact tables, read single-threaded through `ParquetSource`. Allocation fell from 4058 MB to 2729 MB (−33%), and it was the same in every round. CPU fell about 10–15% in the quieter A/B rounds on a loaded machine (load average 13–18).
