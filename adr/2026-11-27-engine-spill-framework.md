# ADR: Spill files hold the engine's own column blocks, past a soft threshold below the limit

Date: 2026-11-27 · PR #2584

## Context

- The engine counts each query's large holders (`MemoryContext`) and fails it with RESOURCE_EXHAUSTED past its limit; nothing degrades to disk. TPC-H and TPC-DS at SF100 need 10–21 GB live heap for their heaviest queries, more than a laptop JVM has.
- Join, aggregation, sort/window, and CTE spilling all need the same pieces: when to spill, where files go, how batches are written, and when files are deleted. Scala.js has no files; Native has files but no default heap cap.

## Decision

- **A soft threshold, not the hard limit, triggers spilling.** Spillable holders call `MemoryContext.tryReserve`, which returns false past the query's spill threshold; the holder spills and releases its child context. The hard limit stays the safety net for everything else. The default threshold is the smaller of the query limit and half the JVM's max heap; Native has none (no heap cap by default), Scala.js never spills. Pool pressure does not trigger spilling yet, so it doesn't race the low-memory killer.
- **Raw column blocks, not Parquet.** Spill files are written once, read once, and deleted in the same process. Each block is one batch with its columns as the engine holds them: primitive arrays, UTF-16 strings (exact for any `String`, no encoding pass), batch-entry strings as entries plus codes, BigDecimal decimals as unscaled bytes. `AnyT` columns are not spillable. No compression yet.
- **One owner-only folder per query.** `wvlet-spill-<random>/` under `java.io.tmpdir` (or `withSpillDirectory`), created on the first spill with owner-only permissions, and only the files the query created are deleted, with the folder, when its memory context closes (success, failure, kill); a shutdown hook covers process exit.
- **Platform code** lives in `wvlet-engine/jvm-native` (files, shared by the JVM and Native) behind `SpillCompat`; the block format is shared code.

## Consequences

- Later holders reuse the pieces: grace hash join partitions build and probe with the same hash-partitioned writer, external sort writes one file per sorted run, CTE results can be one file read by every reader.
- Query data reaches the local disk unencrypted while a spilling query runs, readable only by the process owner.
- Off-heap buffers (uni `Canvas`) were weighed and left out: a probe showed fresh canvases slower than heap arrays below ~16 MB, and the benefit is reuse; a pooled segment allocator is a separate item ([plan](../plans/2026-11-27-engine-spill-framework.md)).
- Spill write cost is partly tuned: file appends are serial per aggregation (encoding is parallel), each block gets a fresh encode buffer, and there is no compression.
- `-Dwvlet.engine.spillThreshold=<bytes>` sets the JVM default threshold, for CLI runs and benches.
