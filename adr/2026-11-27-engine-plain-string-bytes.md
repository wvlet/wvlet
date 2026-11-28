# ADR: Hold PLAIN Parquet strings as on-heap bytes, not off-heap canvases

Date: 2026-11-27 · PR #2587

## Context

- `StrVec` held one `String` per row. A SF10 probe (live class histograms at each query's peak, async-profiler allocation; `plans/2026-11-27-engine-compact-string-vectors.md`) found strings a minor share of peak heap: about 0% in TPC-DS q23/q78/q51/q59 and TPC-H q9, where `long[]`/`int[]` dominate, and 25–50% (an upper bound) in TPC-H q13/q18 and TPC-DS q04/q11/q74/q67.
- Dictionary-coded columns already share one `String` per entry, and `GroupTable` already keys shared-dictionary strings by packed codes. With compressed oops a `String[]` slot is as wide as an Int code, so moving them to codes saves no heap.
- PLAIN strings cost a `String` and a `byte[]` per row, and are 18% of TPC-H q13/q18 allocation. uni's `BinaryColumn` already exposes the page's `data` bytes and `offsets`.
- uni's `Canvas` was a candidate store: `Long` offsets, outside `-Xmx`, freed with `close()`. On JDK 25 (uni 2026.3.18, a shared FFM arena per canvas), a fresh canvas was slower than `new byte[]` up to 1 MB on both the Mac (Apple silicon) and a DGX Spark (GB10). At 64 KB it took 11 vs 0.7 µs on the Mac and 15 vs 1.2 µs on the Spark, and 164 µs / 34 µs with 12 busy threads, because closing a shared arena is a handshake. At 16 MB it was faster on the Spark (203 vs 275 µs). A pooled segment costs ~0. Hashing through `Canvas.readLong` was 18–40% (Mac) and 26–29% (Spark) slower than over `byte[]`. uni#1219 (unreleased) allocates with `malloc` and skips the zero-fill, but each close still costs ~19 µs.

## Decision

- `plain_string_bytes`: scans hold a PLAIN string column as uni's bytes and offsets (`StrVec.ofBytes`), shared and not copied. LIKE patterns of literal fragments and `%` match the bytes, so a filter-only column never builds Strings.
- A gather of a bytes vector is a view: the same bytes plus a composed row index. Copying bytes per output row would duplicate a dimension's strings across a join's fan-out (TPC-DS q67 ran 1.33x CPU with copies).
- Any String-level read builds the Strings of the source vector once, and views share them. A row is never decoded more often than the old scan decoded it, and equal rows stay one `String` instance, which keeps `equals` and the cached hash cheap in group-by keys.
- No `Canvas` for vectors. Vectors are shared across operators, threads, gathers and late materialization, with no owner to call `close()`; a missed release leaks memory outside `-Xmx` or frees memory still in use. Off-heap pooling fits single-owner buffers (spill blocks, join and group tables), and the spill framework's pool covers those.

## Consequences

- SF10 on a DGX Spark: TPC-H q13 holds 35% less peak live heap (1.26–1.34 vs 2.01–2.07 GB: its join build keeps `o_comment` for 15M orders, now as bytes), allocates 25% less, and runs at 0.87x CPU (0.71–0.86x on the Mac). q20 holds 20% less. The suite totals are unchanged (TPC-H 0.99x CPU, TPC-DS 1.00x). Columns grouped, joined or output as Strings (q18's `c_name`) see no change.
- Until its Strings are built, a view keeps its source page's bytes alive: a filter keeping few rows of a page still holds the page's bytes. A vector drops its bytes (and a view its source) once its Strings are built, so held state that reads Strings is counted and kept as Strings.
- Kernels that want the bytes read `StrVec.bytesView` once per batch (null once the Strings are built). Every other reader keeps working through `values` and `str`.
