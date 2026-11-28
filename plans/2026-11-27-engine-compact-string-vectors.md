# Engine: compact string vectors, measured first

## Goals

- Cut the heap and allocation that strings cost where they matter for SF100, without slowing SF10.
- Decide the representation (on-heap bytes + offsets vs uni's off-heap `Canvas`) before migrating any kernel.
- Re-scope the item to what the measurements support: strings are a minor share of peak heap in most heavy queries.

## Background (measured 2026-11-27, main 9de8a9d5, SF10, bench lock, load ~10)

- Method: a scratch probe took a live class histogram (a full GC) every 20 ms during one warm run per query and kept the largest. Per-class bytes are the query's own share above the warm baseline. Allocation was profiled with async-profiler `alloc,total` on H q13/q18 and DS q67/q23/q04/q78, 3 runs each. Raw data, probe, and benchmark code are in `attachments/wvlet-engine-bench/string-heap-probe-sf10-2026-11-27/`.
- Peak live heap is mostly `long[]`/`int[]`, not strings:
  - DS q78 937 MB (`long[]` 814), q23 888 MB (`long[]` 716), q51 737 MB, q59 237 MB, H q9 546 MB (`int[]` 351 + `long[]` 191): strings ≈ 0%.
  - Strings matter in a few queries (String + `byte[]` + `String[]`, an upper bound since `byte[]` also holds Parquet buffers): H q13 193/393 MB, H q18 99/330 MB, DS q67 117/490 MB (almost all `String[]`), DS q04 128/277 MB, q11 85/185 MB, q74 82/151 MB.
- Dictionary-coded columns already share one `String` per dictionary entry. Their cost is the 4-byte `String[]` slot per row next to the 4-byte code (q67: 107 MB of `String[]`, 3 MB of `String`).
- PLAIN-encoded strings (`o_comment`, `c_name`, `c_customer_id`, …) cost one `String` + `byte[]` per row, about 76 B for a 28-byte value (24 B object + 48 B array + 4 B slot). Bytes + Int offsets would take about 32 B, a 2.4x cut for those columns only.
- Allocation: strings are 18% of TPC-H q13/q18 bytes (5.6 of 30.5 GB, all from uni's `BinaryColumn.utf8` building a `String` per PLAIN value). In TPC-DS they are ~6%: `String[]` 3.3 GB, 43% of it from `GroupTable.StrStore` growth and 24% from `StrVec.gather`. `long[]` (62%) and `int[]` (20%) dominate, led by uni's `LongBuffer.reserve` and `LongVec.gather`.
- Estimated effect of a full `StrVec` migration on peak heap: H q13 −20–25%, q18 −18%, DS q04/q11/q74 −25–30%, q67 −10–20%, the other heavy queries ~0. The item's "≥ 2x less heap for string columns" holds per PLAIN column, not per query, and GC pauses mostly track live array bytes, which strings barely add to.
- uni `Canvas` on JDK 25 (micro-benchmark, best of 5; 12 busy threads where noted):
  - Fresh canvas (`newOffHeapCanvas` + `close`) vs `new byte[]`: 64 KB 11.4 vs 0.7 µs (164 µs with busy threads: closing a shared arena is a handshake with every thread), 1 MB 20 vs 7.6 µs, 16 MB 270 vs 95 µs. Each canvas gets its own `Arena.ofShared()` and a `Cleaner` registration, and `arena.allocate` zero-fills.
  - A pooled segment (acquire/release without close): ~0.02 µs at any size. Zeroing it again (`fill(0)`) costs about what `new byte[]` does (16 MB 127 µs), so the gain is skipping both the allocation and the zero-fill.
  - Access over 1M strings as bytes + offsets: hashing through `Canvas.readLong` is 18–40% slower than `byte[]` with a VarHandle (bounds check + trait dispatch), and a raw `MemorySegment` is 13–22% slower. Equality is within ±7%. `String.hashCode` (cached) and `String.equals` (intrinsic) beat every byte path by 2–8x, so byte kernels must hash once per value and keep the hash.

## Design

Lane: Standard. It re-scopes an L item, picks the representation, and changes the item's done-when.

- Revision after approval (2026-11-27), from reading the code before step 2:
  - Step 2 is dropped. With compressed oops (heap < 32 GB) a `String[]` slot is 4 B, as wide as an Int code. `GroupTable` already keys shared-dictionary strings by packed codes (`stableCodes`/`packCodes`). q67's 107 MB of `String[]` is its result key vectors (4.84M base groups × 5 string keys × 4 B ≈ 97 MB), which take 4 B/row in either form. What remains is a few percent of allocation and GC tracing, not heap.
  - Step 1 needs no uni PR. uni's `BinaryColumn` already exposes `data: Array[Byte]` + `offsets: Array[Int]`, and the engine builds a `String` per PLAIN row in `TableSource.toVec`. A bytes-backed `StrVec` can wrap them without copying.
  - The first PR is step 1 alone. Byte kernels for LIKE/contains/prefix and equality filters let a filter-only column (q13's `o_comment`) never become Strings. `gather` copies the surviving bytes, so a vector never pins a whole page's data.
- Two targeted steps instead of a full `StrVec` migration (superseded by the revision above). Each step sits behind an `Optimization` toggle and gets its own A/B and conformance run (22/22, 99/99):
  - **Step 1: PLAIN strings as bytes.** uni hands PLAIN BINARY pages to the engine as UTF-8 bytes + Int offsets per batch (a uni PR first). `StrVec` gains a bytes-backed form that builds a `String` only when a row is read through `str`/`values`. `like`/`contains`/prefix, equality and hashing read the bytes directly. Targets: H q13 (`o_comment not like`), H q18, DS q04/q11/q74.
  - **Step 2: Stop forcing `values` on dictionary-coded vectors.** `gather` already keeps codes + entries (`lazy_dictionary_strings`). The `String[]` comes from callers that force `StrVec.values` (0.5 GB of DS `String[]` from `decodeEntries`): `GroupTable.StrStore.hashInto` and its growing key `String[]`, `Batch.concat`, `JoinBuildSteps`, and a `VExpr` kernel. These callers read codes + entries instead, and `StrStore` keeps shared-dictionary keys as codes. Target: DS q67 and the `StrStore`/`gather` `String[]` allocation.
- Each step updates `MemoryContext`'s string size estimate (today 40 B per `String` + 8 B per slot) for the new forms and revises design/engine.md's Memory accounting section in the same PR. An ADR records "on-heap bytes + offsets, no `Canvas` for vectors". `StrVec` changes also run `engineGpu/Test/compile`, since `GpuTypes` reads `StrVec` and engineGpu is not in CI.
- Representation: on-heap `Array[Byte]` + `Array[Int]` offsets per batch. A batch is far below 2 GB, so `Long` offsets are not needed. It is the same code on JVM, JS and Native, and needs no new JDK floor.
- Kernels hash each value once per vector and cache the hashes next to the offsets, since `String` caches its hash and byte hashing per probe would lose to it.
- No `Canvas` for string vectors:
  - Vectors are shared freely across operators, threads, `gather` and late materialization, with no single owner to call `close()`. A pooled or off-heap buffer would need reference counting on every vector, and a missed release either leaks memory outside `-Xmx` or frees memory still in use.
  - Fresh canvases were slower than arrays at every measured size (64 KB–16 MB). The hash loops lose 18–40%, and JDK 17–21 library users would need the heap fallback anyway.
- Off-heap and pooling belong where buffers have one clear owner: spill blocks, join flat tables, and group tables. There, skipping the zero-fill already shows in profiles (`LongGroupTable`/`GroupTable.emptySlots` 3 GB of TPC-DS allocation). That work is the spill session's proposed `SegmentPool` item: a slab pool of large reused segments, plus a uni PR for pooled or non-zeroing (`malloc`-backed) allocation, since FFM arenas always zero-fill and each shared-arena close is a handshake. A confined arena (`Arena.ofConfined`) ties `new byte[]` at 64 KB (0.63 vs 0.72 µs) with no handshake, which makes it an option for single-thread owners.
- New done-when, from the measured estimates (probe re-run per step, with SF10 totals no slower):
  - Step 1: H q13 ≥ 20% and DS q04/q11/q74 ≥ 20% less peak live heap. H q13/q18 ≥ 10% less allocation. Each PLAIN string column ≥ 2x smaller.
  - Step 2: DS q67 ≥ 10% less peak live heap, and the TPC-DS `String[]` allocation halved.
- The memory goal should lean on releasing consumed state and on spilling: `long[]`/`int[]` held by join builds, aggregation inputs and decoded fact columns decide whether DS q23/q78/q51 and H q9 fit at SF100. I'll note this in the goal's log.

## Results of step 1 (`plain_string_bytes`, SF10)

- DGX Spark (GB10, A/B in one JVM, 5 runs; outliers re-run with 9): TPC-H 0.99x CPU total, TPC-DS 1.00x, rows match. H q13 0.87x CPU, others within noise.
- Heap and allocation probe on the Spark: H q13 peak live heap −35% and allocation −25%; H q20 peak −20%, allocation −8%. H q18 is flat because it groups by `c_name`.
- Two first versions regressed and were fixed. Copying gathered bytes duplicated dimension strings across join fan-out (DS q67 1.33x CPU), so gathers became indexed views that share the source's Strings. Decoding single-row reads fresh decoded rows twice (H q22 1.14x CPU), so String-level reads build the source's Strings once.

## Alternatives and Why Not?

- **Full `StrVec` migration (the original item).** Weeks of kernel work across hash, compare, gather and copy for 0% in most heavy queries. Steps 1–2 capture most of the measured string bytes.
- **`Canvas`-backed string bytes.** Faster only with a pool, and pooling needs batch ownership that vectors don't have. It is slower in hash loops, and off-heap needs JDK 22+.
- **StringView-style 16-byte inline prefix.** It helps compare-heavy joins on short strings, but adds a second layout to every kernel. Revisit only if step 1 shows hash/compare as the cost.
- **Close the item as no-go.** That would leave TPC-H q13/q18's 18% string allocation and the q04/q11/q74 heap on the table. Both are cheap to address with steps 1–2.
