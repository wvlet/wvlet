# Engine: Parquet decode cost at SF10

## Goals

- Cut the share of engine CPU that goes to uni's Parquet decode in lineitem-heavy TPC-H queries at SF10 (q21, q6, q17), where the engine is 1.8–3.6x DuckDB.
- Pick one fix whose gain the profile backs, not the zeroing figure quoted earlier.

## Background

- wvlet main is on uni 2026.3.9, which already contains every decode PR named in the brief (#1173–#1176, #1179, and #1183 for the footer cache). Only two docs and chore commits are newer on uni main, so there is no unadopted uni API to switch to.
- `ParquetSource.toVec` already hands uni's `long[]` / `int[]` / `boolean[]` arrays to `LongVec` / `IntVec` / `BoolVec` without copying. Only strings, unsigned `INT32`, `INT32` decimals, float, and timestamp unit conversions copy. None of those is hot here: lineitem decimals are `INT64`.
- async-profiler on main 473d2567, SF10, RUNS=3, one JVM per query, counting only engine and decode stacks:

| query | engine CPU | Parquet + codec | Snappy decode | zeroing in decode | zeroing in engine |
| --- | --- | --- | --- | --- | --- |
| q21 | 15 s | 49% | 27% | 8.0% (file read 3.0%) | 7.2% |
| q6 | 4 s | 62% | 27% | 8.0% (file read 1.7%) | 1.6% |
| q17 | 10 s | 51% | 31% | 3.0% (file read 1.0%) | 0.9% |
| q1 | 10 s | 32% | 6% | 5.8% (file read 0.7%) | 16.0% (decimal kernels) |

- The "~25% zeroing" figure was mostly Snappy: `Snappy$.decompressInto` is the top self frame in q21 (18%, plus 6% in `WideCopy`). `__bzero` under decode, from `LongBuffer.reserve`, `IntBuffer.reserve`, and `RandomAccessFileSource.read`, is only 3–8%.
- Allocation: `LongBuffer.reserve` plus `RandomAccessFileSource.read` account for 43% (q21), 69% (q6), and 78% (q17) of the bytes allocated in engine and decode stacks. The value arrays become the engine's vectors, so only the file-read buffers could be pooled.
- uni's own `SnappyPageBench` on SF1 lineitem pages (204 MB compressed, about 60M tags, mostly literals and copies of 1–8 bytes): uni decodes at 1.74 GB/s, snappy-java (native) at 3.40 GB/s. uni's ADR `adr/2026-11-23-snappy-decoder-loop-and-string-buffer-sizing.md` already measured branch-free loops (a tag table, a port of google/snappy's loop) as slower. Parsing the tags alone takes 180 of the decoder's 220 ms, which sets the floor for pure Scala on the JVM. A tag-table recheck today also lost (1.33 GB/s).
- wvlet has no snappy-java on its classpath today, and uni's `Codecs` has no hook for a caller to supply a decompressor.

## Design

- Lane: Standard. **Always-gate: packaging.** It adds a native dependency to the JVM engine and CLI (snappy-java unpacks its native library into the temp directory when it loads), and it needs a uni PR and a release first.
- This changes the brief's target from zeroing to Snappy, because the profile puts 27–31% of CPU in Snappy and only 3–8% in zeroing. You decide that on the card.
- Recommended: **native Snappy on the JVM, behind a hook**.
  - uni: `Codecs` takes an optional page decompressor for a codec, registered once per process. With none registered, behavior stays as it is: pure Scala on JVM, JS, and Native. One uni PR, with its own direction card and a release tag only after you approve.
  - wvlet: the JVM engine registers snappy-java's array decoder behind a new `native_snappy` `Optimization` toggle. The dependency and registration live in `wvlet-engine/.jvm/` behind an `XXXCompat` trait (see design/platforms.md). JS and Native keep uni's decoder. If the native library fails to load (an unsupported OS or architecture), the engine falls back to uni's decoder without failing.
  - Expected gain: Snappy's 27–31% of engine CPU would roughly halve, so q6, q17, and q21 should use 10–15% less CPU at SF10. A/B with `WVLET_ENGINE_BENCH_AB=native_snappy` at SF10 and SF1 will measure it.
  - Order: uni PR, then a uni release tag (asked first), then a wvlet PR that bumps `UNI_VERSION` in build.sbt and project/plugin.sbt, adds the toggle, and adds an ADR for the native dependency.
  - Cost: about 2 MB of snappy-java jar (with bundled natives) in the engine and CLI packages, and a native code path on the JVM.
- Not now: the zeroing work the brief proposed. Pooling uni's file-read buffers would save at most about 3% of q21 CPU. The value-array zeroing can't be removed while the engine keeps uni's arrays without copying.

## Alternatives and Why Not?

- Faster pure-Scala Snappy in uni: uni's ADR found that tag parsing alone takes 180 of 220 ms, and branch-free loops were slower. There is little left to win.
- Pool `RandomAccessFileSource.read` buffers in uni: 1–3% of CPU, but 20–33% of decode allocation. A follow-up if GC becomes the bottleneck.
- Share one lineitem scan across q21's three lineitem references: this would remove two of three decodes in q21 only. It is a large planner change.

## Preferences learned

- Context: choosing a fix for Parquet decode CPU at SF10. Chose: native snappy-java on the JVM through a uni hook (always-gate: packaging). User chose: the pure-Scala zeroing fix (pool uni's file-read buffers). Principle: keep the engine and uni pure Scala; don't add a native dependency for a CPU win (candidate entry for design/preferences.md).
