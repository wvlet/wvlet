# ADR: A join build gives up string codes after a short duplicate probe, and builds PLAIN Strings by row ranges

Date: 2026-11-26 · PR #2506

## Context

TPC-H q2's supplier join build (about 20K rows at SF10, below the 65536-row parallel build threshold) runs `join_build_string_codes` on the coordinator while every other thread waits. Its four string columns (`s_name`, `s_address`, `s_phone`, `s_comment`) are unique, so each one hashed `maxDistinct` = 10K rows before failing, and the all-or-nothing rule then dropped every column's codes. Before that, the parallel decode of supplier's single row group (`parallel_column_decode`, [ADR](2026-11-26-engine-parallel-column-decode.md)) ended with one task building the 100K UTF-8 Strings of `s_comment` after its decode. A JFR thread timeline showed ~6 ms per ~45 ms run with only the coordinator running.

## Decision

- `string_code_probe`: `StrVec.encodeParts` counts repeats after its first `ceil(sqrt(128 * maxDistinct))` non-null rows and gives the column up when fewer than 16 rows repeated a value. Rows drawn at random from exactly `maxDistinct` values repeat about k²/(2d) = 64 values in that prefix, so a column that would encode almost never falls below 16 (Poisson(64) < 16 is about 1e-14), and orderings other than random (sorted, clustered) repeat more. A nearly unique column now fails after ~1.1K rows instead of 10K. `JoinBuildCodes.encodeStrings` also skips the columns after the first that gives up, since the build keeps no codes then.
- The probe uses the repeat count, not the distinct ratio of a fixed prefix: a column with 40% distinct values in random order looks fully distinct over its first 4096 rows when it has 1M rows, so a fixed ratio would drop codes that aggregations use.
- `parallel_string_decode`: after a parallel decode's column tasks, the Strings of PLAIN string columns (no dictionary, at least 32K rows) are built by ranges of 16K rows as tasks of their own. Dictionary-coded columns keep their per-entry String cache on one thread.
- `ParallelDecodeMinBytes` stays at 2 MB. A sweep of the `parallel_column_decode` A/B at 512 KB, 1 MB, 2 MB and 4 MB (SF10, one JVM per value) gave q2 0.78, 0.73, 0.77 and 0.87, and q98 0.91, 0.93, 0.94 and 1.02: flat up to 2 MB, worse at 4 MB. TPC-DS q06, the reason for the 2 MB grouping, reads under 2 MB from each single-row-group table and decodes no column in parallel at any of these values. Its ±15% A/B swings are noise.

## Consequences

- TPC-H q2 runs at 0.92–0.97x wall at SF10 with `string_code_probe` alone (four JVMs) and 0.86–0.96x with both toggles on. `parallel_string_decode` alone is neutral (1.00): it moves `s_comment`'s Strings off the coordinator, but the Snappy decode of that column still bounds the stretch. q2's CPU ratio moves between 0.96 and 1.12 from JVM to JVM, as it does for `parallel_column_decode`. The TPC-H (q9, q18, q21) and TPC-DS (q04, q06, q11, q74, q98) controls are neutral.
- A column whose values all appear once before any appears twice (two concatenated copies of a unique list) loses its codes when it is longer than the probe. Only the codes are lost, not correctness.
- What stays serial in q2 is `s_comment`'s single Snappy page (it can't be split without decompressing again) and ~9 ms of planning on the caller thread.
