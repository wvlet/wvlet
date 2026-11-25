# Engine: group string keys by their Parquet dictionary codes

## Goals

- Cut the per-row string hashing and comparison in hash aggregation when every group key is a dictionary-encoded Parquet string column with few entries: TPC-H q1 (`l_returnflag`, `l_linestatus`) and q12 (`l_shipmode`).
- Keep this first step small: it changes no plan and no `Vec` consumer, and sits behind a new toggle. The wider dictionary work (filters, mixed keys, lazy strings) comes later and is listed under Follow-ups.

## Background

- The scan decodes every Parquet string into `StrVec` (`Array[String]`). uni 2026.3.12 already shares one `String` per dictionary entry and exposes `BinaryColumn.dictionaryIndex(i)` (the entry, or -1 for a null or PLAIN row) and `dictionarySize`. It does not expose a dictionary identity, so codes can only be compared within one batch.
- q1 SF10 profile on main 75c3f0f1 (16k engine CPU samples, load avg ~18):
  - `GroupTable` string work (`hashInto` + `find` + `sameKey` + `StrStore.equalsAt`) is about 5%, and building the `StrVec` (`toVec`) is about 2%. This PR targets that slice.
  - Accumulators (`SumDecimal`, `CountNonNull`, `CountStar` update) are about 13%.
  - uni's `ColumnVector.take` (filter-first path from #2404) is 26%, all self time. Its `pick` loop runs inside constructor arguments, so HotSpot skips OSR ("stack not empty at OSR entry point") and the loop runs interpreted until the tier-4 compile, which comes after the warm-up. That is a uni fix (bind the array to a local, as in #2319) and is recorded as a follow-up, not part of this PR.
- DuckDB (`TryAddDictionaryGroups`): it takes this path only for a single dictionary group column. It caches group pointers per dictionary entry, across vectors when the dictionary has an id. Without an id it takes the path only when the dictionary has fewer than half as many entries as the chunk has rows.

## Design

Lane: Standard, direction pre-set by the launch prompt (smallest first PR, dictionary-coded group keys for q1/q12, behind a toggle). Merge authorization comes from the loop rules.

- The scan attaches the dictionary codes to the `StrVec` it builds from a dictionary-encoded `BinaryColumn`: an optional code array plus the dictionary size. Values stay materialized, so every existing consumer is unchanged. The codes are dropped wherever a vector is rebuilt, except by `gather`, which carries them so they survive filters and join probes (q12 groups after a join).
- Contract: within one vector, two rows with the same code ≥ 0 hold equal strings. Code -1 says nothing (null, PLAIN page, or gathered padding).
- `GroupTable` dictionary path: when every key column carries codes and the product of the dictionary sizes is small (×2 ≤ the batch's rows, the same rule as DuckDB's no-id case), each row maps its combined code to a group id through a per-batch cache. Only the first row with each combination is hashed and found or inserted as today. Rows with a -1 code take the per-row path.
- Group ids, their order, and the key vectors stay exactly as the current path gives, so accumulators, partial-aggregate merges, and results don't change.
- New toggle `dictionary_group_keys` (default on), off restores today's scan and grouping. Listed in `OptimizerOptionsTest`. No plan change, so no golden plan diff.
- Tests: a `GroupTable` test comparing ids with and without codes (nulls, -1 rows, multi-key, large dictionary falls back), a scan test that a dictionary column carries codes, and conformance on spec/basic and TPC-H/DS through the existing suites. SF10 A/B on q1 and q12, plus q10 and q16 as unchanged controls.

## Alternatives and Why Not?

- **A new `DictStrVec` Vec class.** Every `match` on `StrVec` would need a case or a decode. It is the right end state for lazy strings and dictionary filters, but too broad for the first PR.
- **Cache group ids across batches.** It needs a dictionary identity, which uni doesn't expose. With few entries, a per-batch cache costs only a handful of lookups per batch. A uni accessor is a follow-up.
- **Pack dictionary codes into the existing `PackedGroupTable`.** Codes are only meaningful within one batch, and packed keys persist across batches.
