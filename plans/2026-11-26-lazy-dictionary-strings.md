# Lazy dictionary strings (TPC-H q1 / q5 profile at SF10)

Lane: Express (engine optimization loop; merge authorized after green CI and a no-regression SF10 A/B).

## Profile (SF10, main cc01ad86, async-profiler cpu, 30 runs)
- q1 (1.7x DuckDB): engine CPU is Snappy 17%, `FusedAccumulators.fewGroups` 14%, `ParquetSource.stringsWithCodes` 9.6% (per-row `getString` + `dictionaryIndex` + String[] stores), uni dictionary decode of numeric columns (`BitPacking.unpackInts` 7%, `appendFromDictionary` 6%), `decimalArith` 7%, `GroupTable.dictionaryIds` 4.7%.
- q5 (1.9x): Snappy is ~45% of engine CPU (lineitem `l_orderkey`/`l_suppkey`/`l_extendedprice`, 1.4 GB uncompressed); then uni `RowFilter.memberRows` 3%, `RowKeyFilter.mayContain` 1.7%, `LongJoinTable.insertAll` 1.6%. Nothing engine-side is large; the Snappy bytes are needed (orders' date filter keeps rows in every lineitem page).

## Change
- `lazy_dictionary_strings`: dictionary-coded Parquet string vectors hold the rows' codes and the batch's entry strings (`StrVec.ofEntries`); `values` is built on first use. Grouping by codes (`dictionaryIds`, `StrStore`) and per-entry filters read single rows through `StrVec.str(i)`, so q1 never builds the per-row array.
- Falls back to the per-row path when a non-NULL row has no code (dictionary → PLAIN fallback).

## Validation
- `EngineLazyDictionaryStringsTest` (same strings/codes as eager reads, gather keeps entries, group/filter/join queries match with the toggle off); toggle listed in `OptimizerOptionsTest` and `EngineOptimizerToggleTest` (no plan change).
- SF10 A/B on H q1, q5, q3, q7, q8, q10, q14.
