# ADR: Join builds encode their string columns all or nothing, and aggregations key groups by the codes

Date: 2026-11-25 · PR #2430

## Context

- TPC-DS q47, q57, and q89 group by item, store, or call-center strings that arrive through hash-join builds, mixed with integer date keys. In the q47 SF10 profile, `GroupTable` (hashing, comparing, and storing strings) took 29% of engine CPU.
- The Parquet dictionary codes of `dictionary_group_keys` (#2409) only identify values within one vector: uni exposes no dictionary identity. They are also lost when a build concatenates its batches. The per-batch dictionary path takes only all-string keys with few combinations.
- Encoding costs a hash per row unless the input already carries Parquet codes. Builds such as TPC-DS `customer` (500K rows at SF10) hold unique id and name columns that no aggregation can group by codes. Encoding them every time made q04 3x slower.

## Decision

- `StrVec` gets an optional `dict`, the distinct values its codes stand for. Vectors that share a `dict` instance share the coding, so a code identifies a value across batches. `gather` carries the codes lazily.
- `JoinBuild` (`join_build_string_codes`) encodes the string columns of builds of at most 2^20 rows, one task per column. It hashes each Parquet dictionary entry once per batch; only builds of at most 2^16 rows hash rows that have no codes. Semi and anti joins never encode.
- All or nothing: a build keeps its codes only when every string column encodes, meaning at most half its rows are distinct. An aggregation uses the codes only when all its string keys carry them, and a build holds only the columns its join uses. A build with one unique column would only make probes gather codes nobody reads. A large build with a column without Parquet codes is rejected before any hashing.
- An aggregation table (`GroupTable(stableCodes = true)`) whose first batch's string keys all carry a `dict` keys its groups by those codes plus the other keys, in a `PackedGroupTable`, and reads each group's strings from the dicts once. A batch with other dicts, or without codes, moves the groups to the string stores in id order, and the table stays there.

## Consequences

- At SF10, q57 CPU is 0.79x and q47 0.92x, while the TPC-DS and TPC-H suites are flat. Group ids and their order match grouping by strings, so accumulators and results don't change.
- The partial aggregates' key vectors keep the codes, so the final aggregation also groups by codes. `PartitionedAggOp` still routes rows by string hashes.
- Builds over 2^20 rows (TPC-H `part`, `customer`) and scan-side string keys don't take this path. Scan keys need a dictionary identity from uni.
- Join probes that look up string keys (`GroupTable.lookup`) still hash strings.
