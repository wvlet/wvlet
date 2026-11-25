# Engine: group join build strings by codes shared across batches

## Goals

- Cut the string hashing, comparing, and storing in hash aggregations whose string keys come from hash-join build sides: TPC-DS q47, q57, q89 (`i_category`, `i_brand`, `s_store_name`, `s_company_name` with `d_year`, `d_moy`), TPC-H q16.
- Cover mixed string and integer keys, which the per-batch dictionary path of #2409 does not.

## Background

- q47 SF10 profile on main 03036797: `GroupTable` is 29% of engine CPU (find, insert, `StrStore.hashInto` / `equalsAt` / `addValue`), mostly in the per-morsel partial aggregation, whose tables see few rows per group.
- The keys come through three joins, so the Parquet codes of #2409 are gone (build columns are concatenated), and the per-batch path would not take them anyway: the key combinations outnumber half a batch's rows, and two keys are integers.
- TPC-H q10's group keys are mostly unique customer strings: `GroupTable` is about 5% of its CPU, and Parquet decode dominates. Codes don't help there; it stays a control.
- uni's `BinaryColumn.getString` already shares one `String` per dictionary entry, so the lazy-string part of the item (#2417's 4.4% of q1) is uni's per-row byte decode, not engine work.

## Design

Lane: Standard, direction pre-set by the work item (mixed keys, codes through join builds, lazy key strings, behind a toggle). Merge authorization comes from the loop rules.

- `StrVec` gets an optional `dict: Array[String]`: the distinct values the codes stand for. Vectors sharing a `dict` instance share the coding, so a code identifies a value across batches. `gather` carries it.
- `JoinBuild` encodes each string column of a build of at most 2^20 rows once, after concatenating it (`StrVec.encode`, one `HashMap` pass). Probe outputs gather the codes with the strings.
- `GroupTable(stableCodes = true)`, used by `HashAggOp` and DISTINCT: when the first batch's string keys all carry a `dict`, the groups are keyed by the codes (as Int32 columns) and the other keys in a `PackedGroupTable`, so they take the single-Int64 path, and no string is hashed or stored. `keyVectors` reads each group's strings from the dicts once and keeps the codes and dict, so the final aggregation over the partials takes the same path. A batch with other dicts or without codes moves the groups to the string stores in id order (ids stay valid) and the table stays there.
- Join builds' own key tables don't set `stableCodes`, so probes never change them.
- New toggle `join_build_string_codes` (default on): off keeps build strings as they are. No plan change (toggle-test ignore entry).

## Alternatives and Why Not?

- **A second code-keyed table in front of the string table** (first version). Every new group was inserted twice; with few rows per group in the morsel partials, q47 CPU went to 1.13x.
- **Codes from the build row index.** They need no hashing, but every item row gets its own code, so the code combinations outnumber the groups.
- **A dictionary identity from uni for Parquet codes.** That would extend the cross-batch path to scan columns (TPC-H q1); uni changes go through the coordinator.
