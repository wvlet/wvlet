# Engine: evaluate string filters once per Parquet dictionary entry

Work item: "Engine: evaluate string =, IN and LIKE once per Parquet dictionary entry (TPC-H q12, q16, q19; TPC-DS q02)". Follows #2409, where scans started keeping dictionary codes on `StrVec`.

## Findings

- uni's row filter already compares string `=` / IN once per dictionary entry (`ParquetFilter.RowFilter.compareAll`, `Key.Bytes`), so conjuncts moved into `row_filter` (#2404) need no change, and neither does uni.
- What remains per row is in the engine's `FilterOp`: LIKE / NOT LIKE, `!=`, string comparisons under OR over several columns (TPC-H q19's part filter), and any string `=` / IN over multi-file scans or with `scan_row_filters` off.

## Design

- New toggle `dictionary_filters` (default on). It has an effect only when scans keep codes (`dictionary_group_keys`).
- `DictionaryPredicates.wrap(cond)` wraps each maximal boolean subexpression over one string column (Compare, NullSafeDistinct, IS NULL, IN list, LIKE, AND / OR / NOT of those, literals) in a `PerDictEntry`.
- `PerDictEntry.eval`: when the column carries codes, the batch's distinct codes, one shared NULL entry, and each PLAIN row form a small batch. The condition runs on it and the results are gathered back to the rows by code. With more entries than half the rows, or without codes, it runs per row as before.
- `FilterOp` wraps its condition lazily at runtime (both the Selection and the full-mask paths). `sig` is the inner expression's, so plan text and golden plans don't change. The toggle is set through `Planner.configureFilters` and copied by `withSettingsOf`.
- Selection: `PerDictEntry` is safe and adds no cost of its own (its inner cost decides ordering), so conjunct order is unchanged.

## Not in scope (follow-ups)

- Exact selectivity of string predicates from the dictionary for `selective_scan_row_filters` (#2415 treats them as 0.3).
- Results cached across batches of one dictionary (uni exposes no dictionary identity).
- Build-side columns after a hash join lose codes (TPC-DS q02 day names, TPC-H q19's post-join filter).
- Deterministic string functions (`substr`, `lower`) inside the per-entry subexpression.
