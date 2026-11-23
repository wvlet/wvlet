# M5: Runtime join filters through UNION ALL

## Goals

- A hash join's range filter (`RuntimeFilter`, PR 7) also reaches the Parquet scans under a `UNION ALL` on its probe side, so each input scan skips row groups outside the build keys' range.
- Target: SF1 TPC-DS q05 and q54 read `catalog_sales` (clustered by `cs_sold_date_sk`) through a union joined with a filtered `date_dim` (15 days and one month), and read every row group today.

## Background: which follow-up skips row groups at SF1

The follow-ups recorded under PR 7 were checked first against SF1 data (`parquet_metadata` per row group) and SF1 plans (`WVLET_ENGINE_BENCH_EXPLAIN=1` over the 43 TPC-DS queries that read `catalog_sales` or `inventory`).

- Only `inventory.inv_date_sk` (96 row groups) and `catalog_sales.cs_sold_date_sk` / `cs_ship_date_sk` (12) have row-group ranges narrower than the key's whole range among join keys. `customer_demographics.cd_demo_sk` and the order/ticket numbers are clustered too, but their builds span the whole range.
- No string column has row-group ranges that differ between row groups, and every table with string join keys has a single row group, so IN-set filters for strings would skip nothing.
- Unfiltered date-keyed scans at SF1: q05 and q54 (through a union: this PR), q37, q82 and q29 (the filtered `date_dim` is the probe side of its join and the fact scan is in the build: no runtime filter direction reaches it), q02 (the date join is under an aggregation keyed by `d_week_seq`, and `date_dim` there is unfiltered), q16's second `catalog_sales` scan and q64 (no date key). None of these needs filters through aggregation group keys, onto build sides, or into shared CTE bodies.

## Design

- Lane: Express. An internal optimizer change under the existing `runtime_join_filters` toggle; merge is pre-authorized by the launch prompt.
- `Lowering.scanColumns` (was `scanColumn`) returns every (scan column id, Parquet column) a probe key column reads, and follows each input of a `Union` at the key's position. An input column cast to a wider type is skipped. Each output row of a UNION ALL is one input row with the key's value unchanged, so dropping input scan rows outside the range only drops rows the join drops.
- The join creates one `RuntimeFilter` per target scan (they share the key index), and pending filters are keyed by the scan's own column id instead of the probe key's id.
- Golden plans change by the `join_filter=` annotation only: q05, q54, q71.

## Not in this PR

- IN-set filters, filters through aggregation group keys, onto build sides, and into shared CTE bodies: no SF1 plan would skip row groups (see Background).
- `Distinct` / `SetOp` inputs: no plan reaches them.

## Alternatives and Why Not?

- Push the date range statically into the union inputs: the range comes from the build's keys (a filtered `date_dim`), which only the runtime knows.
