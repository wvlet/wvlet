# Engine: group estimates from sampled key clustering

Work item: `2026-11-26-engine-group-count-and-distinct-estimates-from-sampled-parqu` (TPC-DS q74, q11, q04, q68, q70).

## Problem

- Group counts are the product of the keys' distinct values, capped at the input rows. Fact rows come in clusters that share their keys (an order's ~12 web_sales lines share the customer and the sold date), so a group-by over a filtered fact table has far fewer groups than either bound.
- q74's web leg: (ws_bill_customer_sk, d_year) is estimated at 1M (500K customers × 2 years); DuckDB counts 212,810. The eager-aggregation guard then rejects the partial aggregation (#2445).
- q68: (ticket, customer, addr, city) is capped only by the 76K input rows; the real count is 6.9K.

## Approach

- `TableSource.keyRunLength(column)`: the mean run length of equal consecutive values of a column in the sampled row groups (the same up-to-4 row groups / 250K rows as #2442's `valueCounts`), cached per file version. web_sales' ws_bill_customer_sk and ws_sold_date_sk both have runs of 11.9.
- `Stats.clusterRun(p, keys)`: the run of the key tuple in p's probe stream: the minimum over the keys and over the probe-path columns that select rows (filter columns, inner/semi join probe keys), so that a filter that cuts clusters apart (data sorted by the group key, filtered by another column) falls back to the row estimate. A column from a join's build side counts as a function of the probe keys when the build key is unique.
- With a run r ≥ 2, the groups of R input rows over D key combinations are D·(1 − e^(−(R/r)/D)) (Cardenas' formula over R/r independent clusters). Otherwise the estimate stays min(D, R).
- Used by `Stats.treeRows`' aggregation estimate (join search, build sides, semi-join placement) and `EagerAggregation`'s partial-group estimate.
- Behind a new Optimization, `clustered_group_estimates`.

## Validation

- q74 web leg at SF10: 1M·(1 − e^(−2.87M/11.9/1M)) ≈ 214K vs 212,810 real.
- Golden plans regenerated; SF10 A/B on DS q74, q11, q04, q68, q46, q70, q01, q03, q42, q52, q55 and H q10, q18, q3, plus a suite no-regression check.

## Decisions

- Run-length clustering with Cardenas' formula instead of an HLL/GEE distinct sketch over the sample: the sample is a few contiguous row groups, so a sketch over it sees the clustering as low distinctness and GEE would be biased by it; and the group keys usually span joined tables, which no single-file sketch can count together. The run length is one cheap pass per column and composes through joins.
