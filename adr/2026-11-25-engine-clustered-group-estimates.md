# ADR: Estimate groups of clustered fact rows from sampled run lengths, not distinct sketches

Date: 2026-11-25 · PR #2456

## Context

Group counts were the product of the keys' distinct values, capped at the input rows. Fact rows come in clusters that share their keys: an order's lines share its customer and sold date. A group-by over filtered fact rows therefore has far fewer groups than either bound. TPC-DS q74's web leg estimated 1M (customer, year) groups where DuckDB counts 212,810, so the eager-aggregation guard skipped its partial aggregation. The work item suggested HyperLogLog or a GEE/Chao1 estimator over #2442's sampled row groups.

## Decision

- `TableSource.keyRunLength` measures the mean run of equal consecutive values of a column in the sampled row groups. `ClusteredGroups` (`clustered_group_estimates`) estimates R rows over D key combinations in clusters of r rows as D (1 − e^(−(R/r)/D)), Cardenas' formula over clusters.
- r is the shortest run among the keys and every column that selects rows on the probe path: filter columns, inner/semi/anti probe keys, and residuals. A filter that cuts clusters apart (rows ordered by the key, filtered by another column) then keeps the row estimate. A dimension column joined on a unique build key runs as long as the probe key it is a function of.
- Runs below 2, or unknown runs, keep min(D, R).
- No distinct sketch: the sample is a few contiguous row groups, so a sketch over it reads clustering as low distinctness, which biases GEE-style scaling. The group keys of interest usually span joined tables (customer id, year from date_dim), which a single-file sketch can't count together. A run length is one pass per column, cached per file version, and composes through joins.

## Consequences

- At SF10, q74 web estimates 214K groups (212,810 real), and its web leg aggregates below the customer join: wall 0.94–0.97, CPU 0.84–0.90. q68 and q46 inner groups are estimated near their real 7.4K and 34K. Both suites are flat at SF10.
- The formula assumes clusters spread uniformly over the key combinations, so skewed keys still overestimate.
- Accurate small aggregate estimates can reorder joins in ways the join search's cost doesn't price, such as nested builds that serialize (q46 wall 1.04–1.11, CPU flat).
