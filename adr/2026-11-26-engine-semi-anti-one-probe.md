# ADR: Probe a table once for an EXISTS and a NOT EXISTS over it, instead of caching decoded row groups

Date: 2026-11-26 · PR #2492

## Context

- TPC-H q21 was 1.6–1.8x DuckDB at SF10 and read lineitem three times: the outer rows `l1`, the EXISTS over `l2` (a RIGHT SEMI join building on the outer rows), and the NOT EXISTS over `l3` (a RIGHT ANTI join building on the semi join's output). Without its NOT EXISTS, q21 took 208 vs 305 ms.
- The first proposal (2026-11-24) was a per-query cache of decoded row groups, shared by scans of the same Parquet file. The three scans run one after another, though: each one's output builds the next join, so the cache would have to hold ~1.4 GB of decoded lineitem columns between phases at SF10, and 10x that at SF100. SF10 files have one page per row group, so the runtime join filters can't skip pages either.
- An aggregation per order (min / max of the supplier key, also over late lines only), as `SelfJoinAggregate` does for whole-table self joins, was slower (471 vs 350 ms). It runs before the selective supplier chain, so its input keeps half of lineitem.

## Decision

- `semi_anti_one_probe` fuses `Anti(Semi(B, σq(T), on₁), σp(T), on₂)`, and the other nesting, into one RIGHT SEMI join that builds on B and probes T once. Each probe row with an equal key marks a build row matched when `q and on₁` holds and excluded when `p and on₂` holds, and the join emits the matched rows not excluded (`HashJoinOp`'s `exclude`, a second shared mark array next to `matched`, so split probes and the parallel tail work as before).
- It is matched at lowering (`SemiAntiProbe` in `Lowering`), not as a runtime pass. The two joins stay in the runtime plan, so no pass, estimate, or Stats rule has to learn a new node. The rewrite only changes how the pair runs.
- Conditions: the same source table under filters and column-renaming projections on both sides, the same keys (after mapping both scans' columns onto one probe), both joins building on B, and deterministic conditions. The probe reads the union of both scans' columns and the OR of their row-group filters (none when either side reads all row groups).
- The Lean model `formal/WvletFormal/SemiAntiProbe/` proves the identity for any three-valued conditions (`anti_semi_eq`, `semi_anti_eq`). `SemiAntiProbeConformanceTest` replays its traces against the engine and DuckDB.

## Consequences

- TPC-H q21 at SF10: 0.72x wall and 0.71x CPU with the toggle on (10-run one-JVM A/B, 246 vs 341 ms). It reads lineitem twice. The second read decodes the two date columns over the semi join's probe rows too, which is cheaper than the third scan and its join.
- The rule fuses joins by their runtime-plan meaning (an anti join keeps the rows with no pair satisfying its keys and residual), whatever subquery produced them. A keyed anti join from NOT IN carries its NULL handling in its residual, which the identity treats as any other condition.
- The item's decode-cache direction is closed. Reading lineitem twice (the outer rows and the probe) remains: q21's two scans have different filters and run in different phases.
