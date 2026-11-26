# Probe a table once for an EXISTS and a NOT EXISTS over it (TPC-H q21)

## Goals

- TPC-H q21 is the last of the item's targets that is still slow: 1.6–1.8x DuckDB at SF10 on origin/main (b0d14f99). It reads lineitem three times. The goal is to read it twice by checking the EXISTS and the NOT EXISTS in one pass over lineitem.
- Upper bound: without its NOT EXISTS, q21 takes 208 ms vs 305 ms (10 runs, same JVM, load ~9–12), so the third scan and its join are about 32% of the query and 1.3 GB of its 4.2 GB allocation.
- The combined probe still decodes `l_commitdate`/`l_receiptdate`, now over l2's rows (which a larger key set filters than l3's). The expected net is therefore smaller, roughly 0.75–0.85x of today's q21. A runtime-only prototype measures this before the Lean work starts.

## Background

- Re-measured on main at SF10, 5–10 runs:
  - H q18 is now 0.9x DuckDB, and DS q70 is 1.1x wall / 0.8x CPU. q70's two store_sales reads already share one aggregation (#2454). Neither has a repeated scan left.
  - H q21 plan: lineitem l1 (filtered by supplier keys) → orders → **RightSemi** probing lineitem l2 (EXISTS, residual `l2.suppkey != l1.suppkey`) → **RightAnti** probing lineitem l3 (NOT EXISTS, same residual plus `l3.receiptdate > l3.commitdate`). Both probes are runtime-filtered on l_orderkey: l2 by the l1 chain's keys, l3 by the semi join's output keys (a subset).
- The three scans run one after another: each one's output builds the next join. A decoded-column cache (the item's 11-24 proposal) would have to hold lineitem's decoded columns between phases: about 1.4 GB at SF10, and 10x that at SF100. SF10 files have one page per row group, so the runtime filters can't skip pages ([[engine-parquet-one-page-per-chunk]]).
- Rewriting q21 as one aggregation per order (min/max of suppkey, also over late lines only) is slower: 471 ms vs 350 ms. The aggregation runs before the selective supplier chain, so its input keeps half of lineitem. This is the same reason `SelfJoinAggregate` leaves q21 alone.

## Design

Lane: Standard. This replaces the item's direction (a shared decode cache) with a plan rewrite, and adds a Lean-modeled rewrite.

- New runtime pass `semi_anti_one_probe` (an `Optimization` toggle). Runtime plans have `Join(kind = Semi | Anti, buildLeft)`; RightSemi/RightAnti appear only at lowering. The rule rewrites `Anti(Semi(B, T), σp(T))` into one semi join over B and T with a second, anti condition. It applies only when:
  - both joins build on B (the same `buildLeft` side). Otherwise the pass leaves the pair alone;
  - both probe inputs read the same table T, and the anti side is the semi side's rows filtered by a probe-only predicate `p` (l3's late-line filter);
  - both joins have the same equi-keys;
  - `p` and both residuals are deterministic, because the combined join evaluates them once per pair;
  - (Implementation: fused at lowering, by the joins' runtime-plan meaning. An anti join keeps the rows with no pair satisfying its keys and residual, so a keyed anti join from NOT IN, whose NULL handling is in its residual, needs no exclusion; see the ADR.)
- Implemented at lowering instead of as a pass in `defaultPasses`: by then `buildLeft` is fixed, and a scan's row-group filter only skips data (the filter over it stays), so the probe reads the OR of both scans' row-group filters.
- The combined join probes once and keeps two marks per build row: *matched* (key plus semi residual) and *excluded* (key plus anti residual plus `p`; a NULL `p` or residual does not exclude). At the end it emits the build rows that are matched and not excluded.
- Today a join build has one shared `matched` array, and the tail emits by one flag. So the scope includes a second shared *excluded* array for the own, split-part, and parallel-tail modes, and a matched-and-not-excluded emit. The new join field also has to be handled by the cardinality estimate, Stats, and column pruning. The runtime join filter on the probe stays as it is for the semi join.
- The one probe scan reads the union of both scans' columns: q21 reads `l_commitdate`/`l_receiptdate` once more than l2 did, instead of reading the whole l3 scan.
- Proof: a Lean model `WvletFormal/SemiAntiProbe/` of the identity `B ⋉ₛ T ▷ₐ σp(T) = {b ∈ B | ∃ match ∧ ¬∃ p-match}` with NULL keys, residuals, and three-valued `p`. It gets a trace fixture and a conformance test, as `SelfJoinAggregate` has, and design/formal-verification.md lists it.
- Tests: plan tests for the matched shape and for shapes it must leave alone (different tables, different keys, a filter on the semi side the anti side lacks), plus a spec query covering NULL keys and NULL residual columns. Golden plans are regenerated (q21's changes).
- Merge after an SF10 A/B with the toggle on and off for H q21 plus control queries with EXISTS/NOT EXISTS (H q4/q22, DS q16/q94/q35), showing no regression.

## Alternatives and Why Not?

- **Per-query decoded column cache** (the item's proposal). The scans are sequential phases, so the cache would hold about 1.4 GB at SF10 and grow with scale. With a memory cap, q21 would reuse little.
- **Cache decompressed page bytes in uni.** Same memory problem, and it saves only Snappy, not decoding.
- **`group by l_orderkey` with min/max (SelfJoinAggregate for q21).** Measured slower (471 vs 350 ms): it runs before the selective build.
- **Stop here and close the item.** q21 stays at 1.6–1.8x, the largest remaining TPC-H gap from repeated scans.
