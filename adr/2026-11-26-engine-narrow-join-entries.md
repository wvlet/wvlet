# ADR: Fit a flat join table's row numbers in only the bits the build needs

Date: 2026-11-26 · PR #2496

## Context

A flat `LongJoinTable` stores a one-Long entry `(key - min) << 32 | row` when the build keys span fewer than 2^32 values, and a two-Long entry (key, row) otherwise. Multi-column integer keys packed by `KeyPacker` rarely fit in 32 bits: TPC-DS q78's joins on (year, item, customer) and its store_returns joins span 36–39 bits, so all six of its flat tables (2M to 8M slots each) took 16-byte entries. `LongJoinTable.firstRows` was about 10% of q78's CPU after #2493.

## Decision

- With `narrow_join_entries`, a narrow entry is `(key - min) << rowBits | row`, where `rowBits = 64 - nlz(buildSize)` (20–22 bits in q78). Key ranges below 2^(64 - rowBits) stay narrow, and the toggle off keeps 32 row bits.
- Entries stay exact: one compare per entry, as before. A key-tag entry, a hash fragment beside the row, would also halve the table, but every tag match would then need a read of the build key array, an extra cache miss on each hit.
- The empty entry stays -1. The row field never holds all ones, because the largest row is at most `2^rowBits - 2`.

## Consequences

- q78's flat tables take half the memory (about 270 MB less at SF10). Speed is neutral: CPU was 0.97–1.01x in three 10-run one-JVM A/Bs, because a 32–64 MB table misses cache on each probe just as a 64–128 MB one does. Making q78's three-key LEFT probes faster needs fewer cache misses per probe, for example probes clustered or partitioned by slot, not smaller entries.
- The INTERSECT / EXCEPT anti and semi joins of q38 and q87 key on strings with NULL-equal keys, so they never build a flat table and can't use the `probe_miss_filter` Bloom filter wherever `markMissFilters` runs. Moving the pass was left out: it would only add a plan tag.
