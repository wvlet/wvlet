# ADR: Rendezvous sampling hashes keys with DuckDB's hash()

Date: 2026-11-27 · PR #2593

## Context

Differentially private queries sample by a privacy unit (e.g., a user id): every row of a unit must be kept or dropped together, and every relation sampled by the same unit must keep the same units, or joins over the samples lose rows. A random per-row sample (`sample 10%`) has neither property. The sample has to be reproducible across the wvlet engine and the SQL it generates, and the engine is conformance-tested against DuckDB.

## Decision

- `sample rendezvous (p%[, seed = n]) by key, ...` keeps a row when its keys are non-null and the top 53 bits of DuckDB's `hash(seed, keys...)` are under `p% * 2^53`.
- The engine binds it as an ordinary `Filter` on a deterministic `RowFn`, not as `RuntimePlan.Sample`: it depends on the key values only, so PushDownFilters and the other filter passes move it freely.
- The engine computes DuckDB's hash (`DuckDBHash`, matched to duckdb_jdbc 2.0.1), so the engine and DuckDB keep the same rows and `EngineConformance` compares them row for row. Trino uses `xxhash64` over the keys' text; other databases reject it.

## Consequences

- The hash is not cryptographic. The seed acts as the secret for privacy amplification by subsampling: a release must pick a random seed and keep it secret. A keyed cryptographic hash (e.g., SipHash or HMAC) would be stronger but would not match any SQL engine's built-in function.
- Hashes are type-sensitive as in DuckDB: integers hash by their 64-bit value (int and bigint keys agree), strings by their UTF-8 bytes; other types hash their text in the engine, which can differ from DuckDB.
- Trino samples differ from the engine's and DuckDB's for the same seed.
- Fixed along the way: the engine's multi-argument `hash(a, b)` used DuckDB's pre-2.0 hash combination and disagreed with DuckDB.
- Fixed-size samples (`sample rendezvous (1000) by k`, the k smallest key hashes) are not supported yet.
