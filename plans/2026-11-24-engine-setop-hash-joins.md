# Engine: EXCEPT / INTERSECT as parallel hash anti / semi joins

## Goals

- Run the set operation of INTERSECT / EXCEPT (without ALL) as morsel-parallel probes, not serially on the calling thread.
- Target TPC-DS q87 (418 ms vs DuckDB 91 ms at SF10, 3.9–5 busy cores) and q38; also q14, q08.

## Background

- `SetOpOp` buffers the right input into a boxed `HashMap[GroupKey, Int]`, then walks the left input row by row, all on one thread. `Parallelizer` already reduces both inputs to distinct rows in parallel (`distinctInput`).
- DuckDB plans these as hash SEMI / ANTI joins on every column with `IS NOT DISTINCT FROM` over distinct inputs; DataFusion rewrites them to joins over aggregates.
- `HashJoinOp` drops rows with a NULL key on both sides (SQL equality). Set operations treat NULLs as equal, so the join needs a null-safe mode.
- `GroupTable` already stores NULL as a group (GROUP BY semantics); only `lookup` resets NULL probe keys to "no match".

## Design

- Lane: **Express** — internal engine optimization behind a new toggle, no user-visible behaviour change, no always-gate area. Merge authorized by the launch prompt.
- New `Optimization` toggle `set_op_joins` (on by default). INTERSECT ALL / EXCEPT ALL stay on `SetOpOp`.
- In `Parallelizer`, before pipeline detection, `A INTERSECT B` becomes `distinct(A) ⋉ B` and `A EXCEPT B` becomes `distinct(A) ▷ B`, keyed on every column cast to the set operation's column types. Children convert first, so q87's chain `(A − B) − C` becomes one morsel pipeline over the distinct aggregation of A, probing two shared builds.
- The left side is already distinct when it is an aggregation, DISTINCT, or a converted semi / anti join; otherwise it gets the parallel distinct (`distinctInput`) or a streaming `DistinctOp`. The right side keeps its parallel distinct reduction to shrink the build.
- `HashJoinOp` / `JoinBuild` get a `nullsEqual` flag: when the build has a NULL key, every build row is valid, the build skips the packed / direct / flat paths and uses `GroupTable`, and probes look up NULL keys as values. Builds without NULL keys keep every fast path (a NULL probe key cannot match them anyway).
- A cast projection is added on top only when the left input's types differ from the set operation's schema.
- Build side stays on the right input; choosing the smaller side (RIGHT SEMI / ANTI) needs cardinalities the Parallelizer doesn't have, so it is a follow-up.
- Tests: conformance cases for INTERSECT / EXCEPT with NULLs and duplicates on both sides and mixed column types, a plan test for the rewrite, and the toggle in `OptimizerOptionsTest`; SF10 A/B on q87, q38, q14, q08.

## Alternatives and Why Not?

- Encode each key as `(is_null, coalesce(x, default))`: doubles the key columns and defeats `KeyPacker` for strings anyway; the flag is smaller.
- Rewrite in the logical planner (Lowering) instead: the Parallelizer already owns the distinct-input reduction, and the serial plan gains nothing from a join.
- Parallelize `SetOpOp` itself: duplicates the join's build / probe machinery.
