# Engine: UNION ALL of pipelines as one morsel pipeline

- Lane: Express (engine planner change behind a toggle; engine optimization loop auto-merge authorization).
- Problem: `Parallelizer.pipeline` stopped at `UnionAllOp`, so a hash join probe over a union, and the aggregation above it, ran on the calling thread. SF10 profile of TPC-DS q02 on main 915bc935: 67% of calling-thread CPU in `HashJoinOp.next` over web_sales ∪ catalog_sales, then a `PartitionedAgg` routing all 21M joined rows (5.5x DuckDB).
- Change: `MorselOp` takes several segments (scan + per-part pipeline). `Parallelizer.pipeline` turns a UNION ALL whose inputs are all pipelines (no tails) into one pipeline with every input's segments, cast to the union's columns; the operators above it (join probes over one shared build, two-phase partial aggregates) run on the parts of every input. Output order is input order, then part order. A segment whose source has one part runs on the calling thread in its turn. A union with nothing worth running above it keeps its inputs' own pipelines.
- Toggle: `union_all_pipelines`.
- Gates: engine tests, golden plans (12 TPC-DS plans change), JS/Native compile, SF10 A/B on q02, q05, q49, q75, q76, q77, q80 and unchanged-plan controls.
