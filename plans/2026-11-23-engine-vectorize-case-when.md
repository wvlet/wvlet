# Engine: typed CASE WHEN and leaner boolean kernels

## Goals

- `CaseWhen.eval` stops boxing every row. TPC-DS `sum(case when d_day_name = ... then price end)` (q02, q43, q59) spends 46% of engine CPU on q02 (the work item's async-profiler figure) in `Vec.get` → `JBigDecimal.valueOf` → `DecimalBuilder.addBig`.
- `Kernels.and` / `or` stop paying per-row `isNull` calls and fresh null arrays on nullable inputs. q28 (`x BETWEEN a AND b AND (... OR ... OR ...)` over OPTIONAL columns) spends 28% of engine CPU in these kernels (work item figure; the raw q28 profile shows `and`, `or`, `BoolVec.nulls`, `Vec.isNull`, and `compareScalar` as the hot frames).
- Rows stay identical: TPC-H 22/22, TPC-DS 99/99, spec/basic conformance.

## Background

- Vectors are immutable. A `nulls` array of `null` means "no nulls", and kernels already share input null arrays (`compareScalar` returns the input column's `nulls` as is), so sharing a null array between vectors is safe.
- The value slot of a NULL row is unspecified: `compareScalar` may leave `true` there. Kernels must mask values with the null array instead of trusting them.
- `BETWEEN` binds to `AndExpr(x >= a, x <= b)`. With literal bounds (q28), both sides go through `compareScalar` and carry the *same* null array object (x's). With column bounds, `compare` merges null arrays through `orNulls`.
- `and` / `or` already skip null arrays when neither input has nulls. The cost is on the nullable path: Parquet columns DuckDB writes are OPTIONAL, and a column with any NULL in a batch carries a null array.
- CASE branches are cast to the result type before filling, so all branch vectors share one physical layout, except wide decimals (ObjVec) and nested types.

## Design

- Lane: Express. An internal performance change with no change in results and no always-gate area. The launch prompt says "Do not merge", so the PR is held for the user's merge decision.
- **Typed CASE WHEN**: conditions still pick a branch per row (`chosen`). The output is then filled per physical type straight from the branch vectors' arrays (Bool, Int, Long and unscaled Long decimals, Double, String), with nulls taken from the chosen branch's null array. A missing ELSE yields NULL.
- Decimals stay on unscaled longs when every branch, after the cast, is a `LongVec` of the result type. Branches of other scales are already rescaled by that cast, so mixed scales take the fast path too. Wide decimals (ObjVec) and nested values keep today's row path.
- A branch no row picks in a batch is not evaluated. This is a deliberate, tested change: an error (e.g. a cast overflow) raised only by rows a branch does not produce no longer fails the query, which matches DuckDB, where CASE evaluates a branch only on its rows.
- **AND / OR**: work on raw arrays with the null case resolved per side: both sides non-null (as today), one side nullable, and both nullable.
- When both sides share one null array (BETWEEN), the result reuses it: a row is NULL exactly when the shared input is NULL, and no new array is allocated.
- Otherwise the null array is computed branch-free, and dropped when no row ended up NULL.
- `compare` (column vs column) also reuses a shared null array instead of merging it with itself. `compareScalar` already shares its input's null array; its output values array is the one allocation left.
- Tests: unit tests for CASE WHEN over each physical type (NULL branches, missing ELSE, mixed decimal scales, wide decimal fallback) and for AND/OR three-valued truth tables with shared and separate null arrays. SF1 A/B on q02, q28, q43, q59, q67 against an origin/main worktree.

## Not in this PR

- Fusing BETWEEN into one range kernel, and COALESCE on typed arrays: follow-ups if the profile still shows them.
- Writing results into input arrays: vectors are shared (cached constants, scan columns, gathered columns), so in-place reuse needs ownership tracking first.
- Selection vectors and Parquet assembly: other sessions own them.

## Alternatives and Why Not?

- Bitmap null masks: changes the `Vec` contract every operator relies on; far larger than this item.
- Keep the row path and make `DecimalBuilder` faster: still boxes per row; the win is in not boxing at all.
