# Fix compiler/CLI bugs found while verifying the AI-agent docs

## Goals

- Fix four bugs found while checking the examples on the AI-agent docs page (#2141), so that the
  documented commands and syntax either work or fail with a clear message.
- Add one regression spec or test per bug.

## Background

- (a) `from a union { from b }`: Wvlet has no `union` keyword on purpose (WvletToken.scala: users
  often write UNION where UNION ALL is meant; `concat` is the operator). The parser reads the bare
  `union` as a partial-query application. `FunctionInliner.resolvePartialQuery` only logs
  "Partial query 'union' not found" and keeps the `PartialQueryApply` node. `SqlGenerator.relation`
  has no case for it, so the generic `case r: Relation` fallback re-dispatches the same node until
  the stack overflows (the same failure the `ModelScan` case was added to stop).
- (b) `_.count_distinct(col)` fails with "Too many arguments". The stdlib defines
  `count_distinct`, `max_by(expr)`, `min_by(expr)`, `to_array` on `array[A]`. After `group by`, a
  column has that array type, so the column is the receiver: `col.count_distinct`,
  `col.max_by(other)`. `_` is the whole group and accepts no column argument. The syntax reference
  (website/docs/syntax/index.md) documents `_.count_distinct(column)`, `_.max_by(a, b)`,
  `_.min_by(a, b)` and `_.array_agg(column)`, and none of them work (`_.sum` also fails at DuckDB
  with `sum(_)`). for-ai-agents.md already lists `_.count_distinct(col)` as a mistake.
- (c) `from '/abs/path/t.csv'` becomes `'.//abs/path/t.csv'`. `Context.dataFilePath` always
  prefixes the work-env path (default `.`) unless the path starts with `s3://` or `https://`.
- (d) `wvlet compile --help` prints the top-level help. In uni's launcher, the top-level
  `OptionParser` scans every argument for `-h`/`--help`, including those after the sub-command
  name, so the top level handles the flag before the method command sees it. This is a bug in
  uni (`CommandLauncher.execute`), not in wvlet; it is still present on uni main.

## Design

Lane: Express — clear diagnoses, no new behaviour beyond turning a crash into an error and making
documented commands work; no always-gate area. (d) may widen into the uni repo; see below.

- (a) Report an unknown partial query as a user error instead of a warning: throw
  `StatusCode` error from `resolvePartialQuery` (not found / not a partial query). When the name
  is `union`, the message suggests `concat` (or `dedup` after it for UNION semantics). Also add an
  explicit `PartialQueryApply` case in `SqlGenerator.relation` that raises an error, so an
  unresolved node can never recurse forever. Regression: spec/neg/union-not-supported.wv plus a
  FunctionInlineTest case checking the status code.
- (b) Correct the docs rather than the stdlib: rewrite the examples to `product.count_distinct`,
  `product.max_by(amount)`, `product.min_by(amount)`, `col.to_array` / `array_agg(col)`, and fix the
  tip text. Keep for-ai-agents.md as is (it already says the right thing), extending its mistake
  row to cover `_.max_by(a, b)`. Regression: a spec/basic case running the corrected doc example.
  No stdlib change, so no catalog regeneration.
- (c) In `Context.dataFilePath`, return absolute paths (`/…`, Windows `C:\…`) and remote URLs
  (`DataFilePath.isRemote`, any `scheme://`) unchanged. Regression: a unit test on
  `dataFilePath` and a runner test reading an absolute path.
- (d) Fix in uni: make `CommandLauncher.execute` treat `-h/--help` as top-level help only when it
  appears before the sub-command name, then bump `UNI_VERSION` in wvlet after a uni release.
  This needs a separate uni PR and release, so it is asked about rather than assumed.

## Alternatives and Why Not?

- (b) Make `_.count_distinct(col)` work by adding overloads with a column argument. Two spellings
  for the same aggregation, and `_` would mean "the group" in `_.count` but "nothing" in
  `_.count_distinct(col)`. The receiver style is what the stdlib, the rest of the docs, and
  for-ai-agents.md already teach.
- (d) Work around the launcher in wvlet (rewrite args, or build a per-command help launcher).
  Duplicates launcher logic and leaves the same bug in every other uni CLI, including
  `wvlet flow` / `wvlet catalog`.
