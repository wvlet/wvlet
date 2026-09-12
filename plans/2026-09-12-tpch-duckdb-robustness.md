# Improve TPC-H on DuckDB: schema-less codegen, perf probe, session warm-up

- Author: Taro L. Saito
- Date: 2026-09-12
- Work item: 2026-09-08-improve-tpc-h-on-duckdb (follows the 2026-09-07 TPC-H performance study)

## Goals

- Wvlet-generated TPC-H SQL already runs at parity with canonical SQL on DuckDB (sf=1, sf=10), so
  the codegen hot path needs no work. This PR hardens the surrounding pieces instead:
  1. `wvlet compile` without a schema declaration must not emit invalid SQL such as
     `o_comment."like"('%special%')`.
  2. A repeatable, opt-in TPC-H timing probe so future codegen changes can be compared, plus a
     documented manual recipe for the larger `EXPLAIN ANALYZE` runs.
  3. Understand and, where possible, cut the ~1.8 s first-query latency of a fresh DuckDB session.
- Keep `spec/tpch` at sf=0.01 as the CI-covered correctness suite; nothing perf-related becomes a
  CI gate.

## Background

### 1. Keyword-named member calls without a schema

`spec/tpch/schema.wv` declares the TPC-H tables, so within the spec folder every column is typed
and `o_comment.like('%x%')` resolves to the stdlib def in `wvlet-stdlib/module/standard/string.wv`
(`def like(pattern:string): boolean = sql"${this} like ${pattern}"`), which `FunctionInliner`
inlines into operator form. From a work folder without the schema, the qualifier's type is
unknown, `FunctionInliner.resolveFunctionApply` leaves the `FunctionApply(DotRef(qual, like), ...)`
node untouched, and `SqlGenerator` prints it as a plain function call. Because `like` is a SQL
keyword, `Identifier.toSQLAttributeName` double-quotes it, giving `o_comment."like"(...)`.

Reproduced today by copying `spec/tpch/q*.wv` into `target/tpch-repro`, running
`wvlet compile -f qN.wv`, and executing on the duckdb CLI (sf=0.01): 9 of 22 queries fail only at
execution time.

| queries | member call | DuckDB error |
|---|---|---|
| q9, q13, q14, q16 | `.like(...)` | Scalar Function with name like does not exist |
| q18, q19, q20 | `.in(...)` | Scalar Function with name in does not exist |
| q7, q8 | `.extract('year')` | Scalar Function with name extract does not exist |

`q19` also emits `p_size."between"(1, 5)` and `q16` emits `ps_suppkey.not_in(...)` (a legal
identifier, so it fails later with "function not_in does not exist"). The stdlib names that
collide with SQL keywords are `like`, `in`, `between` (plus `count`, `concat`, which are
non-reserved and print fine, and `exists`, `distinct`, `exclude`, `truncate`, which have no
member-call use today).

### 2. No perf probe exists

`TyperBench` (`wvlet-lang/.jvm/src/test/.../TyperBench.scala`) is the only timing probe: log-only,
no assertion, compared by eye within one JVM session. The TPC-H study used an ad-hoc
`target/tpch-bench/run_bench.sh` (duckdb CLI, `EXPLAIN ANALYZE`, min/median over N runs) that is
not in the repo.

### 3. Where the first-query latency really comes from

Measured in-JVM (DuckDB JDBC 1.5.5.1, macOS arm64, warm extension cache):

| phase | time |
|---|---|
| first `DriverManager.getConnection("jdbc:duckdb:")` | 1.46–1.64 s |
| `install tpch` | 2 ms |
| `load tpch` | 6 ms |
| `call dbgen(sf=0.01)` | 65 ms |
| `call dbgen(sf=1)` | 1.6 s |
| second in-process connection | 4 ms |

So the TPC-H extension and sf=0.01 dbgen together cost under 80 ms. The 1.5 s is the JDBC driver
bootstrapping its 109 MB `libduckdb_java` native library (extract from the jar plus `dlopen`).
Pre-extracting the library and pointing `java.library.path` at it did not change the number
(1.58 s), so the cost is the load itself, not the temp-file copy. `DuckDBConnector` already runs
this bootstrap in a background thread from construction, and the default `wv` profile creates the
connector during `WvletScriptRunner` init, so in the REPL the load overlaps terminal setup and the
source-folder precompile. `wvlet compile` never touches DuckDB (0.6 s wall) while
`wvlet run -c "select 1"` takes ~2.0 s: the difference is this bootstrap, which only shows up when
each query starts a fresh JVM.

Conclusion: a TPC-H CLI flag or extension caching cannot buy more than ~80 ms. The item's third
task is re-scoped to (a) make the scale factor configurable, which the probe needs anyway, and
(b) make the init phases observable so the next latency report is attributable. The native
bootstrap itself is a DuckDB JDBC packaging matter and is left as a follow-up issue.

Side finding, out of scope: `wv -l trace` throws `ClassCastException: String cannot be cast to
LogLevel` in the option parser (a follow-up issue).

## Design

### 1. Lower keyword-named member calls in `SqlGenerator`, fail fast otherwise

In `SqlGenerator`'s `case f: FunctionApply` (`SqlGenerator.scala:1372`), before the regular
function branch, match `f.base` against `DotRef(qual, method: Identifier)` and lower the
operator-shaped stdlib methods to exactly the SQL their stdlib definitions inline to:

| member call | generated SQL |
|---|---|
| `q.like(p)` / `q.not_like(p)` | `q like p` / `q not like p` |
| `q.in(a, b, ...)` / `q.not_in(...)` | `q in (a, b, ...)` / `q not in (...)`; a single subquery argument is printed as `q in (subquery)` like the `In` node does |
| `q.between(a, b)` / `q.not_between(a, b)` | `q between a and b` |
| `q.extract('year')` | `extract(YEAR from q)`; the field must be a string literal naming an `IntervalField` |

- Emitted text matches the `FunctionInliner` output for typed qualifiers, so `spec/tpch` with the
  schema and the schema-less copy generate identical SQL. No parentheses are added, matching the
  inlined form (`and not o_comment like '...'`).
- The lowering fires only when the node survived the analyzer un-inlined, i.e. when the
  qualifier's type could not be resolved. Resolved calls never reach this branch.
- Any other `DotRef`-based call whose method name `requiresQuotation` (a SQL keyword) raises
  `StatusCode.SYNTAX_ERROR` via the generator's existing error helper, with a message naming the
  call and suggesting a schema/table declaration. Emitting `x."keyword"(...)` is never valid SQL,
  so failing at compile time is strictly better than at execution.
- `FunctionInliner` is left unchanged: keeping the fallback in codegen keeps the RLike →
  `regexp_matches` precedent and avoids adding these methods to the `any` type, which would widen
  the language surface (`1.like('x')` would type-check).

Tests:
- `SqlKeywordMemberCallTest` (wvlet-lang, parse-only generator like `SqlQuotingTest`) covering
  each lowered form, the subquery `in`, and the fail-fast error.
- `TPCHSchemalessTest` (wvlet-runner JVM): copy `spec/tpch/q*.wv` (not `schema.wv`) into a
  `target/` folder, compile them with a schema-less compiler, and execute each on a DuckDB
  connector with `prepareTPCH` at sf=0.01. This is the exact `wvlet compile` failure mode from
  the item and guards it end to end.

### 2. `TPCHBench`: opt-in, log-only timing probe

- `wvlet-runner/.jvm/src/test/scala/wvlet/lang/runner/TPCHBench.scala`, same shape as
  `TyperBench`: no assertion, numbers go to the log.
- Opt-in via the environment variable `WVLET_TPCH_BENCH_SF` (e.g. `1`); when unset the single
  test calls `ignore(...)` so `runnerJVM/test` and CI stay untouched. Env vars reach the forked
  test JVM, unlike `-D` properties.
- Setup once: DuckDB profile with `prepareTPCH=true` and `tpchScaleFactor=<sf>` (new property,
  see 3), one `Compiler` over `spec/tpch`, `ExecutionPlanner`/`QueryExecutor` as in `RunnerSpec`.
- Per query `q1..q22`: compile once (logged separately), one warm-up execution, then 3 timed
  executions with `isDebugRun=false` (the `.wv` files carry no assertions; the sf=0.01 expectations
  live in `spec/tpch/test`). Log per-query min/median and the total, plus the dbgen time.
- Documented in a new `website/docs/development/benchmark.md` together with the manual recipe:
  build `tpch_sf10.duckdb` with the duckdb CLI, dump SQL with `wvlet compile -f`, and run
  `EXPLAIN ANALYZE` N times per query. The `run_bench.sh` from the study is committed as
  `scripts/tpch-bench.sh` so the recipe is runnable as-is. `CLAUDE.md` gets the one-line
  command next to the `TyperBench` note.

### 3. Configurable scale factor and observable DuckDB init

- `DuckDBConnector` gains `tpchScaleFactor: Double = 0.01` (and `tpcdsScaleFactor` for symmetry
  of `loadTPCDS`), wired through `DuckDBConnectorFactory` as connector properties
  `tpchScaleFactor` / `tpcdsScaleFactor` and through `RunnerSpec`'s constructor with the same
  defaults. `spec/tpch` and `spec/tpcds` keep sf=0.01.
- The init thread logs, at debug level, the elapsed time of the connection bootstrap and of the
  TPC-H/TPC-DS load as separate lines, so `wv --debug` shows what a slow first query paid for.
- No new CLI flag: `wvlet ui --tpch` keeps its meaning; the sf knob is available through profile
  properties for people who want a larger demo set.
- Follow-up issues (not in this PR): DuckDB JDBC native bootstrap latency (~1.5 s per JVM) and the
  `wv -l trace` ClassCastException.

## Alternatives and Why Not?

- **Add `like`/`in`/`extract` to the stdlib `any` type** so the existing AnyType fallback in
  `findFunctionDef` resolves them: minimal Scala change, but it changes typing (`any` would accept
  `like`), alters the generated stdlib docs and engine catalogs (freshness-gated in CI), and does
  not cover the fail-fast half.
- **Fail fast only, no lowering**: simpler, but `wvlet compile` on the 9 queries would then fail
  where it could just work; the lowering table is small and mirrors the stdlib exactly.
- **Run the perf probe in CI with a threshold**: rejected by the item; timings are machine-bound
  and dbgen(sf=1) alone is 1.6 s of setup.
- **Cache the TPC-H extension / add a `--tpch` flag to `wv`** as the item suggested: the
  measurement above shows the extension path costs under 80 ms, so it is not worth a flag.
