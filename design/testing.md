# Testing

This page owns the spec-driven test layout, the CI ratchets outside the engine, and test-run pitfalls. Engine gates are on [the engine page](engine.md); Lean conformance tests are on [formal verification](formal-verification.md).

## Spec-driven tests

`.wv` and `.sql` files under `spec/` are executable test cases, run by the spec runners in `wvlet-runner` and parsed by the parser specs in `wvlet-lang`.

- `spec/basic/`: core functionality. `spec/neg/`: expected compilation or execution errors. `spec/cdp_*/`: Customer Data Platform behavior.
- `spec/tpch/`, `spec/tpcds/`: benchmark queries in Wvlet. `spec/tpcds/*.wv` is generated from `spec/sql/tpc-ds/*.sql`: `./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.TPCDSWvletSpecGenerator"`.
- `spec/snowflake-sql/`: the golden Snowflake SQL of the stdlib specs; see [stdlib](stdlib.md#snowflake-variants).
- `spec/sql/basic/`, `spec/sql/tpc-h/`, `spec/sql/tpc-ds/`: SqlParser tests. They run against DuckDB, so use `VALUES` instead of tables that do not exist.

`.wv` files carry their assertions in `test` statements:

```wv
from 'data.json'
test _.size should be 10
test _.columns should contain 'user_id'
```

Run one spec file with `./sbt "runnerJVM/testOnly *RunnerSpecBasic -- spec:basic:hello.wv"` (wildcards like `query*.wv` work); parse one with `./sbt "langJVM/testOnly *ParserSpecBasic -- spec:basic:query.sql"` or `*ParserSpecSqlBasic -- spec:sql:basic:query.sql`. TPC-DS runs with `*RunnerSpecTPCDS` (Wvlet) and `*RunnerSpecSqlTPCDS` (SQL, full compile plus DuckDB execution).

## Ratchets and gates

- Typing coverage over spec/basic: `./sbt "langJVM/testOnly *TyperCoverageCheck"`. Raise its thresholds when type resolution improves; never lower them. A new spec/basic file can fail it, so type the expression at its source.
- The npm CLI (`sdks/cli-node`) bundles libduckdb through `@duckdb/node-bindings-*` optional dependencies pinned to the same DuckDB release as `DUCKDB_JDBC_VERSION`. Bump them together with duckdb_jdbc; `pnpm --filter @wvlet/cli test` fails on drift.
- Agent docs and the bundled skill have their own snippet and freshness tests; see [agent docs](agent-docs.md).
- Compiler performance: compare before and after with `WVLET_TYPER_BENCH=1 ./sbt "langJVM/testOnly *TyperBench"` (a log-only timing probe, ignored without the variable).

## Running tests

- Debug logs: append `-- -l debug`, e.g. `./sbt "langJVM/testOnly *Test -- -l debug"`. SQL generator trace: `-L *GenSQL=trace`.
- `./sbt -Dx=y` does not reach the forked test JVM.
- Plain `test` in engine, lang, and runner can skip classes as cached; run gates and spec classes with `testOnly`.
- Python SDK: quick tests run on every PR; full wheel tests run on tags, PRs labeled `test-wheels`, and a weekly schedule. Add the label for platform-specific changes.

## CI runner budget

The wvlet org's Free plan runs at most 20 GitHub-hosted jobs (5 macOS) at once, shared by every PR and every push to main, so CI time is mostly time spent waiting for a runner. Keep the number of jobs per push low:

- The main ruleset requires one status check, "CI passed" (the `ci_passed` job in `test.yml`). It fails when any job it `needs` failed or was cancelled; a job skipped by its path filter passes. `test.yml` calls the doc, VS Code, highlight.js, and Prism.js workflows (`workflow_call`), so their jobs are part of it. A new PR check goes into `test.yml` or a workflow it calls, and into `ci_passed`'s `needs`, never into the ruleset.
- `test.yml` and `native.yml` set `concurrency` keyed on the PR number with `cancel-in-progress` for `pull_request` events, so a rebase or new push cancels the older run. Runs on main and tags use `run_id` as their group, so they are never cancelled. A called workflow has no `concurrency` block: it shares the caller's `github.workflow`, so the same group would cancel the caller.
- `native.yml` builds `wvcLib` on mac, linux-arm64, and Windows only in its nightly run and on manual dispatch. Pushes to main build linux-x64; PRs build linux-x64 only when they touch `wvc-lib/`, `project/`, `*.sbt`, the Nix flake, or `SCALA_VERSION`. The required "Scala Native" job in `test.yml` still compiles and tests every Native project on each PR. To check a branch on every platform, run `gh workflow run native.yml --ref <branch>`.
- A matrix lists only the entries that run (see `native.yml`'s "Plan platforms" step). A job with every step skipped still takes a runner.
- Only full-matrix `native.yml` runs upload the merged `wvc-lib` artifact. `python-publish.yml` packages the newest one from main.
