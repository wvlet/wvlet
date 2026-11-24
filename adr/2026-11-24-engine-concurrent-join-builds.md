# ADR: The hash join builds of one pipeline run at the same time

Date: 2026-11-24 · PR #2362

## Context

- A probe pipeline prepares every hash join build it reads before its morsels start ([morsel parallelism](2026-11-22-engine-morsel-parallelism.md)), one after another on the calling thread. TPC-H q21 at SF10 waits for three builds over `lineitem`.
- Even with [parallel build steps](2026-11-24-engine-parallel-join-build.md), each build has serial parts (zeroing new build arrays, opening the Parquet file, the tail of its input scan), and they added up while the pool idled.
- A build drains a plan with parallel operators, so it waits for pool tasks: it must not run on a pool thread ([TaskRunner](../wvlet-engine/src/main/scala/wvlet/lang/engine/exec/TaskRunner.scala) contract).

## Decision

- With `concurrent_join_builds` (on by default), a pipeline with more than one prepare step gets one step that runs them through `TaskRunner.runTogether`: the first on the calling thread, the others on coordinator threads, the pool already used for shared CTE results.
- Each step is a `ComputeOnce`. The caller takes the steps in order and runs any step no coordinator has started yet itself, so neither the caller nor a coordinator ever waits for queued work, and the bounded coordinator pool cannot deadlock nested pipelines.
- After a failure, steps that haven't started never start; the first failure is raised once the started ones finish.

## Consequences

- TPC-H q21 at SF10 0.86x, q18 0.91x, TPC-DS q83 0.65x; SF1 suites 0.95x (TPC-H) and 0.94x (TPC-DS); CPU unchanged.
- A pipeline's builds hold their inputs and tables at the same time during the build phase.
- A runtime join filter that one build sets for another's input scan may now arrive after that scan starts, which only reads more rows; such filters did not take effect before either, since lower builds ran first.
