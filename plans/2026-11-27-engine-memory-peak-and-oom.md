# Engine memory: per-query peak in the bench and OutOfMemoryError at the query boundary

## Goals

- Show each query's peak tracked memory in the bench TSV, so memory regressions show up in the optimization loop and the SF100 baseline can name its heaviest queries.
- Turn a JVM `OutOfMemoryError` during a query into a status-coded RESOURCE_EXHAUSTED error that says how to give the engine more heap, instead of an uncaught JVM error.

## Background

- #2573–#2575 added `MemoryContext` per `WvletEngine.execute`: holders reserve estimated bytes, past `queryMemoryLimit` the query fails with RESOURCE_EXHAUSTED, and every reservation is released when `execute` returns.
- The context tracks only the current total; nothing records the peak.
- The bench (`EngineTPCHBench.plannedRun`) plans and drains operators itself, without opening a `MemoryContext`, so today it measures no reservations at all.
- An `OutOfMemoryError` is fatal to Scala's `NonFatal`: it passes the engine's plan-time fallbacks and the runner's `guarded` (which converts only `ColumnNotFound`) and reaches the user as a raw JVM error. Task runners rethrow a task's error unwrapped, so it arrives at `execute` as is.
- Another session (`query-manager-memory-followups`, #2577 and an early-release branch) adds a low-memory killer, child contexts that release early, and counting `DistinctOp`, set operations, and nested-loop builds. This PR leaves those out.

## Design

Lane: Express (bench column and an error conversion, no plan change, no always-gate area); merge authorized by the launch prompt.

- `MemoryContext` keeps the highest total it reached (`peakBytes`), updated where reservations add up, so child contexts from the parallel branch still count at the query's root.
- The bench opens a query context in `plannedRun` the way `execute` does and reports the median peak as `engine_peak_mb` in the TSV and the markdown table. Reservations now run in the bench too, which matches what `execute` pays.
- `WvletEngine.execute` catches `OutOfMemoryError` after its `finally` blocks have closed the operators and released the context, and throws RESOURCE_EXHAUSTED naming the max heap, the query's peak tracked memory, and how to raise it (`-Xmx` in `JAVA_OPTS` for the JVM CLI, `GC_MAXIMUM_HEAP_SIZE` for the Native one) or set `--max-query-memory` to fail earlier.
- The conversion is not behind a toggle: it changes no plan and no hot path, only the error a dying query reports.
- Tests: peak stays after release and covers a tiny-limit failure; an `OutOfMemoryError` thrown from an operator surfaces as RESOURCE_EXHAUSTED with the heap hint and leaves the pool at zero.

## Alternatives and Why Not?

- Add peak to `EngineResult`: every caller would carry a field only the bench reads; the bench can read it from its own context.
- Catch `OutOfMemoryError` in the runner's `guarded`: the engine's context and operators are already gone by then, but other engine callers (server, `TableWriter`) would still see the raw error.
