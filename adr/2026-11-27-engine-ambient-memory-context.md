# ADR: Operators capture an ambient per-query memory context

Date: 2026-11-27 · PR #2573

## Context

Per-query memory accounting needs every large in-memory holder (join builds, aggregation tables, sort and window buffers, CTE results) to reserve bytes from its query's `MemoryContext`. Operators are built in dozens of places: `Lowering`, `JoinOrderOptimizer`, `ColumnPruner`, `Parallelizer`'s rebuilds, plan-time subqueries, and lazily computed shared results that may lower plans on a worker thread. The builds themselves run on whichever worker thread probes first.

## Decision

`MemoryContext.current` is a thread-local scope. `WvletEngine.execute` opens the query's context and makes it current before planning; `Planner` captures it when constructed and re-enters it in `plan`, `lower`, and `physical`. Memory-holding operators read `MemoryContext.current` into a field at construction and reserve from that field at run time. An operator that builds operators at run time, outside the planner, passes its own context on.

## Consequences

- No constructor or Parallelizer rebuild site takes a new parameter; adding a holder is one field and its reservations.
- An operator built outside any scope (unit tests, `explain`, `planStages`) gets `MemoryContext.NoOp` and counts nothing, so a missed scope fails open (no accounting) rather than failing queries.
- `withCurrent` restores the previous context in `finally`, so reused server worker threads never carry a finished query's context.
- Reviewers must check that new run-time operator construction (outside the planner) passes the parent's context.

## Alternatives

- A `MemoryContext` constructor parameter on every operator: explicit, but touches most planner sites and every rebuild for no behavioural gain.
- Binding contexts by walking the finished operator tree: misses operators that run while planning (scalar subqueries, plan-time CTEs).
- Propagating the context through `TaskRunner` tasks: couples accounting to the scheduler, which phase 3 of the query manager plan is changing.
