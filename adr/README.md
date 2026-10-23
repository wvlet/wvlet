# Architecture Decision Records

Each ADR records one design decision that a future reader would otherwise have to reverse-engineer from commits: the context, the decision, and its consequences. Longer design explorations live in `plans/`; an ADR is the short, durable outcome.

## Writing an ADR

- File name: `adr/YYYY-MM-DD-(topic).md`
- Start with `# ADR: <title>`, then `Date: YYYY-MM-DD · PR #<number>`
- Sections: **Context**, **Decision**, **Consequences** (add worked examples when they clarify the decision)
- Add the new record to the index below in the same PR

## Index

- [2026-08-28 — Routing of `from '<file>'` schema inference](2026-08-28-data-file-schema-inference-routing.md) — how `from '<file>'` picks JSONAnalyzer vs DuckDB for schema inference (`DataFilePath` classifier, remote paths and DuckDB-less platforms)
- [2026-09-12 — Lowering keyword-named member calls](2026-09-12-keyword-member-call-lowering.md) — why un-inlined `x.like(...)`/`in`/`between`/`extract` calls are lowered to operators in `SqlGenerator` (not FunctionInliner or the `any` type) and why there is no fail-fast for other keyword-named calls
- [2026-09-20 — `for` loops iterate at run time](2026-09-20-for-loop-runtime-iteration.md) — why `for` loops iterate at run time with a per-iteration `Context` (not compile-time unrolling), and why loop variables bind with `scope.add`
- [2026-10-22 — Cross-platform orchestration in Scala](2026-10-22-scala-native-orchestration.md) — why orchestration is cross-platform Scala (JVM + Scala.js) with a task-queue worker protocol, not Temporal, so the same server and SDK run on both JVM and Node.js
- [2026-10-23 — Activity attempt recovery](2026-10-23-activity-attempt-recovery.md) — why restarted task queues and batches resume or mark in-flight attempts uncertain instead of re-executing them, and how result fencing treats `uncertain`
- [2026-10-23 — Activity reruns and cancel](2026-10-23-activity-rerun-and-cancel.md) — why failed activity items re-run only with `retry_failed`, why cancelled items re-run on resume, and why a batch waits up to a grace period for its cancelling items to confirm
