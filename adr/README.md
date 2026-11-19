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
- [2026-10-27 — Group aggregations take the column as the receiver](2026-10-27-group-aggregation-receiver.md) — why `product.count_distinct` / `product.max_by(amount)` is the only Wvlet spelling and `_.count_distinct(product)` is not supported
- [2026-10-31 — The wv REPL uses uni's LineReader](2026-10-31-repl-uni-line-reader.md) — why JLine was replaced by uni's cross-platform `LineReader`, why line editing requires Java 22+, and which uni gaps the REPL accepts
- [2026-11-18 — Lean 4 models with trace conformance](2026-11-18-lean-formal-verification.md) — why behavior with all-interleavings invariants (starting with activity items) is modelled in Lean under `formal/`, and how JSON trace fixtures keep the Scala code conformant to the proofs
- [2026-11-18 — Name-lookup precedence in the Typer](2026-11-18-name-lookup-precedence.md) — the order in which a bare identifier or relation name resolves (scope, visible globals by file basename, import by name, input columns; columns shadow only function definitions), and which surprising consequences were kept on purpose
- [2026-11-18 — Materialized models build on the flow executor](2026-11-18-model-build-semantics.md) — why model config is a typed `with { }` block (no YAML or Jinja), why materialized models are read from storage only inside `wvlet build` and are always statically typed, and why builds run as a flow
- [2026-11-18 — npm is the primary CLI distribution](2026-11-18-npm-cli-distribution.md) — why `npm install -g @wvlet/cli` with bundled DuckDB (optional platform packages) replaces Homebrew as the recommended install, and why updates are a cached notice plus `wvlet upgrade` delegating to the package manager
- [2026-11-18 — String concatenation treats NULL as an empty string](2026-11-18-string-concat-null.md) — why string `+` and `s"..."` skip NULL operands instead of following SQL's `||`, and why every engine gets one `coalesce`-wrapped `concat`
