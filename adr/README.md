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
