---
name: SqlParser — Trino SQL grammar parity
about: Close gaps between our SqlParser and Trino’s SqlBase.g4 in phases
title: "SqlParser: Achieve Trino SQL grammar parity (phase-by-phase)"
labels: ["enhancement", "area:parser"]
assignees: []
---

Summary
- Align SqlParser with Trino’s SQL grammar to broaden dialect compatibility and reduce surprises when running Trino-style queries. Work will land in small, reviewable phases guarded by tests.

Motivation
- Users expect common Trino syntax to parse. Improving parity reduces friction and enables porting Trino queries and examples. Reference grammar: io.trino: SqlBase.g4 (ANTLR).

Current Coverage (short)
- Queries: SELECT, VALUES, WITH/RECURSIVE, subqueries, EXISTS, UNION/INTERSECT/EXCEPT (+ ALL).
- Clauses: WHERE, GROUP BY (simple), HAVING, ORDER BY, LIMIT/OFFSET, windows (OVER with PARTITION/ORDER, ROWS frames), FILTER (...) and window after function calls, AT TIME ZONE, CAST/TRY_CAST, :: cast, CASE, LIKE [ESCAPE], BETWEEN, IN/NOT IN.
- Structures: arrays, maps, JSON literal + JSON_OBJECT (KEY…VALUE + modifiers), lambdas, subscripts.
- Table features: TABLESAMPLE + USING SAMPLE, UNNEST [WITH ORDINALITY], LATERAL (subquery), joins, ASOF join token, parenthesized relations.
- DML/DDL: INSERT/INSERT INTO (columns or query), UPDATE, DELETE, MERGE (basic branches), CREATE SCHEMA/TABLE/VIEW (+ IF NOT EXISTS, CTAS), DROP SCHEMA/TABLE/VIEW, ALTER TABLE (rename/add/drop column, set data type/default/not null, set authorization/properties/execute …).
- Commands: EXPLAIN [PLAN] [FOR …], SHOW (catalogs|schemas|tables [IN …]|columns FROM …|functions|CREATE VIEW …), USE schema.
- Prepared statements: PREPARE, EXECUTE (… USING … or (…)), DEALLOCATE.

Key Gaps vs Trino
- Query features
  - GROUP BY: GROUPING SETS, CUBE, ROLLUP.
  - Windows: RANGE/GROUPS frames; EXCLUDE (CURRENT ROW/TIES/NO OTHERS); named windows.
  - QUALIFY clause.
  - FETCH FIRST/NEXT n ROW(S) ONLY (in addition to LIMIT/OFFSET).
  - NATURAL JOIN; broader LATERAL/table function forms.
  - Predicates: IS DISTINCT FROM / IS NOT DISTINCT FROM; quantified comparisons (ANY/SOME/ALL).
  - Ordered-set aggregates: WITHIN GROUP (ORDER BY …) e.g., LISTAGG.
- Statements/commands
  - Transactions: START TRANSACTION / COMMIT / ROLLBACK (+ options).
  - SET/RESET forms: SET SESSION, RESET SESSION, SET PATH, SET TIME ZONE, SET/RESET TRANSACTION.
  - EXPLAIN ANALYZE; DESCRIBE INPUT/OUTPUT.
  - CALL procedures.
- Table functions / JSON
  - JSON_TABLE; extended LATERAL table function aliasing/column lists.
- Types/intervals
  - Ensure date/time precisions and “WITH TIME ZONE” forms match Trino where applicable.

Proposed Plan
- Phase 1 — Syntactic additions (low risk)
  - [ ] FETCH FIRST/NEXT … ONLY in query tail.
  - [ ] QUALIFY clause (translate to a Filter after windowed selections).
  - [ ] NATURAL [LEFT|RIGHT|FULL|INNER] JOIN resolution (USING columns).
  - [ ] Predicates: IS DISTINCT FROM / IS NOT DISTINCT FROM.
  - [ ] Quantified comparisons: =, <, … ANY/SOME/ALL (subquery).
  - [ ] SHOW extensions: SHOW CREATE TABLE/MATERIALIZED VIEW/FUNCTION; SHOW STATS FOR table.
  - [ ] Tests under spec/sql/basic for each addition.

- Phase 2 — Windows + Grouping
  - [ ] Window frames: RANGE, GROUPS; EXCLUDE (NO OTHERS|CURRENT ROW|TIES); named windows.
  - [ ] GROUP BY GROUPING SETS, CUBE, ROLLUP (parser + AST or normalized form).
  - [ ] Tests incl. TPC‑DS/Trino examples where feasible.

- Phase 3 — Statements
  - [ ] Transactions: START TRANSACTION / COMMIT / ROLLBACK (+ isolation/access modes).
  - [ ] SET/RESET: SESSION, PATH (path spec), TIME ZONE, TRANSACTION; RESET SESSION.
  - [ ] EXPLAIN ANALYZE; DESCRIBE INPUT/OUTPUT.
  - [ ] Tests for each statement variant.

- Phase 4 — Procedures & LATERAL table functions
  - [ ] CALL proc(args) support.
  - [ ] LATERAL with table functions (UNNEST/JSON_TABLE) + column alias lists.
  - [ ] Tests for CALL and LATERAL table function forms.

- Phase 5 — Ordered‑set aggregates + JSON_TABLE
  - [ ] WITHIN GROUP (ORDER BY …) e.g., LISTAGG.
  - [ ] JSON_TABLE minimal column spec parsing; represent as table function node.
  - [ ] Targeted tests.

Implementation Notes
- Keep parser recursive‑descent style; extend `SqlToken` for missing keywords/operators and classify as reserved/non‑reserved appropriately.
- Introduce minimal AST nodes for: grouping sets/cube/rollup, advanced window frames/exclude, quantified comparisons, distinct‑from, transactions, CALL, extended SHOW/DESCRIBE/EXPLAIN.
- Extend `SqlGenerator` only where required initially (e.g., FETCH, SHOW variants); deeper codegen/rewrites can follow.
- Backward compatible: maintain existing behavior; new syntax guarded by tests.

Test Plan
- Add unit specs in `spec/sql/basic` per new feature; a few focused TPC‑DS/Trino samples for grouping/windows.
- Ensure `./sbt test` remains green across modules; add parser stress tests for ambiguous forms.

Acceptance Criteria
- New syntax parses into coherent AST with accurate spans.
- Round‑trip (parse → generate) holds for simple cases where supported by `SqlGenerator`.
- Documentation/tests cover added syntax; CI green.

Risks / Open Questions
- Some Trino behaviors (e.g., name resolution for NATURAL/USING) require analyzer updates; we can normalize in parser or handle in analyzer.
- JSON_TABLE breadth is large; we’ll start with a minimal viable subset.
- Labeling and milestones can split phases into separate issues if preferred.

References
- Trino grammar: core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4
- Parser code: `wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlParser.scala`
- Tokens: `wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlToken.scala`
- AST: `wvlet-lang/src/main/scala/wvlet/lang/model`

