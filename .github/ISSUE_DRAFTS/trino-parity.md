Summary
Align SqlParser with Trino’s SQL grammar (SqlBase.g4) to improve dialect compatibility and reduce surprises when running Trino‑style queries. Work ships in small, reviewed phases with tests.

Motivation
Users expect Trino syntax to parse. Improving parity reduces friction and enables porting queries and examples.

Current Coverage (short)
- SELECT, VALUES, WITH/RECURSIVE, subqueries, EXISTS, UNION/INTERSECT/EXCEPT (+ ALL).
- WHERE, GROUP BY (simple), HAVING, ORDER BY, LIMIT/OFFSET; windows (OVER with PARTITION/ORDER, ROWS frames); FILTER (...) and window on function calls; AT TIME ZONE; CAST/TRY_CAST; :: cast; CASE; LIKE [ESCAPE]; BETWEEN; IN/NOT IN.
- Arrays, maps, JSON literal + JSON_OBJECT (KEY…VALUE + modifiers), lambdas, subscripts.
- TABLESAMPLE + USING SAMPLE; UNNEST [WITH ORDINALITY]; LATERAL (subquery); joins; ASOF token; parenthesized relations.
- INSERT/INSERT INTO (columns or query), UPDATE, DELETE, MERGE (basic branches); CREATE SCHEMA/TABLE/VIEW (+ IF NOT EXISTS, CTAS); DROP SCHEMA/TABLE/VIEW; ALTER TABLE (rename/add/drop column, set data type/default/not null, set authorization/properties/execute …).
- Commands: EXPLAIN [PLAN] [FOR …], SHOW (catalogs|schemas|tables [IN …]|columns FROM …|functions|CREATE VIEW …), USE schema.
- Prepared: PREPARE, EXECUTE (… USING … or (…)), DEALLOCATE.

Key Gaps vs Trino
- Query features
  - GROUP BY: GROUPING SETS, CUBE, ROLLUP.
  - Windows: RANGE/GROUPS frames; EXCLUDE (CURRENT ROW/TIES/NO OTHERS); named windows.
  - QUALIFY clause.
  - FETCH FIRST/NEXT n ROW(S) ONLY.
  - NATURAL JOIN; broader LATERAL/table function forms.
  - Predicates: IS DISTINCT FROM / IS NOT DISTINCT FROM; quantified comparisons (ANY/SOME/ALL).
  - Ordered‑set aggregates: WITHIN GROUP (ORDER BY …) e.g., LISTAGG.
- Statements/commands
  - Transactions: START TRANSACTION / COMMIT / ROLLBACK (+ options).
  - SET/RESET forms: SET SESSION, RESET SESSION, SET PATH, SET TIME ZONE, SET/RESET TRANSACTION.
  - EXPLAIN ANALYZE; DESCRIBE INPUT/OUTPUT.
  - CALL procedures.
- Table functions / JSON
  - JSON_TABLE; extended LATERAL table function aliasing/column lists.
- Types/intervals
  - Align date/time precisions and WITH TIME ZONE forms where applicable.

Plan (phased)
- Phase 1 — Syntactic additions (low risk)
  - [ ] FETCH FIRST/NEXT … ONLY in query tail.
  - [ ] QUALIFY clause (filter after windowed selections).
  - [ ] NATURAL [LEFT|RIGHT|FULL|INNER] JOIN (USING columns).
  - [ ] Predicates: IS DISTINCT FROM / IS NOT DISTINCT FROM.
  - [ ] Quantified comparisons: =, <, … ANY/SOME/ALL (subquery).
  - [ ] SHOW extensions: SHOW CREATE TABLE/MATERIALIZED VIEW/FUNCTION; SHOW STATS FOR table.
  - [ ] Tests in spec/sql/basic for each.
- Phase 2 — Windows + Grouping
  - [ ] Window: RANGE, GROUPS; EXCLUDE (NO OTHERS|CURRENT ROW|TIES); named windows.
  - [ ] GROUP BY: GROUPING SETS, CUBE, ROLLUP (parser + AST or normalization).
  - [ ] Tests incl. TPC‑DS/Trino examples where feasible.
- Phase 3 — Statements
  - [ ] Transactions: START TRANSACTION / COMMIT / ROLLBACK (+ isolation/access modes).
  - [ ] SET/RESET: SESSION, PATH, TIME ZONE, TRANSACTION; RESET SESSION.
  - [ ] EXPLAIN ANALYZE; DESCRIBE INPUT/OUTPUT.
  - [ ] Tests for each statement variant.
- Phase 4 — Procedures & LATERAL table functions
  - [ ] CALL proc(args).
  - [ ] LATERAL with table functions (UNNEST/JSON_TABLE) + column alias lists.
  - [ ] Tests for CALL and LATERAL table function forms.
- Phase 5 — Ordered‑set aggregates + JSON_TABLE
  - [ ] WITHIN GROUP (ORDER BY …) e.g., LISTAGG.
  - [ ] JSON_TABLE minimal column spec parsing; represent as table function node.
  - [ ] Targeted tests.

Implementation Notes
- Extend SqlToken for missing keywords/operators; keep reserved/non‑reserved consistent.
- Add AST for grouping sets/cube/rollup, advanced window frames/exclude, quantified comparisons, distinct‑from, transactions, CALL, extended SHOW/DESCRIBE/EXPLAIN.
- Extend SqlGenerator selectively at first (e.g., FETCH, SHOW variants).
- Maintain backward compatibility; new syntax guarded by tests.

Test Plan
- New specs under spec/sql/basic; a few focused TPC‑DS/Trino samples for grouping/windows. CI stays green.

Acceptance Criteria
- New syntax parses into coherent AST with accurate spans.
- Round‑trip (parse → generate) holds for simple cases where supported.
- Docs/tests cover added syntax; CI green.

References
- Trino grammar: core/trino-grammar/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4
- Parser: wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlParser.scala
- Tokens: wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlToken.scala
- AST: wvlet-lang/src/main/scala/wvlet/lang/model
