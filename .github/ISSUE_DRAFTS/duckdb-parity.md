Summary
Align SqlParser with the DuckDB SQL dialect to improve compatibility when running DuckDB-oriented queries and examples. Ship in small phases with tests.

Motivation
DuckDB adds pragmatic statements (ATTACH/DETACH, PRAGMA, SET/RESET), file I/O (COPY, IMPORT/EXPORT), extensions (INSTALL/LOAD), macros (CREATE MACRO), and query features (QUALIFY, SAMPLE forms, PIVOT/UNPIVOT, grouping sets, window details). Supporting these in the parser reduces friction for users moving queries between wvlet and DuckDB.

Current Coverage (short)
- Queries: SELECT, VALUES, WITH/RECURSIVE, subqueries, EXISTS, UNION/INTERSECT/EXCEPT (+ ALL).
- Clauses: WHERE, GROUP BY (simple), HAVING, ORDER BY, LIMIT/OFFSET; windows (OVER with PARTITION/ORDER, ROWS frames); FILTER (...) and window after function calls; AT TIME ZONE; CAST/TRY_CAST; :: cast; CASE; LIKE [ESCAPE]; BETWEEN; IN/NOT IN.
- Structures: arrays ( [] and ARRAY(...) ), maps ({k:v} and MAP(...)), JSON literal + JSON_OBJECT (KEY…VALUE + modifiers), lambdas, subscripts.
- Table features: TABLESAMPLE and USING SAMPLE; UNNEST [WITH ORDINALITY]; LATERAL (subquery); joins.
- DML/DDL: INSERT/INSERT INTO (columns or query), UPDATE, DELETE, MERGE (basic branches); CREATE SCHEMA/TABLE/VIEW (+ IF NOT EXISTS, CTAS); DROP SCHEMA/TABLE/VIEW; ALTER TABLE (rename/add/drop column, set data type/default/not null, set authorization/properties/execute …).
- Commands: EXPLAIN [PLAN] [FOR …], SHOW (catalogs|schemas|tables [IN …]|columns FROM …|functions|CREATE VIEW …), USE schema.
- Prepared: PREPARE, EXECUTE (stmt(args)) and EXECUTE … USING …, DEALLOCATE.

Key Gaps vs DuckDB
- Statements/commands
  - ATTACH/DETACH database; USE database alias.
  - PRAGMA and SET/RESET forms for DuckDB configuration.
  - COPY TO/FROM (with options); IMPORT/EXPORT DATABASE; VACUUM; CHECKPOINT; SUMMARIZE.
  - Extensions: INSTALL/LOAD.
  - CALL procedures; CREATE MACRO (scalar/table macros).
  - DESCRIBE variants; COMMENT ON.
- Query features
  - QUALIFY clause.
  - SAMPLE variants already present (USING SAMPLE), ensure parity with ROWS/PERCENT forms and modifiers.
  - PIVOT / UNPIVOT clauses.
  - GROUP BY GROUPING SETS, CUBE, ROLLUP.
  - Window details: RANGE/GROUPS frames; EXCLUDE (CURRENT ROW/TIES/NO OTHERS); named windows.
  - Predicates: IS DISTINCT FROM / IS NOT DISTINCT FROM; quantified comparisons (ANY/SOME/ALL).
- DML nuances
  - INSERT OR REPLACE ; INSERT ON CONFLICT … DO NOTHING/UPDATE; UPDATE … FROM.

Plan (phased)
- Phase 1 — Query clause parity (shared with Trino plan when possible)
  - [ ] QUALIFY clause.
  - [ ] GROUPING SETS, CUBE, ROLLUP.
  - [ ] Window: RANGE/GROUPS; EXCLUDE; named windows.
  - [ ] Predicates: IS DISTINCT FROM / NOT DISTINCT FROM; ANY/SOME/ALL.
  - [ ] PIVOT/UNPIVOT clauses.
  - [ ] Tests in spec/sql/basic.

- Phase 2A — Catalog/config primitives
  - [ ] ATTACH/DETACH database; USE alias.
  - [ ] PRAGMA; SET/RESET (DuckDB forms).
  - [ ] Transaction management statements.
  - [ ] Tests for each statement variant.

- Phase 2B — I/O and extensions
  - [ ] COPY TO/FROM (options); IMPORT/EXPORT DATABASE.
  - [ ] INSTALL/LOAD extension; VACUUM; CHECKPOINT; SUMMARIZE; DESCRIBE; COMMENT ON.
  - [ ] Tests for I/O, extension, and utility statements.

- Phase 3 — Macros & CALL
  - [ ] CREATE MACRO (scalar/table) + parameter parsing; CALL proc(args).
  - [ ] LATERAL for table macros where applicable.
  - [ ] Tests for macros and CALL.

- Phase 4 — DML conveniences
  - [ ] INSERT OR REPLACE; INSERT ON CONFLICT (DO NOTHING / DO UPDATE SET …).
  - [ ] UPDATE … FROM; DELETE USING (if applicable).
  - [ ] Tests for each DML form.

Implementation Notes
- Extend SqlToken with new keywords (ATTACH, DETACH, PRAGMA, INSTALL, LOAD, COPY, EXPORT, IMPORT, VACUUM, CHECKPOINT, SUMMARIZE, CONFLICT, REPLACE, ON, DO, NOTHING, etc.). Keep reserved/non‑reserved consistent.
- Add minimal AST case classes for new statements (Attach/Detach/Pragma/Copy/Export/Import/Install/Load/Vacuum/Checkpoint/Summarize/Describe/Comment/Call/CreateMacro/UseDbAlias).
- Reuse existing syntax where aligned (SAMPLE, QUALIFY, window, grouping sets) to minimize code changes relative to Trino plan.
- Keep parser recursive‑descent style and span tracking; add tests for ambiguity hotspots.

Test Plan
- Add parser specs in spec/sql/basic covering each new feature, plus a couple of integrated examples (e.g., ATTACH + USE + COPY; CREATE MACRO + CALL).
- Ensure `./sbt test` remains green across modules.

Acceptance Criteria
- New DuckDB dialect forms parse to coherent AST with accurate spans.
- Overlapping features with Trino are unified in AST and parser paths.
- Docs/tests cover the added syntax; CI green.

References
- DuckDB SQL Dialect Overview
- Specific docs: ATTACH/DETACH & USE; PRAGMA/SET; COPY/IMPORT/EXPORT; QUALIFY; PIVOT/UNPIVOT; GROUPING SETS; CREATE MACRO; CALL; Transactions.

