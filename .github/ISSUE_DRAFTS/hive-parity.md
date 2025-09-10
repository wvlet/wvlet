Summary
Align SqlParser with the Apache Hive SQL dialect to improve compatibility when parsing Hive-oriented queries and DDL/DML. Ship in small phases with tests.

Motivation
Hive is a widely used SQL dialect with rich DDL (partitions/buckets/serde), file I/O, and LATERAL VIEW/UDTF patterns. Parsing core Hive syntax reduces migration friction and enables running Hive examples. References: Hive Language Manual and Hive grammar (HiveParser.g).

Current Coverage (short)
- Core queries: SELECT, VALUES, WITH/RECURSIVE, subqueries, EXISTS, UNION/INTERSECT/EXCEPT (+ ALL).
- Clauses: WHERE, simple GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET; windows (OVER with PARTITION/ORDER, ROWS frames). Function FILTER and window; AT TIME ZONE; CAST/TRY_CAST; :: cast; CASE; LIKE [ESCAPE]; BETWEEN; IN/NOT IN.
- Structures: arrays, maps, JSON literal + JSON_OBJECT (KEY…VALUE + modifiers), lambdas, subscripts.
- Table features: TABLESAMPLE / USING SAMPLE; UNNEST [WITH ORDINALITY]; LATERAL (subquery). Joins.
- DML/DDL: INSERT/INSERT INTO (columns or query), UPDATE, DELETE, MERGE (basic), CREATE SCHEMA/TABLE/VIEW (+ IF NOT EXISTS, CTAS), DROP SCHEMA/TABLE/VIEW, ALTER TABLE (rename/add/drop column, set data type/default/not null, set authorization/properties/execute …).
- Existing Hive helpers: HiveRewriteFunctions, HiveRewriteUnnest, and example specs in spec/sql/hive/ (for guidance/tests).

Key Gaps vs Hive
- LATERAL VIEW & UDTFs
  - Parse `LATERAL VIEW [OUTER] <udtf>(args) table_alias [AS col[, col]...]` in FROM.
  - Support UDTFs like `explode`, `posexplode`, `inline`, `stack` as table functions.
  - Multiple LATERAL VIEWs chained after a base relation.
- Hive DDL table options
  - `CREATE [EXTERNAL|TEMPORARY] TABLE` options: `PARTITIONED BY (...)`, `CLUSTERED BY (...) [SORTED BY (...)] INTO n BUCKETS`, `ROW FORMAT SERDE ... WITH SERDEPROPERTIES (...)`, `STORED AS <format>`, `LOCATION '...'`, `TBLPROPERTIES (...)`, `LIKE`.
  - ALTER TABLE partition maintenance: `ADD/DROP/RENAME PARTITION`, `RECOVER PARTITIONS (MSCK REPAIR)`.
- DML w/ partitions & directories
  - `INSERT OVERWRITE|INTO TABLE t [PARTITION (...)] select ...` (partition spec grammar).
  - Multi-insert: `FROM src INSERT OVERWRITE TABLE t1 SELECT ... INSERT INTO TABLE t2 SELECT ...`.
  - `INSERT OVERWRITE DIRECTORY` [LOCAL] path [ROW FORMAT/FILEFORMAT options].
  - `LOAD DATA [LOCAL] INPATH '...' INTO TABLE t [PARTITION (...)]`.
- Utility/maintenance
  - `TRUNCATE TABLE`, `ANALYZE TABLE ... COMPUTE STATISTICS`.
  - `SHOW PARTITIONS`, `SHOW TABLE EXTENDED`, `DESCRIBE [FORMATTED|EXTENDED]`.
- Types & extras
  - Confirm complex type forms: `array<...>`, `map<k,v>`, `struct<...>`, and consider `uniontype<...>`.
  - Backquoted identifiers are supported; ensure coverage across new clauses.
  - Hints (/*+ ... */) are out-of-scope initially due to comment skipping.

Plan (phased)
- Phase 1 — LATERAL VIEW & UDTFs
  - [ ] Parse `LATERAL VIEW [OUTER] <udtf>(...) alias [AS cols]` as table function relations; allow chaining.
  - [ ] UDTFs: explode/posexplode/inline/stack accepted as function names; reuse UNNEST machinery where reasonable.
  - [ ] Tests mirroring spec/sql/hive/hive-lateral-view.sql and additional cases (multiple lateral views, OUTER).

- Phase 2 — Hive CREATE/ALTER TABLE options
  - [ ] Extend parser to accept partition/bucketing/sorting/serde/file format/location/tblproperties options in CREATE TABLE (and CTAS).
  - [ ] Partition DDL: ALTER TABLE ... ADD/DROP/RENAME PARTITION; RECOVER PARTITIONS.
  - [ ] Represent options in AST (new nodes/fields) or normalized properties; document mapping.
  - [ ] Tests for common variants.

- Phase 3 — Partition-aware DML & directory targets
  - [ ] INSERT OVERWRITE|INTO TABLE ... [PARTITION (...)] SELECT ...
  - [ ] Multi-insert FROM ... INSERT ... INSERT ...
  - [ ] INSERT OVERWRITE DIRECTORY [LOCAL] ... with output format options.
  - [ ] Tests for partition specs and multi-insert.

- Phase 4 — Utility statements
  - [ ] TRUNCATE TABLE; ANALYZE TABLE ... COMPUTE STATISTICS.
  - [ ] LOAD DATA [LOCAL] INPATH ... INTO TABLE ... [PARTITION (...)]
  - [ ] SHOW PARTITIONS; SHOW TABLE EXTENDED; DESCRIBE [FORMATTED|EXTENDED].
  - [ ] Tests for each statement family.

- Phase 5 — Types & extras
  - [ ] Confirm complex type angle-bracket forms and add `uniontype<...>` if needed.
  - [ ] Evaluate acceptance of hints later (blocked by comment skipping).

Implementation Notes
- Extend `SqlToken` with missing keywords (e.g., LATERAL VIEW, OUTER, EXTERNAL, PARTITIONED, CLUSTERED, SORTED, BUCKETS, SERDE/ROW FORMAT, STORED AS, LOCATION, TBLPROPERTIES, LIKE, DIRECTORY, LOCAL, LOAD DATA, MSCK/REPAIR, ANALYZE, TRUNCATE, PARTITION, COMPUTE STATISTICS, etc.). Keep reserved/non‑reserved consistent.
- Add relation parsing for table functions in FROM (beyond UNNEST), including alias + column list.
- Add DDL/DML nodes or extend existing ones to carry Hive options/partition specs while keeping SQL generator changes minimal initially.
- Maintain backward compatibility; reuse existing Hive rewrite passes where helpful.

Test Plan
- Add new specs under `spec/sql/hive/` and `spec/sql/basic/` for each feature; keep `./sbt test` green.

Acceptance Criteria
- New Hive forms parse into coherent AST with accurate spans.
- Overlapping features (e.g., UDTFs) integrate with existing relation model (UNNEST/table functions).
- Docs/tests cover added syntax; CI green.

References
- Hive Language Manual (Apache Confluence)
- Hive grammar: parser/src/java/org/apache/hadoop/hive/ql/parse/HiveParser.g

