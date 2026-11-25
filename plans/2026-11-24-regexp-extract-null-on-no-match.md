# regexp_extract returns NULL when nothing matches

## Goals
- `s.regexp_extract(pattern)` and `s.regexp_extract(pattern, group)` mean one thing on every engine: NULL when the pattern does not match (user decision, 2026-11-23).
- Today DuckDB, Hive, and the wvlet engine return `''`, while Trino, Snowflake, and BigQuery return NULL, so the same query gives different answers depending on the engine.

## Background
- The naming ADR (`adr/2026-11-23-stdlib-function-naming.md`) says a stdlib function has one meaning on every engine, and each dialect variant wraps the engine's own function to get it.
- NULL is the ANSI-like and majority behaviour (Trino, Snowflake, BigQuery, PostgreSQL `substring(... from regex)`), and it keeps "no match" apart from "matched an empty string" (`'b'.regexp_extract('a*')` is `''`).
- The dbt gap report found TD Trino SQL that tests `regexp_extract(...) IS NULL`; with `''` those queries silently change meaning on DuckDB and the engine.
- The engine runs the stdlib's `in duckdb` variants, so fixing the DuckDB variant fixes the engine too. The engine's bare `regexp_extract` SQL function stays DuckDB-compatible (`''`), as the catalog function it imitates.

## Design
- Lane: **Express** — the user already chose the behaviour; the change is a small stdlib variant fix plus docs, no always-gate area.
- DuckDB variant (also the engine): return the extract only when the pattern matches, otherwise NULL (`case when regexp_matches(...) then regexp_extract(...) end`). A real empty match stays `''`, unlike a `nullif(..., '')` wrap.
- Hive variant: the same guard with `rlike`. Hive's two-argument `regexp_extract` defaults to group 1, so the variant passes group 0 explicitly to keep "whole match" as the default.
- Trino and Snowflake already return NULL; no change.
- Add the rule to the ADR's semantics list and the doc comments: no match gives NULL; a match keeps its text, even when empty.
- Tests: engine vs DuckDB conformance for no-match, empty match, NULL inputs, and group forms, plus an assertion that no-match is NULL.

## Known gaps (follow-ups, not in this PR)
- A capture group that does not take part in the match (`'b'.regexp_extract('(a)|b', 1)`) is `''` on DuckDB/engine and NULL on Trino. Fixing it needs a different DuckDB expression; rare in practice.
- BigQuery's `REGEXP_EXTRACT` third argument is a position, not a group, so `s.regexp_extract(pattern, group)` is wrong there today; and with a capturing pattern the one-argument form returns group 1. The ADR rule would make the group form absent for BigQuery.

## Alternatives and Why Not?
- `nullif(regexp_extract(...), '')`: one call, but turns a real empty match into NULL.
- Change the engine's bare `regexp_extract` to return NULL: breaks DuckDB compatibility for users writing DuckDB SQL against the engine.
