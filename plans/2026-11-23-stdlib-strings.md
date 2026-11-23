# Stdlib string functions on every engine

## Goals
- Add the ADR's tier-1 string names (`adr/2026-11-23-stdlib-function-naming.md`) to the stdlib: `s.translate(from, to)`, `s.normalize`, `s.octet_length`, `s.regexp_extract_all(pattern)` / `(pattern, group)`, `s.regexp_split(pattern)`, `s.url_encode` / `s.url_decode`, `s.hamming_distance(other)`, `s.jaro_winkler_similarity(other)`.
- Run them on the `wvlet` engine with the same results as DuckDB.

## Background
- Engines disagree on details the ADR fixes: Spark's `regexp_extract_all` defaults to group 1, Trino and Spark URL-encode a space as `+`, and Snowflake's Jaro-Winkler is a case-insensitive integer from 0 to 100.
- The engine runs on the JVM, Scala.js, and Scala Native. `java.text.Normalizer` and `java.net.URLEncoder` are JVM-only.
- The engine's `strlen` currently returns the character count. DuckDB's `strlen` returns the byte count.

## Design
- Lane: Express. The names and their meaning come from the approved ADR, and nothing here touches an always-gate area.
- Each function keeps one meaning on every engine. Variants wrap the engine's function where needed, and a function is left out on an engine that cannot match:
  - `regexp_extract_all` defaults to the whole match (group 0). Hive has no `regexp_extract_all` (it is Spark-only), and BigQuery cannot choose a group, so both are left out.
  - `regexp_split` keeps empty strings, including trailing ones. Snowflake and BigQuery have no regex split, so they are left out.
  - `url_encode` follows RFC 3986: unreserved characters (`A-Z a-z 0-9 - _ . ~`) stay as they are, everything else becomes `%XX` of the UTF-8 bytes, and a space becomes `%20`. Trino's Java-style encoding is rewritten to match. Engines without URL functions are left out.
  - `normalize` is NFC. Hive and Snowflake have no NFC function, so they are left out.
  - `jaro_winkler_similarity` is a double from 0 to 1, case-sensitive, over characters. DuckDB is the only engine with that meaning, so only DuckDB gets it (and the wvlet engine).
  - `hamming_distance` counts differing characters and fails on strings of different lengths, as DuckDB 2.0.1 and Trino do. DuckDB also rejects empty strings, and Trino returns 0 for them.
- The engine implements DuckDB's functions in pure Scala for URL coding, Hamming, and Jaro-Winkler. NFC normalization goes through a small per-platform shim: the JVM's `Normalizer`, JavaScript's `String.prototype.normalize`, and on Scala Native ASCII input only (other input reports that it is unsupported).
- The engine's `strlen` now counts UTF-8 bytes, as DuckDB's does. `s.octet_length` compiles to it on DuckDB.
- Tests: compare each function against DuckDB, covering NULL, empty strings, non-ASCII text, and the edges listed above. Shared unit tests check the error cases and also run on Scala.js.

## Alternatives and Why Not?
- Snowflake's `JAROWINKLER_SIMILARITY / 100`: it ignores case, so results would still differ.
- Leave `normalize` out of the engine on every platform: it is common in data cleaning, and only Scala Native lacks a normalizer.
