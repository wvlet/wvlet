# Stdlib encoding and hashing functions on every engine

## Goals
- Add the ADR's tier-1 encoding names (`adr/2026-11-23-stdlib-function-naming.md`) to the stdlib: `s.to_base64` / `s.from_base64`, `s.to_hex` / `s.from_hex`, `s.sha1`, and a top-level `uuid` that returns a string.
- Run them, and the existing `s.sha256`, on the `wvlet` engine with the same results as DuckDB.

## Background
- The engine has no binary (BLOB) type. Its values are strings, numbers, dates, and nested values.
- DuckDB's `from_base64` and `unhex` return BLOB, and `uuid()` returns a UUID. Trino and BigQuery work on varbinary/BYTES.
- The engine's `md5` is a small pure-Scala digest because Scala.js has no `MessageDigest`. `sha1` and `sha256` need the same.

## Design
- Lane: Express. The names and their meaning come from the approved ADR, and nothing here touches an always-gate area.
- String functions work on the string's UTF-8 bytes. Decoders return a string and fail on input that is not valid base64/hex or that decodes to invalid UTF-8.
- `to_hex` returns lowercase, like the ADR's hex digests (`md5`, `sha256`, `sha1`). DuckDB, Trino, Spark, and Snowflake uppercase by default, so their variants wrap it in `lower(...)`. `from_hex` accepts either case.
- Each engine gets a variant only where it keeps that meaning: DuckDB, Trino, Hive/Spark, Snowflake, BigQuery for the string functions. `uuid` skips Hive, which has no built-in UUID function.
- The DuckDB variants convert at the edges: `to_base64(encode(s))`, `decode(from_base64(s))`, `lower(hex(s))`, `decode(unhex(s))`, `cast(uuid() as varchar)`.
- On the engine, `encode` / `decode` pass strings through, and `from_base64` / `unhex` return the decoded string. Raw DuckDB-style `from_base64(s)` therefore yields a string on the engine rather than a BLOB. That is acceptable until the engine has a binary type.
- `uuid()` is evaluated per row (a random v4 UUID), like `random()`. It is called with parentheses, like `row_number()`. The compiler only inlines a bare top-level name for `= native` defs (`ulid_string`), so bare `uuid` is a follow-up.
- Tests: conformance against DuckDB for each function (NULL, empty string, non-ASCII, invalid input), plus shared unit tests for the digests' known vectors and decode errors, which also run on Scala.js.

## Alternatives and Why Not?
- Uppercase `to_hex` (DuckDB's default): it would disagree with the lowercase digests the ADR fixes, so `s.to_hex` and `s.md5` would print differently.
- A binary type in the engine: much wider scope, and no stdlib function returns binary today.
